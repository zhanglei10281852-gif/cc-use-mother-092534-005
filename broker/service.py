"""连接器会话代理服务：四维最小授权、版本双重校验、分页收缩、幂等写与回执对账。"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterable, Sequence

from .errors import (
    AuthorizationDenied,
    CAPABILITY_ISOLATED,
    CAPABILITY_NOT_GRANTED,
    CAPABILITY_REVOKED,
    EXPIRED,
    EXPANSION_UNCONFIRMED,
    InvalidTransition,
    NOT_YET_VALID,
    OWNER_MISMATCH,
    QUOTA_EXHAUSTED,
    SCOPE_REDUCED,
    SESSION_CLOSED,
    SESSION_NOT_ACTIVE,
    SESSION_RECONCILING,
    DeliveryUncertain,
)
from .fakes import FakeConnector
from .models import Capability, minimum_set
from .storage import Store, new_id


def _now() -> datetime:
    return datetime.now().astimezone()


def _cap_row_to_capability(row: Any) -> Capability:
    return Capability(
        resource=row["resource"],
        action=row["action"],
        data_scope=row["data_scope"],
        valid_from=datetime.fromisoformat(row["valid_from"]),
        valid_to=datetime.fromisoformat(row["valid_to"]),
    )


def _within_window(cap_row: Any, moment: datetime) -> bool:
    return (
        datetime.fromisoformat(cap_row["valid_from"])
        <= moment
        <= datetime.fromisoformat(cap_row["valid_to"])
    )


class Broker:
    def __init__(self, store: Store | None = None,
                 connectors: dict[str, FakeConnector] | None = None) -> None:
        self.store = store or Store()
        self.connectors = connectors or {}

    def register_connector(self, connector: FakeConnector) -> None:
        self.connectors[connector.connector_id] = connector

    def _connector(self, connector_id: str) -> FakeConnector:
        if connector_id not in self.connectors:
            raise KeyError(f"未注册的连接器：{connector_id}")
        return self.connectors[connector_id]

    @contextmanager
    def _tx(self):
        # 拒绝也是必须保留的审计事实，所以异常时不回滚，只提交已记录的内容。
        # 真正的编程错误（IntegrityError 等）仍向上抛出。
        with self.store.lock():
            try:
                yield
            finally:
                self.store.commit()

    # ------------------------------------------------------------------
    # 1. 任务提交：计算最小能力集合
    # ------------------------------------------------------------------
    def submit_task(self, tenant_id: str, connector_id: str, owner_id: str,
                    task_id: str, demands: Sequence[Capability],
                    actor_id: str | None = None,
                    now: datetime | None = None) -> str:
        now = now or _now()
        caps = minimum_set(demands)
        if not caps:
            raise ValueError("任务至少需要一项能力")
        session_id = new_id("sess")
        with self._tx():
            self.store.create_session(
                {
                    "session_id": session_id,
                    "tenant_id": tenant_id,
                    "connector_id": connector_id,
                    "owner_id": owner_id,
                    "task_id": task_id,
                },
                now,
            )
            self.store.add_demands(session_id, caps, status="requested")
            self.store.append_event(
                "demand.calculated", session_id,
                {
                    "task_id": task_id,
                    "minimum_capabilities": [c.to_dict() for c in caps],
                    "note": "按资源/动作/数据范围/有效时间四维计算的最小集合",
                },
                now, actor_id or owner_id, session_id=session_id, version=0,
            )
        return session_id

    # ------------------------------------------------------------------
    # 2. 资源所有者确认（授权恰好等于最小集合，不多一维）
    # ------------------------------------------------------------------
    def confirm_consent(self, session_id: str, granted_by: str,
                        now: datetime | None = None) -> int:
        now = now or _now()
        with self._tx():
            session = self._require_session(session_id)
            if session["owner_id"] != granted_by:
                raise AuthorizationDenied(
                    OWNER_MISMATCH, "只有资源所有者可以确认授权"
                )
            if session["status"] != "proposed":
                raise InvalidTransition(
                    f"会话处于 {session['status']}，不能首次确认授权"
                )
            caps = self._demanded_caps(session_id)
            version = self.store.bump_version(session_id, now)
            self.store.add_grant(session_id, version, granted_by, caps, now)
            self.store.add_capabilities(session_id, caps, version)
            self.store.conn.execute(
                "update session_demands set status = 'granted' where session_id = ?",
                (session_id,),
            )
            self.store.update_session_status(session_id, "consented", now)
            self.store.append_event(
                "consent.confirmed", session_id,
                {"granted_by": granted_by,
                 "capabilities": [c.to_dict() for c in caps]},
                now, granted_by, session_id=session_id, version=version,
            )
            return version

    # ------------------------------------------------------------------
    # 3. 扩权：登记扩权需求，必须由资源所有者重新确认后才生效
    # ------------------------------------------------------------------
    def request_expansion(self, session_id: str, new_demands: Iterable[Capability],
                          actor_id: str, now: datetime | None = None) -> None:
        now = now or _now()
        caps = minimum_set(new_demands)
        with self._tx():
            self._require_live_session(session_id)
            existing = {
                (r["resource"], r["action"], r["data_scope"], r["valid_from"], r["valid_to"])
                for r in self.store.demands(session_id)
            }
            fresh = [
                c for c in caps
                if (c.resource, c.action, c.data_scope,
                    c.valid_from.isoformat(), c.valid_to.isoformat()) not in existing
            ]
            if not fresh:
                raise InvalidTransition("扩权请求没有带来任何新能力")
            self.store.add_demands(session_id, fresh, status="expansion_requested")
            self.store.append_event(
                "demand.calculated", session_id,
                {"kind": "expansion_requested",
                 "minimum_capabilities": [c.to_dict() for c in fresh]},
                now, actor_id, session_id=session_id,
                version=self.store.get_session(session_id)["version"],
            )

    def confirm_expansion(self, session_id: str, granted_by: str,
                          now: datetime | None = None) -> int:
        now = now or _now()
        with self._tx():
            session = self._require_session(session_id)
            if session["owner_id"] != granted_by:
                raise AuthorizationDenied(
                    OWNER_MISMATCH, "扩大范围必须由资源所有者重新确认"
                )
            self._require_live_session(session_id)
            pending = [
                d for d in self.store.demands(session_id)
                if d["status"] == "expansion_requested"
            ]
            if not pending:
                raise InvalidTransition("没有待确认的扩权请求")
            caps = [
                Capability(
                    resource=d["resource"], action=d["action"],
                    data_scope=d["data_scope"],
                    valid_from=datetime.fromisoformat(d["valid_from"]),
                    valid_to=datetime.fromisoformat(d["valid_to"]),
                )
                for d in pending
            ]
            version = self.store.bump_version(session_id, now)
            self.store.add_grant(session_id, version, granted_by, caps, now)
            self.store.add_capabilities(session_id, caps, version)
            self.store.conn.execute(
                "update session_demands set status = 'granted' "
                "where session_id = ? and status = 'expansion_requested'",
                (session_id,),
            )
            self.store.append_event(
                "consent.confirmed", session_id,
                {"kind": "expansion", "granted_by": granted_by,
                 "capabilities": [c.to_dict() for c in caps]},
                now, granted_by, session_id=session_id, version=version,
            )
            return version

    # ------------------------------------------------------------------
    # 4. 管理员：隔离 / 恢复 / 撤销单个能力或整类资源
    # ------------------------------------------------------------------
    def isolate_capability(self, session_id: str, resource: str, action: str,
                           data_scope: str, actor_id: str, reason: str = "",
                           now: datetime | None = None) -> int:
        return self._change_capability(
            session_id, resource, action, data_scope, actor_id,
            new_status="isolated", event_type="capability.isolated", reason=reason,
            now=now,
        )

    def restore_capability(self, session_id: str, resource: str, action: str,
                           data_scope: str, actor_id: str,
                           now: datetime | None = None) -> int:
        return self._change_capability(
            session_id, resource, action, data_scope, actor_id,
            new_status="granted", event_type="capability.restored", reason=None,
            require_status="isolated", now=now,
        )

    def revoke_capability(self, session_id: str, resource: str, action: str,
                          data_scope: str, actor_id: str, reason: str = "",
                          now: datetime | None = None) -> int:
        return self._change_capability(
            session_id, resource, action, data_scope, actor_id,
            new_status="revoked", event_type="capability.revoked", reason=reason,
            now=now,
        )

    def revoke_resource(self, session_id: str, resource: str, actor_id: str,
                        reason: str = "", now: datetime | None = None) -> int:
        """撤销某项资源的全部能力；同一会话的其他资源任务不受影响。"""
        now = now or _now()
        with self._tx():
            self._require_live_session(session_id)
            targets = [
                c for c in self.store.capabilities(session_id)
                if c["resource"] == resource and c["status"] != "revoked"
            ]
            if not targets:
                raise InvalidTransition(f"资源 {resource} 没有可撤销的能力")
            version = self.store.bump_version(session_id, now)
            for row in targets:
                self.store.set_capability_status(row["id"], "revoked", version, reason)
            self.store.update_session_status(session_id, "restricted", now)
            self.store.append_event(
                "capability.revoked", session_id,
                {"resource": resource, "reason": reason,
                 "capabilities": [_cap_row_to_capability(r).to_dict() for r in targets]},
                now, actor_id, session_id=session_id, version=version,
            )
            return version

    def reduce_scope(self, session_id: str, removed: Iterable[Capability],
                     actor_id: str, reason: str = "",
                     now: datetime | None = None) -> int:
        """收缩若干具体能力（用于分页读取途中授权收缩）。"""
        now = now or _now()
        removed = minimum_set(removed)
        with self._tx():
            self._require_live_session(session_id)
            version = self.store.bump_version(session_id, now)
            hit = []
            for cap in removed:
                row = self.store.find_capability(
                    session_id, cap.resource, cap.action, cap.data_scope
                )
                if row is not None and row["status"] == "granted":
                    self.store.set_capability_status(row["id"], "revoked", version, reason)
                    hit.append(cap.to_dict())
            if not hit:
                raise InvalidTransition("没有可收缩的已授权能力")
            self.store.update_session_status(session_id, "restricted", now)
            self.store.append_event(
                "scope.reduced", session_id,
                {"reason": reason, "removed": hit},
                now, actor_id, session_id=session_id, version=version,
            )
            return version

    def _change_capability(self, session_id: str, resource: str, action: str,
                           data_scope: str, actor_id: str, new_status: str,
                           event_type: str, reason: str | None,
                           require_status: str | None = None,
                           now: datetime | None = None) -> int:
        now = now or _now()
        with self._tx():
            self._require_live_session(session_id)
            row = self.store.find_capability(session_id, resource, action, data_scope)
            if row is None:
                raise InvalidTransition("能力不存在，无法变更")
            if require_status and row["status"] != require_status:
                raise InvalidTransition(
                    f"能力当前为 {row['status']}，需要 {require_status}"
                )
            if row["status"] == new_status:
                raise InvalidTransition(f"能力已经是 {new_status}")
            if row["status"] == "revoked" and new_status != "revoked":
                raise InvalidTransition("已撤销的能力不能恢复，需要重新授权")
            version = self.store.bump_version(session_id, now)
            self.store.set_capability_status(row["id"], new_status, version, reason)
            # 会话级状态：仍有受损能力则 restricted；全部健康则回到 consented
            if self.store.has_non_granted(session_id):
                self.store.update_session_status(session_id, "restricted", now)
            else:
                self.store.update_session_status(session_id, "consented", now)
            self.store.append_event(
                event_type, session_id,
                {"resource": resource, "action": action, "data_scope": data_scope,
                 "reason": reason},
                now, actor_id, session_id=session_id, version=version,
            )
            return version

    # ------------------------------------------------------------------
    # 5. 分页读取：每页调用前后双重版本校验；收缩时保留已取数据、停止未取页
    # ------------------------------------------------------------------
    def execute_read(self, session_id: str, resource: str, action: str,
                     data_scope: str, actor_id: str,
                     now: datetime | None = None) -> dict[str, Any]:
        now = now or _now()
        call_id = new_id("call")
        with self._tx():
            session, version = self._pre_check(
                session_id, resource, action, data_scope, now, call_id=call_id,
                actor_id=actor_id,
            )
            connector_id = session["connector_id"]
            self.store.insert_call(
                {
                    "call_id": call_id, "session_id": session_id,
                    "tenant_id": session["tenant_id"], "connector_id": connector_id,
                    "resource": resource, "action": action, "data_scope": data_scope,
                    "is_write": False, "status": "in_flight",
                    "pre_version": version, "payload": None,
                },
                now,
            )
            self.store.append_event(
                "call.authorized", session_id,
                {"call_id": call_id, "resource": resource, "action": action,
                 "data_scope": data_scope, "pre_version": version},
                now, actor_id, session_id=session_id, version=version,
            )
        return self._fetch_page(call_id, actor_id, initial=True, now=now)

    def fetch_next_page(self, call_id: str, actor_id: str,
                        now: datetime | None = None) -> dict[str, Any]:
        return self._fetch_page(call_id, actor_id, initial=False, now=now)

    def _fetch_page(self, call_id: str, actor_id: str, initial: bool,
                    now: datetime | None = None) -> dict[str, Any]:
        now = now or _now()
        with self._tx():
            call = self.store.get_call(call_id)
            if call is None:
                raise InvalidTransition(f"调用不存在：{call_id}")
            if call["status"] == "completed":
                raise InvalidTransition(f"调用 {call_id} 的分页已全部领取完毕")
            if call["status"] in ("stopped", "denied", "closed"):
                raise AuthorizationDenied(
                    call["deny_reason"] or SCOPE_REDUCED,
                    f"调用 {call_id} 已处于 {call['status']}",
                )
            session_id = call["session_id"]
            # —— 调用前校验（翻页时同样执行）——
            session, version = self._pre_check(
                session_id, call["resource"], call["action"], call["data_scope"],
                now, call_id=call_id, actor_id=actor_id, existing_call=call,
            )
            pre_version = call["pre_version"] if initial else version
            if not initial:
                self.store.update_call(call_id, pre_version=version)

            connector = self._connector(call["connector_id"])
            page = connector.read_page(
                call["resource"], call["data_scope"], call["page_cursor"]
            )

            # —— 调用后校验：重新读取该能力的状态。
            # 不能只比较版本号——对其他能力的隔离/恢复也会升版本，却不应影响本调用。
            # 只有“本次调用所用能力”在请求期间被收缩/撤销/隔离或过期，才停止翻页。
            fresh_session = self.store.get_session(session_id)
            post_version = fresh_session["version"]
            cap_after = self.store.find_capability(
                session_id, call["resource"], call["action"], call["data_scope"]
            )
            contracted = (
                fresh_session["status"] in ("closed", "revoked")
                or cap_after is None
                or cap_after["status"] != "granted"
                or not _within_window(cap_after, now)
            )
            page_index = self.store.next_page_index(call_id)

            self.store.add_page_audit(
                call_id, page_index, page.rows,
                delivered=not contracted, version=post_version, now=now,
            )

            if contracted:
                # 此前已交付的页保留审计；本页数据外部虽已返回，但留存审计、不交付；
                # 尚未领取的后续页立即停止。
                self.store.update_call(
                    call_id, status="stopped", post_version=post_version,
                    deny_reason=SCOPE_REDUCED, attempts=call["attempts"] + 1,
                )
                self.store.append_event(
                    "call.denied", session_id,
                    {"call_id": call_id, "reason": SCOPE_REDUCED,
                     "pre_version": pre_version, "post_version": post_version,
                     "retained_rows": len(page.rows),
                     "note": "授权在分页期间收缩：本页留存审计但不交付，未领取页停止"},
                    now, actor_id, session_id=session_id, version=post_version,
                )
                raise AuthorizationDenied(
                    SCOPE_REDUCED,
                    f"会话版本 {pre_version}->{post_version}，未领取页已停止",
                )

            # 只有确认交付的页才扣减额度；收缩当页未交付，不计费
            self.store.consume_quota(
                call["tenant_id"], call["connector_id"],
                call["resource"], call["action"], now.date().isoformat(),
            )

            if page.next_cursor is None:
                self.store.update_call(
                    call_id, status="completed", post_version=post_version,
                    page_cursor=None, exhausted=True, attempts=call["attempts"] + 1,
                )
            else:
                self.store.update_call(
                    call_id, post_version=post_version,
                    page_cursor=page.next_cursor, attempts=call["attempts"] + 1,
                )
            self.store.append_event(
                "call.completed", session_id,
                {"call_id": call_id, "page_index": page_index,
                 "row_count": len(page.rows), "post_version": post_version,
                 "exhausted": page.next_cursor is None},
                now, actor_id, session_id=session_id, version=post_version,
            )
            return {
                "call_id": call_id,
                "rows": page.rows,
                "next_cursor": page.next_cursor,
                "exhausted": page.next_cursor is None,
                "version": post_version,
            }

    # ------------------------------------------------------------------
    # 6. 写操作：幂等键 + 先落待核销回执 + 崩溃安全重试
    # ------------------------------------------------------------------
    def execute_write(self, session_id: str, resource: str, payload: Any,
                      idempotency_key: str, actor_id: str,
                      now: datetime | None = None) -> dict[str, Any]:
        now = now or _now()
        call_id = new_id("call")
        with self._tx():
            session = self._require_session(session_id)

            # 幂等键优先：同一租户+连接器下同一键永远复用首次结果，绝不二次发送
            existing = self.store.get_receipt(idempotency_key)
            if existing is not None:
                if existing["tenant_id"] != session["tenant_id"] or \
                        existing["connector_id"] != session["connector_id"]:
                    raise InvalidTransition("幂等键已在其他租户或连接器范围内使用")
                return self._replay_receipt(existing, actor_id, now)

            session, version = self._pre_check(
                session_id, resource, "write", "", now, call_id=call_id,
                actor_id=actor_id, write_payload=payload,
                idempotency_key=idempotency_key,
            )
            self.store.insert_call(
                {
                    "call_id": call_id, "session_id": session_id,
                    "tenant_id": session["tenant_id"],
                    "connector_id": session["connector_id"],
                    "resource": resource, "action": "write", "data_scope": "",
                    "is_write": True, "idempotency_key": idempotency_key,
                    "status": "in_flight", "pre_version": version, "payload": payload,
                },
                now,
            )
            # 关键顺序：先持久化 pending 回执，再向外部发送。
            # 这样即使发送后、回执前崩溃，恢复时仍凭同一幂等键对账，不会重复发送。
            self.store.insert_receipt(
                {
                    "idempotency_key": idempotency_key,
                    "tenant_id": session["tenant_id"],
                    "connector_id": session["connector_id"],
                    "call_id": call_id, "status": "pending",
                },
                now,
            )
            self.store.append_event(
                "receipt.pending", session_id,
                {"call_id": call_id, "idempotency_key": idempotency_key,
                 "pre_version": version},
                now, actor_id, session_id=session_id, version=version,
            )

        return self._send_write(call_id, actor_id, now=now)

    def retry_write(self, idempotency_key: str, actor_id: str,
                    now: datetime | None = None) -> dict[str, Any]:
        """本地重试：复用同一幂等键，外部服务端去重，不产生重复副作用。"""
        now = now or _now()
        with self._tx():
            receipt = self.store.get_receipt(idempotency_key)
            if receipt is None:
                raise InvalidTransition("没有对应的待核销回执，不能重试")
            if receipt["status"] != "pending":
                return self._replay_receipt(receipt, actor_id, now)
            call_id = receipt["call_id"]
        return self._send_write(call_id, actor_id, now=now, is_retry=True)

    def _send_write(self, call_id: str, actor_id: str,
                    now: datetime, is_retry: bool = False) -> dict[str, Any]:
        with self._tx():
            call = self.store.get_call(call_id)
            session_id = call["session_id"]
            key = call["idempotency_key"]
            connector = self._connector(call["connector_id"])
            try:
                receipt = connector.send(call["resource"],
                                         json_loads(call["payload"]), key)
            except TimeoutError:
                self.store.touch_receipt_attempt(key)
                self.store.update_call(call_id, attempts=call["attempts"] + 1)
                self.store.append_event(
                    "receipt.pending", session_id,
                    {"call_id": call_id, "idempotency_key": key,
                     "attempt": call["attempts"] + 1,
                     "note": "送达状态不明，禁止换键重发，等待对账"},
                    now, actor_id, session_id=session_id,
                    version=self.store.get_session(session_id)["version"],
                )
                raise DeliveryUncertain(call_id, key)
            self.store.consume_quota(
                call["tenant_id"], call["connector_id"],
                call["resource"], "write", now.date().isoformat(),
            )
            fresh_session = self.store.get_session(session_id)
            post_version = fresh_session["version"]
            self.store.mark_receipt_received(
                key, receipt.external_id,
                {"duplicate": receipt.duplicate, **receipt.payload}, now,
            )
            self.store.update_call(
                call_id, status="completed", post_version=post_version,
                attempts=call["attempts"] + 1,
                result={"external_id": receipt.external_id,
                        "duplicate": receipt.duplicate},
            )
            self.store.append_event(
                "receipt.received", session_id,
                {"call_id": call_id, "idempotency_key": key,
                 "external_id": receipt.external_id,
                 "duplicate": receipt.duplicate,
                 "post_version": post_version},
                now, actor_id, session_id=session_id, version=post_version,
            )
            self.store.append_event(
                "call.completed", session_id,
                {"call_id": call_id, "post_version": post_version},
                now, actor_id, session_id=session_id, version=post_version,
            )
            return {
                "call_id": call_id,
                "idempotency_key": key,
                "external_id": receipt.external_id,
                "duplicate": receipt.duplicate,
                "post_version": post_version,
            }

    def _replay_receipt(self, receipt_row: Any, actor_id: str,
                        now: datetime) -> dict[str, Any]:
        """命中已核销回执：直接返回原回执，登记 call.replayed，不触碰外部。"""
        replay_call_id = new_id("call")
        call = self.store.get_call(receipt_row["call_id"])
        self.store.insert_call(
            {
                "call_id": replay_call_id,
                "session_id": call["session_id"],
                "tenant_id": receipt_row["tenant_id"],
                "connector_id": receipt_row["connector_id"],
                "resource": call["resource"], "action": "write", "data_scope": "",
                "is_write": True,
                "idempotency_key": receipt_row["idempotency_key"],
                "status": "replayed",
                "result": {"external_id": receipt_row["external_id"]},
            },
            now,
        )
        self.store.append_event(
            "call.replayed", call["session_id"],
            {"call_id": replay_call_id,
             "original_call_id": receipt_row["call_id"],
             "idempotency_key": receipt_row["idempotency_key"],
             "external_id": receipt_row["external_id"]},
            now, actor_id, session_id=call["session_id"],
            version=self.store.get_session(call["session_id"])["version"],
        )
        return {
            "call_id": replay_call_id,
            "idempotency_key": receipt_row["idempotency_key"],
            "external_id": receipt_row["external_id"],
            "duplicate": True,
            "replayed": True,
        }

    # ------------------------------------------------------------------
    # 7. 服务恢复：进行中会话继续，待核销回执凭原幂等键对账
    # ------------------------------------------------------------------
    def recover(self, actor_id: str = "system",
                now: datetime | None = None) -> dict[str, Any]:
        now = now or _now()
        reconciled: list[dict[str, Any]] = []
        with self._tx():
            pending = self.store.pending_receipts()
            session_ids = {
                self.store.get_call(r["call_id"])["session_id"] for r in pending
            }
            for session_id in session_ids:
                self.store.update_session_status(session_id, "reconciling", now)
                rec_version = self.store.bump_version(session_id, now)
                self.store.append_event(
                    "session.recovered", session_id,
                    {"note": "服务恢复，存在待核销回执，进入对账"},
                    now, actor_id, session_id=session_id, version=rec_version,
                )

        for receipt in pending:
            call = self.store.get_call(receipt["call_id"])
            with self._tx():
                connector = self._connector(receipt["connector_id"])
                # 外部服务端按幂等键去重：若崩溃前已送达，返回的是同一张原始回执
                ext = connector.send(call["resource"], json_loads(call["payload"]),
                                     receipt["idempotency_key"])
                self.store.mark_receipt_received(
                    receipt["idempotency_key"], ext.external_id,
                    {"duplicate": ext.duplicate, **ext.payload}, now,
                    increment_attempt=False,
                )
                self.store.update_call(
                    receipt["call_id"], status="completed",
                    result={"external_id": ext.external_id,
                            "duplicate": ext.duplicate},
                )
                self.store.append_event(
                    "receipt.received", call["session_id"],
                    {"call_id": receipt["call_id"],
                     "idempotency_key": receipt["idempotency_key"],
                     "external_id": ext.external_id,
                     "duplicate": ext.duplicate, "during": "recovery"},
                    now, actor_id, session_id=call["session_id"],
                    version=self.store.get_session(call["session_id"])["version"],
                )

        with self._tx():
            for receipt in pending:
                self.store.mark_receipt_reconciled(receipt["idempotency_key"], now)
                sid = self.store.get_call(receipt["call_id"])["session_id"]
                final_version = self.store.get_session(sid)["version"]
                self.store.append_event(
                    "receipt.reconciled", sid,
                    {"call_id": receipt["call_id"],
                     "idempotency_key": receipt["idempotency_key"]},
                    now, actor_id, session_id=sid, version=final_version,
                )
                fresh = self.store.get_receipt(receipt["idempotency_key"])
                reconciled.append({
                    "call_id": receipt["call_id"],
                    "idempotency_key": receipt["idempotency_key"],
                    "external_id": fresh["external_id"],
                })
                status = "restricted" if self.store.has_non_granted(sid) else "consented"
                self.store.update_session_status(sid, status, now)

        return {"reconciled_receipts": reconciled,
                "sessions": sorted(
                    {self.store.get_call(r["call_id"])["session_id"] for r in pending}
                )}

    def close_session(self, session_id: str, actor_id: str,
                      now: datetime | None = None) -> None:
        now = now or _now()
        with self._tx():
            session = self._require_session(session_id)
            if session["status"] == "closed":
                raise InvalidTransition("会话已经关闭")
            pending = [
                r for r in self.store.pending_receipts()
                if self.store.get_call(r["call_id"])["session_id"] == session_id
            ]
            if pending:
                raise InvalidTransition("仍有待核销回执，请先对账再关闭会话")
            self.store.update_session_status(session_id, "closed", now)
            self.store.append_event(
                "session.closed", session_id, {}, now, actor_id,
                session_id=session_id, version=session["version"],
            )

    # ------------------------------------------------------------------
    # 8. 管理视图：请求范围 / 实际使用 / 拒绝原因 / 剩余额度
    # ------------------------------------------------------------------
    def set_quota(self, tenant_id: str, connector_id: str, resource: str,
                  action: str, daily_limit: int) -> None:
        self.store.set_quota(tenant_id, connector_id, resource, action, daily_limit)

    def session_view(self, session_id: str,
                     now: datetime | None = None) -> dict[str, Any]:
        now = now or _now()
        with self.store.lock():
            session = self.store.get_session(session_id)
            if session is None:
                raise InvalidTransition(f"会话不存在：{session_id}")
            bucket = now.date().isoformat()
            requested = [
                {
                    "resource": d["resource"], "action": d["action"],
                    "data_scope": d["data_scope"],
                    "valid_from": d["valid_from"], "valid_to": d["valid_to"],
                    "demand_status": d["status"],
                }
                for d in self.store.demands(session_id)
            ]
            capabilities = []
            for c in self.store.capabilities(session_id):
                remaining = self.store.quota_remaining(
                    session["tenant_id"], session["connector_id"],
                    c["resource"], c["action"], bucket,
                )
                capabilities.append({
                    "resource": c["resource"], "action": c["action"],
                    "data_scope": c["data_scope"],
                    "valid_from": c["valid_from"], "valid_to": c["valid_to"],
                    "status": c["status"], "reason": c["reason"],
                    "granted_version": c["granted_version"],
                    "status_version": c["status_version"],
                    "quota_remaining": remaining,
                })
            calls = []
            usage_rows = 0
            usage_pages = 0
            denials = []
            for call in self.store.calls_of_session(session_id):
                pages = self.store.pages_audit(call["call_id"])
                delivered_rows = sum(p["row_count"] for p in pages if p["delivered"])
                usage_rows += delivered_rows
                usage_pages += sum(1 for p in pages if p["delivered"])
                entry = {
                    "call_id": call["call_id"], "resource": call["resource"],
                    "action": call["action"], "data_scope": call["data_scope"],
                    "is_write": bool(call["is_write"]),
                    "idempotency_key": call["idempotency_key"],
                    "status": call["status"], "deny_reason": call["deny_reason"],
                    "pre_version": call["pre_version"],
                    "post_version": call["post_version"],
                    "attempts": call["attempts"],
                    "pages_fetched": len(pages),
                    "rows_delivered": delivered_rows,
                }
                calls.append(entry)
                if call["status"] in ("denied", "stopped"):
                    denials.append({
                        "call_id": call["call_id"],
                        "resource": call["resource"], "action": call["action"],
                        "data_scope": call["data_scope"],
                        "reason": call["deny_reason"],
                    })
            pending_receipts = [
                {
                    "call_id": r["call_id"],
                    "idempotency_key": r["idempotency_key"],
                    "attempts": r["attempts"],
                }
                for r in self.store.pending_receipts()
                if self.store.get_call(r["call_id"])["session_id"] == session_id
            ]
            events = self.store.events(session_id)
        return {
            "session_id": session_id,
            "tenant_id": session["tenant_id"],
            "connector_id": session["connector_id"],
            "owner_id": session["owner_id"],
            "task_id": session["task_id"],
            "status": session["status"],
            "version": session["version"],
            "requested_scope": requested,
            "capabilities": capabilities,
            "usage": {
                "calls": len(calls),
                "delivered_pages": usage_pages,
                "delivered_rows": usage_rows,
                "external_writes": sum(1 for c in calls if c["is_write"]
                                       if c["status"] == "completed"),
            },
            "denials": denials,
            "pending_receipts": pending_receipts,
            "audit_events": [
                {"type": e["event_type"], "version": e["version"],
                 "at": e["occurred_at"], "actor": e["actor_id"]}
                for e in events
            ],
        }

    def audit_trail(self, session_id: str) -> list[dict[str, Any]]:
        with self.store.lock():
            return self.store.events(session_id)

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------
    def _demanded_caps(self, session_id: str) -> list[Capability]:
        return [
            Capability(
                resource=d["resource"], action=d["action"],
                data_scope=d["data_scope"],
                valid_from=datetime.fromisoformat(d["valid_from"]),
                valid_to=datetime.fromisoformat(d["valid_to"]),
            )
            for d in self.store.demands(session_id)
            if d["status"] in ("requested", "granted")
        ]

    def _require_session(self, session_id: str) -> Any:
        session = self.store.get_session(session_id)
        if session is None:
            raise InvalidTransition(f"会话不存在：{session_id}")
        return session

    def _require_live_session(self, session_id: str) -> Any:
        session = self._require_session(session_id)
        if session["status"] == "closed":
            raise InvalidTransition("会话已关闭")
        if session["status"] == "proposed":
            raise InvalidTransition("会话尚未获得所有者确认")
        if session["status"] == "reconciling":
            raise AuthorizationDenied(SESSION_RECONCILING, "会话正在对账中")
        return session

    def _deny(self, code: str, detail: str, session_id: str, connector_id: str,
              tenant_id: str, resource: str, action: str, data_scope: str,
              actor_id: str, now: datetime, call_id: str,
              is_write: bool, idempotency_key: str | None,
              existing_call: Any | None, version: int | None = None) -> AuthorizationDenied:
        """记录拒绝（新调用行或更新翻页中的调用），登记 call.denied 事件，并返回异常。"""
        if existing_call is None:
            self.store.insert_call(
                {
                    "call_id": call_id, "session_id": session_id,
                    "tenant_id": tenant_id, "connector_id": connector_id,
                    "resource": resource, "action": action,
                    "data_scope": data_scope, "is_write": is_write,
                    "idempotency_key": idempotency_key,
                    "status": "denied", "deny_reason": code,
                },
                now,
            )
        else:
            stopped = "stopped" if existing_call["status"] == "in_flight" else "denied"
            self.store.update_call(call_id, status=stopped, deny_reason=code)
        self.store.append_event(
            "call.denied", session_id,
            {"call_id": call_id, "reason": code, "detail": detail,
             "resource": resource, "action": action, "data_scope": data_scope},
            now, actor_id, session_id=session_id, version=version,
        )
        return AuthorizationDenied(code, detail)

    def _pre_check(self, session_id: str, resource: str, action: str,
                   data_scope: str, now: datetime, call_id: str, actor_id: str,
                   existing_call: Any | None = None, write_payload: Any = None,
                   idempotency_key: str | None = None) -> tuple[Any, int]:
        """调用前统一校验。通过返回 (session, version)；拒绝时落审计并抛出异常。"""
        is_write = action == "write"
        session = self.store.get_session(session_id)
        if session is None:
            raise InvalidTransition(f"会话不存在：{session_id}")

        def deny(code: str, detail: str, version: int | None = None) -> AuthorizationDenied:
            return self._deny(
                code, detail, session_id, session["connector_id"],
                session["tenant_id"], resource, action, data_scope, actor_id, now,
                call_id, is_write, idempotency_key, existing_call,
                session["version"] if version is None else version,
            )

        status = session["status"]
        if status == "closed":
            raise deny(SESSION_CLOSED, "会话已关闭")
        if status == "proposed":
            raise deny(SESSION_NOT_ACTIVE, "会话尚未获得所有者确认")
        if status == "reconciling":
            raise deny(SESSION_RECONCILING, "会话正在对账中")

        if is_write:
            cap_row = self._find_write_capability(session_id, resource)
        else:
            cap_row = self.store.find_capability(
                session_id, resource, action, data_scope
            )
        if cap_row is None:
            demanded = any(
                d["resource"] == resource and d["action"] == action
                and d["data_scope"] == data_scope
                for d in self.store.demands(session_id)
            )
            if demanded:
                raise deny(EXPANSION_UNCONFIRMED, "扩大的范围尚未经资源所有者重新确认")
            raise deny(CAPABILITY_NOT_GRANTED,
                       f"未授予 {resource}/{action}/{data_scope}")
        if cap_row["status"] == "isolated":
            raise deny(CAPABILITY_ISOLATED,
                       f"能力已被管理员隔离：{cap_row['reason'] or ''}")
        if cap_row["status"] == "revoked":
            raise deny(CAPABILITY_REVOKED, "能力已撤销")

        if not _within_window(cap_row, now):
            valid_from = datetime.fromisoformat(cap_row["valid_from"])
            valid_to = datetime.fromisoformat(cap_row["valid_to"])
            if now < valid_from:
                raise deny(NOT_YET_VALID, f"能力在 {valid_from.isoformat()} 后才生效")
            raise deny(EXPIRED, f"能力已于 {valid_to.isoformat()} 失效")

        remaining = self.store.quota_remaining(
            session["tenant_id"], session["connector_id"], resource, action,
            now.date().isoformat(),
        )
        if remaining is not None and remaining <= 0:
            raise deny(QUOTA_EXHAUSTED, f"{resource}/{action} 当日额度已用尽",
                       version=session["version"])
        return session, session["version"]

    def _find_write_capability(self, session_id: str, resource: str) -> Any:
        """写操作匹配 resource 下任意 action 以 write/send 结尾且在有效期内的能力。"""
        for row in self.store.capabilities(session_id):
            if row["resource"] != resource:
                continue
            if row["action"] in ("write", "send") or row["action"].endswith(".write") \
                    or row["action"].endswith(".send"):
                return row
        return None


def json_loads(raw: str | None) -> Any:
    import json
    return json.loads(raw) if raw else None
