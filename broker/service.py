"""连接器会话代理核心服务。

职责覆盖场景中的全部要求：

- 任务提交时按资源、动作、数据范围、有效时间计算最小能力集合；
- 扩大授权必须由资源所有者重新确认，且授予不得超出最小集合；
- 每次外部调用前后都校验会话版本与能力状态；
- 分页读取中授权收缩：已返回数据保留审计，未领取的页立即停止；
- 写操作先落 ``write.requested`` 预留点再调用外部，幂等键在
  租户 + 连接器范围内唯一；结果未知时只能对账，禁止重发；
- 管理员可隔离单个能力、撤销单项资源而不影响其他合法任务；
- 管理员视图给出请求范围、实际使用、拒绝原因与剩余额度；
- 所有状态来自追加事件流，重启后重放恢复，待核销回执继续对账。
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional

from .catalog import (
    READ,
    WRITE,
    Capability,
    DataScope,
    Demand,
    GrantSpec,
    minimum_capabilities,
)
from .connectors import Connector, ExternalReceipt, Page
from .errors import (
    AuthorizationError,
    Denial,
    DenialReason,
    DuplicateDeliveryError,
    IdempotencyPending,
    RemoteUnavailable,
)
from .storage import EventStore, utc_now

SESSION_TYPE = "session"


def _iso(value: datetime) -> str:
    return value.isoformat()


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _demand_to_dict(demand: Demand) -> dict[str, Any]:
    return {
        "resource": demand.resource,
        "action": demand.action,
        "scope": demand.scope.to_dict(),
        "valid_until": _iso(demand.valid_until),
    }


def _demand_from_dict(raw: dict[str, Any]) -> Demand:
    return Demand(
        resource=raw["resource"],
        action=raw["action"],
        scope=DataScope.from_dict(raw["scope"]),
        valid_until=_dt(raw["valid_until"]),
    )


def _spec_from_demand_dict(raw: dict[str, Any]) -> GrantSpec:
    demand = _demand_from_dict(raw)
    return GrantSpec(demand.resource, demand.action, demand.scope, demand.valid_until)


@dataclass
class _CapabilityState:
    code: str
    resource: str
    action: str
    scope: DataScope
    valid_until: datetime
    version: int
    status: str = "granted"  # granted / restricted / isolated / revoked
    reads: int = 0
    items_requested: int = 0
    items_delivered: int = 0
    writes: int = 0
    quota_limit: Optional[int] = None
    quota_used: int = 0

    @property
    def quota_remaining(self) -> Optional[int]:
        if self.quota_limit is None:
            return None
        return self.quota_limit - self.quota_used

    @property
    def granted(self) -> GrantSpec:
        return GrantSpec(self.resource, self.action, self.scope, self.valid_until)

    def as_capability(self) -> Capability:
        return Capability(
            code=self.code,
            resource=self.resource,
            action=self.action,
            scope=self.scope,
            valid_until=self.valid_until,
            version=self.version,
        )


@dataclass
class _PendingWrite:
    call_id: str
    capability_code: Optional[str]
    resource: str
    idempotency_key: str
    payload: dict[str, Any]
    requested_at: str
    ambiguous: bool = False


@dataclass
class _DeliveredWrite:
    receipt: ExternalReceipt
    resource: str
    payload: dict[str, Any]


@dataclass
class _SessionState:
    session_id: str
    tenant_id: str = ""
    connector_code: str = ""
    owner: str = ""
    task_id: str = ""
    version: int = 0
    closed: bool = False
    demands: list[Demand] = field(default_factory=list)
    quotas: dict[tuple[str, str], int] = field(default_factory=dict)
    pending_expansion: list[GrantSpec] = field(default_factory=list)
    capabilities: dict[str, _CapabilityState] = field(default_factory=dict)
    denials: list[Denial] = field(default_factory=list)
    pending_writes: dict[str, _PendingWrite] = field(default_factory=dict)
    delivered: dict[str, _DeliveredWrite] = field(default_factory=dict)
    retained_audit: list[dict[str, Any]] = field(default_factory=list)
    cap_seq: int = 0

    @property
    def status(self) -> str:
        if self.closed:
            return "closed"
        if not self.capabilities:
            return "proposed"
        if self.pending_writes:
            return "reconciling"
        if any(c.status == "isolated" for c in self.capabilities.values()):
            return "isolated"
        if any(c.status in ("restricted", "revoked") for c in self.capabilities.values()):
            return "restricted"
        return "active"

    def cap_for(self, resource: str, action: str) -> Optional[_CapabilityState]:
        for cap in self.capabilities.values():
            if cap.resource == resource and cap.action == action and cap.status != "revoked":
                return cap
        return None


@dataclass(frozen=True, slots=True)
class PendingWrite:
    """写操作提交结果：已送达带回执，或结果未知待对账。"""

    idempotency_key: str
    status: str  # delivered / pending / ambiguous
    call_id: str
    receipt: Optional[ExternalReceipt] = None


class Reader:
    """分页读取器。

    - 每一页领取前后各做一次授权与会话版本校验；
    - 授权在两次校验之间收缩时，本页已返回数据写入审计（不交给任务），
      后续页不再向连接器领取，读取器永久停止；
    - 连接器故障时页游标不前进，调用方可安全重试同一页。
    """

    def __init__(self, broker: "Broker", session_id: str, capability_code: str) -> None:
        self._broker = broker
        self._session_id = session_id
        self._capability_code = capability_code
        self._page_token: Optional[str] = None
        self._stopped = False
        self._finished = False
        self.pages_claimed = 0
        self.stop_reason: Optional[DenialReason] = None

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def stopped(self) -> bool:
        return self._stopped

    def next_page(self, expected_session_version: int) -> list[dict[str, Any]]:
        if self._stopped:
            raise AuthorizationError(self.stop_reason or DenialReason.CAPABILITY_REVOKED, "读取已停止")
        if self._finished:
            return []
        broker = self._broker
        session = broker._session(self._session_id)
        cap, call_id = broker._pre_authorize_read(
            session, self._capability_code, expected_session_version
        )
        snapshot = (cap.version, cap.status, cap.scope, session.version)
        try:
            page: Page = broker._connector(session).list_page(cap.resource, self._page_token)
        except RemoteUnavailable:
            # 页游标不动，下次重试仍是同一页；GET 语义天然幂等。
            raise
        shrink = broker._post_check(session, cap, snapshot, expected_session_version)
        if shrink is not None:
            # 已返回的数据保留审计，任务侧拿不到；尚未领取的页立即停止。
            broker._retain_page(session, cap, call_id, page.items, shrink)
            self._stopped = True
            self.stop_reason = shrink
            raise AuthorizationError(shrink, f"读取途中授权变更，{len(page.items)} 条数据已留存审计")
        items = broker._project(cap.scope, page.items)
        if cap.quota_remaining is not None and len(items) > cap.quota_remaining:
            broker._deny(
                session,
                DenialReason.QUOTA_EXCEEDED,
                f"本页{len(items)}条超出剩余额度{cap.quota_remaining}",
                cap.code,
            )
            self._stopped = True
            self.stop_reason = DenialReason.QUOTA_EXCEEDED
            raise AuthorizationError(
                DenialReason.QUOTA_EXCEEDED,
                f"本页{len(items)}条超出剩余额度{cap.quota_remaining}",
            )
        broker._complete_read(session, cap, call_id, page.items, items)
        self.pages_claimed += 1
        self._page_token = page.next_page_token
        if page.next_page_token is None:
            self._finished = True
        return items

    def iter_pages(self, expected_session_version: int):
        while not self._finished and not self._stopped:
            yield self.next_page(expected_session_version)


class Broker:
    """会话代理服务。"""

    def __init__(
        self,
        store: EventStore,
        connectors: dict[str, Connector],
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._store = store
        self._connectors = dict(connectors)
        self._clock = clock
        self._sessions: dict[str, _SessionState] = {}
        self._recover()

    # ---------------------------------------------------------------- 恢复

    def _recover(self) -> None:
        """重放全部事件，恢复会话与待核销写操作。"""
        for session_id in self._store.list_aggregates(SESSION_TYPE):
            session = _SessionState(session_id=session_id)
            for event in self._store.stream(SESSION_TYPE, session_id):
                self._apply(session, event.event_type, event.payload)
            self._sessions[session_id] = session

    # ----------------------------------------------------- 任务与最小授权

    def submit_task(
        self,
        tenant_id: str,
        session_id: str,
        task_id: str,
        connector_code: str,
        owner: str,
        demands: list[Demand],
        quotas: Optional[dict[tuple[str, str], int]] = None,
        actor: str = "task-submitter",
    ) -> list[GrantSpec]:
        """提交任务并计算最小能力集合（会话进入 proposed，等待所有者确认）。"""
        if session_id in self._sessions:
            raise ValueError(f"会话已存在：{session_id}")
        if connector_code not in self._connectors:
            raise ValueError(f"未登记的连接器：{connector_code}")
        grants = minimum_capabilities(demands)
        session = _SessionState(
            session_id=session_id,
            tenant_id=tenant_id,
            connector_code=connector_code,
            owner=owner,
            task_id=task_id,
        )
        self._sessions[session_id] = session
        quota_entries = [
            {"resource": r, "action": a, "limit": limit}
            for (r, a), limit in (quotas or {}).items()
        ]
        self._append(
            session,
            "demand.calculated",
            {
                "tenant_id": tenant_id,
                "connector_code": connector_code,
                "owner": owner,
                "task_id": task_id,
                "demands": [_demand_to_dict(d) for d in demands],
                "grants": [
                    _demand_to_dict(Demand(g.resource, g.action, g.scope, g.valid_until))
                    for g in grants
                ],
                "quotas": quota_entries,
            },
            actor,
        )
        return grants

    def request_consent_view(self, session_id: str) -> list[GrantSpec]:
        """资源所有者待确认的最小授权清单。"""
        session = self._session(session_id)
        return minimum_capabilities(session.demands)

    def confirm_consent(
        self,
        session_id: str,
        accepted: Optional[list[GrantSpec]] = None,
        actor: str = "",
    ) -> list[Capability]:
        """资源所有者确认授权。

        接受的集合必须与最小能力集合逐项一致：多给（grant_not_minimal）、
        少给（capability_missing）都被拒绝。
        """
        session = self._session(session_id)
        actor = actor or session.owner
        if session.capabilities:
            raise AuthorizationError(DenialReason.GRANT_NOT_MINIMAL, "会话已完成授权确认")
        proposed = minimum_capabilities(session.demands)
        accepted = accepted if accepted is not None else proposed
        self._assert_exact_minimum(accepted, proposed)
        capabilities: list[Capability] = []
        payload_caps = []
        for spec in proposed:
            cap = self._create_capability(session, spec)
            capabilities.append(cap.as_capability())
            payload_caps.append(self._cap_payload(cap))
        session.version = 1
        self._append(
            session,
            "consent.confirmed",
            {"capabilities": payload_caps, "session_version": 1},
            actor,
        )
        return capabilities

    def add_demands(
        self, session_id: str, demands: list[Demand], actor: str = "task-submitter"
    ) -> list[GrantSpec]:
        """任务追加需求。返回需要所有者重新确认的新增最小授权。

        已有能力能覆盖的需求直接合并；任何扩大（新资源/动作/类别/更晚时间）
        都进入 ``pending_expansion``，在所有者重新确认前，相关调用以
        ``resource_owner_reconfirm_required`` 拒绝。
        """
        session = self._session(session_id)
        now = self._clock()
        new_grants = minimum_capabilities(demands)
        missing: list[GrantSpec] = []
        for spec in new_grants:
            covered = False
            probe = Demand(spec.resource, spec.action, spec.scope, spec.valid_until)
            for cap in session.capabilities.values():
                if cap.status in ("revoked", "isolated"):
                    continue
                if cap.granted.covers(probe, now):
                    covered = True
                    break
            if not covered:
                missing.append(spec)
        session.pending_expansion = missing
        self._append(
            session,
            "demand.calculated",
            {
                "demands": [_demand_to_dict(d) for d in demands],
                "grants": [
                    _demand_to_dict(Demand(g.resource, g.action, g.scope, g.valid_until))
                    for g in new_grants
                ],
                "missing": [
                    _demand_to_dict(Demand(g.resource, g.action, g.scope, g.valid_until))
                    for g in missing
                ],
            },
            actor,
        )
        return missing

    def confirm_expansion(self, session_id: str, actor: str = "") -> list[Capability]:
        """资源所有者重新确认扩大范围；只能补齐最小集合，不能夹带额外能力。"""
        session = self._session(session_id)
        actor = actor or session.owner
        if not session.pending_expansion:
            raise AuthorizationError(DenialReason.GRANT_NOT_MINIMAL, "没有待确认的扩权请求")
        added: list[Capability] = []
        payload_caps = []
        next_version = session.version + 1
        for spec in session.pending_expansion:
            cap = self._create_capability(session, spec, version=next_version)
            added.append(cap.as_capability())
            payload_caps.append(self._cap_payload(cap))
        session.pending_expansion = []
        session.version = next_version
        self._append(
            session,
            "consent.confirmed",
            {"capabilities": payload_caps, "expansion": True, "session_version": next_version},
            actor,
        )
        return added

    def _create_capability(
        self, session: _SessionState, spec: GrantSpec, version: Optional[int] = None
    ) -> _CapabilityState:
        session.cap_seq += 1
        code = f"cap-{session.cap_seq:03d}"
        cap = _CapabilityState(
            code=code,
            resource=spec.resource,
            action=spec.action,
            scope=spec.scope,
            valid_until=spec.valid_until,
            version=version if version is not None else session.version + 1,
            quota_limit=session.quotas.get((spec.resource, spec.action)),
        )
        session.capabilities[code] = cap
        return cap

    # ------------------------------------------------------------- 读取

    def start_read(
        self, session_id: str, resource: str, expected_session_version: int
    ) -> Reader:
        """打开一个分页读取器；具体授权在每页领取时校验。"""
        session = self._session(session_id)
        if expected_session_version != session.version:
            self._deny(
                session,
                DenialReason.SESSION_VERSION_STALE,
                f"持有版本 {expected_session_version}，当前版本 {session.version}",
                None,
            )
            raise AuthorizationError(
                DenialReason.SESSION_VERSION_STALE,
                f"持有版本 {expected_session_version}，当前版本 {session.version}",
            )
        cap = session.cap_for(resource, READ)
        if cap is None:
            if any(
                g.resource == resource and g.action == READ
                for g in session.pending_expansion
            ):
                reason = DenialReason.RESOURCE_OWNER_RECONFIRM_REQUIRED
                detail = f"扩大到 {resource}:read 需要资源所有者重新确认"
            else:
                reason = DenialReason.CAPABILITY_MISSING
                detail = f"缺少 {resource}:read 能力"
            self._deny(session, reason, detail, None)
            raise AuthorizationError(reason, detail)
        # 打开读取器时立即校验一次能力状态（过期/隔离/撤销）。
        self._check_common(session, cap.code, expected_session_version)
        return Reader(self, session_id, cap.code)

    def _pre_authorize_read(
        self, session: _SessionState, code: str, expected_version: int
    ) -> tuple[_CapabilityState, str]:
        cap = self._check_common(session, code, expected_version)
        if cap.quota_remaining is not None and cap.quota_remaining <= 0:
            self._deny(session, DenialReason.QUOTA_EXCEEDED, "剩余额度为 0", cap.code)
            raise AuthorizationError(DenialReason.QUOTA_EXCEEDED, "剩余额度为 0")
        call_id = f"call-{uuid.uuid4().hex[:12]}"
        self._append(
            session,
            "call.authorized",
            {
                "call_id": call_id,
                "capability_code": cap.code,
                "kind": READ,
                "session_version": session.version,
            },
            session.task_id,
        )
        return cap, call_id

    def _post_check(
        self,
        session: _SessionState,
        cap: _CapabilityState,
        snapshot: tuple[int, str, DataScope, int],
        expected_version: int,
    ) -> Optional[DenialReason]:
        """调用返回后的二次校验。返回非 None 表示授权在调用期间收缩。"""
        version_before, status_before, scope_before, session_version_before = snapshot
        if cap.status == "revoked" and status_before != "revoked":
            return DenialReason.CAPABILITY_REVOKED
        if cap.status == "isolated" and status_before != "isolated":
            return DenialReason.CAPABILITY_ISOLATED
        if cap.version != version_before or cap.scope != scope_before:
            return DenialReason.SCOPE_EXCEEDED
        if session.version != session_version_before or expected_version != session.version:
            return DenialReason.SESSION_VERSION_STALE
        if self._clock() >= cap.valid_until:
            return DenialReason.EXPIRED
        return None

    def _project(self, scope: DataScope, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """资源投影：即使连接器多返回数据，也只放行授权范围内的条目。"""
        if scope.allowed is None:
            return list(items)
        return [item for item in items if item.get(scope.field) in scope.allowed]

    def _complete_read(
        self,
        session: _SessionState,
        cap: _CapabilityState,
        call_id: str,
        raw_items: list[dict[str, Any]],
        items: list[dict[str, Any]],
    ) -> None:
        self._append(
            session,
            "call.completed",
            {
                "call_id": call_id,
                "capability_code": cap.code,
                "kind": READ,
                "raw_count": len(raw_items),
                "delivered_count": len(items),
                "quota_remaining": cap.quota_remaining,
            },
            session.task_id,
        )

    def _retain_page(
        self,
        session: _SessionState,
        cap: _CapabilityState,
        call_id: str,
        items: list[dict[str, Any]],
        reason: DenialReason,
    ) -> None:
        record = {
            "call_id": call_id,
            "capability_code": cap.code,
            "reason": reason.value,
            "retained_at": _iso(self._clock()),
            "item_count": len(items),
            "items": items,
        }
        self._append(session, "page.retained", record, session.task_id)
        self._deny(session, reason, f"读取途中授权收缩，{len(items)} 条已留存审计", cap.code)

    # ------------------------------------------------------------- 写入

    def submit_write(
        self,
        session_id: str,
        resource: str,
        payload: dict[str, Any],
        idempotency_key: str,
        expected_session_version: int,
    ) -> PendingWrite:
        """提交一次写操作。

        幂等键在租户 + 连接器范围内唯一：

        - 同键同体重放且已有回执：直接返回原回执，不再次发送；
        - 同键同体但结果未知：抛 :class:`IdempotencyPending`，只能对账；
        - 同键不同体：抛 :class:`DuplicateDeliveryError`，拒绝写入。
        """
        session = self._session(session_id)

        # 已见过的幂等键优先处理：任何情况下都不允许产生第二次外部发送。
        replay = self._lookup_idempotency(session, idempotency_key, payload)
        if replay is not None:
            return replay

        cap = session.cap_for(resource, WRITE)
        if cap is None:
            if any(
                g.resource == resource and g.action == WRITE
                for g in session.pending_expansion
            ):
                reason = DenialReason.RESOURCE_OWNER_RECONFIRM_REQUIRED
                detail = f"扩大到 {resource}:write 需要资源所有者重新确认"
            else:
                reason = DenialReason.CAPABILITY_MISSING
                detail = f"缺少 {resource}:write 能力"
            self._deny(session, reason, detail, None)
            raise AuthorizationError(reason, detail)
        self._check_common(session, cap.code, expected_session_version)

        if cap.scope.allowed is not None:
            value = payload.get(cap.scope.field)
            if value not in cap.scope.allowed:
                self._deny(
                    session,
                    DenialReason.SCOPE_EXCEEDED,
                    f"写入对象 {cap.scope.field}={value!r} 超出授权范围",
                    cap.code,
                )
                raise AuthorizationError(
                    DenialReason.SCOPE_EXCEEDED,
                    f"写入对象 {cap.scope.field}={value!r} 超出授权范围",
                )
        if cap.quota_remaining is not None and cap.quota_remaining <= 0:
            self._deny(session, DenialReason.QUOTA_EXCEEDED, "剩余额度为 0", cap.code)
            raise AuthorizationError(DenialReason.QUOTA_EXCEEDED, "剩余额度为 0")

        call_id = f"call-{uuid.uuid4().hex[:12]}"
        at = _iso(self._clock())
        body = dict(payload)
        # 预留点先落库：崩溃恢复后据此判定“可能已发送”，只能对账。
        self._append(
            session,
            "call.authorized",
            {
                "call_id": call_id,
                "capability_code": cap.code,
                "kind": WRITE,
                "session_version": session.version,
                "idempotency_key": idempotency_key,
            },
            session.task_id,
        )
        self._append(
            session,
            "write.requested",
            {
                "call_id": call_id,
                "capability_code": cap.code,
                "resource": resource,
                "idempotency_key": idempotency_key,
                "payload": body,
                "requested_at": at,
            },
            session.task_id,
        )
        try:
            receipt = self._connector(session).deliver_write(
                resource, idempotency_key, {**body, "_at": at}
            )
        except RemoteUnavailable:
            raise IdempotencyPending(
                f"写请求 {idempotency_key} 结果未知，禁止重发，请调用 reconcile_writes 对账"
            )
        return self._accept_receipt(session, cap, call_id, idempotency_key, resource, body, receipt)

    def reconcile_writes(self, session_id: Optional[str] = None) -> dict[str, int]:
        """核销待确认写操作。服务恢复后可反复调用直到没有 pending。"""
        session_ids = [session_id] if session_id else list(self._sessions)
        delivered = pending = ambiguous = 0
        for sid in session_ids:
            session = self._sessions.get(sid)
            if session is None or session.closed:
                continue
            connector = self._connectors.get(session.connector_code)
            for key, item in list(session.pending_writes.items()):
                if connector is None:
                    pending += 1
                    continue
                try:
                    receipt = connector.lookup_delivery(item.resource, key)
                except RemoteUnavailable:
                    pending += 1
                    continue
                if receipt is None:
                    pending += 1
                    continue
                cap = session.capabilities.get(item.capability_code or "")
                self._accept_receipt(
                    session, cap, item.call_id, key, item.resource, item.payload, receipt
                )
                delivered += 1
        return {"delivered": delivered, "pending": pending, "ambiguous": ambiguous}

    def retry_unconfirmed_after_lookup(
        self, session_id: str, idempotency_key: str
    ) -> PendingWrite:
        """连接器确认从未收到该键时，安全补发一次（连接器侧按键去重）。

        若连接器仍报告“可能收到过”，保持 pending 并抛 IdempotencyPending，
        绝不盲目重发。
        """
        session = self._session(session_id)
        item = session.pending_writes.get(idempotency_key)
        if item is None:
            raise ValueError(f"没有待核销的写请求：{idempotency_key}")
        connector = self._connector(session)
        try:
            existing = connector.lookup_delivery(item.resource, idempotency_key)
        except RemoteUnavailable:
            raise IdempotencyPending("连接器不可用，无法确认是否已送达，保持待核销")
        if existing is not None:
            cap = session.capabilities.get(item.capability_code or "")
            return self._accept_receipt(
                session, cap, item.call_id, idempotency_key, item.resource, item.payload, existing
            )
        try:
            receipt = connector.deliver_write(
                item.resource, idempotency_key, {**item.payload, "_at": item.requested_at}
            )
        except RemoteUnavailable:
            raise IdempotencyPending("补发仍无回执，继续保持待核销")
        cap = session.capabilities.get(item.capability_code or "")
        return self._accept_receipt(
            session, cap, item.call_id, idempotency_key, item.resource, item.payload, receipt
        )

    def _accept_receipt(
        self,
        session: _SessionState,
        cap: Optional[_CapabilityState],
        call_id: str,
        idempotency_key: str,
        resource: str,
        body: dict[str, Any],
        receipt: ExternalReceipt,
    ) -> PendingWrite:
        session.delivered[idempotency_key] = _DeliveredWrite(
            receipt=receipt, resource=resource, payload=dict(body)
        )
        session.pending_writes.pop(idempotency_key, None)
        self._append(
            session,
            "receipt.received",
            {
                "call_id": call_id,
                "capability_code": cap.code if cap else None,
                "resource": resource,
                "idempotency_key": idempotency_key,
                "payload": body,
                "receipt": {
                    "receipt_id": receipt.receipt_id,
                    "external_ref": receipt.external_ref,
                    "status": receipt.status,
                    "received_at": receipt.received_at,
                },
            },
            session.task_id,
        )
        self._append(
            session,
            "call.completed",
            {
                "call_id": call_id,
                "capability_code": cap.code if cap else None,
                "kind": WRITE,
                "idempotency_key": idempotency_key,
            },
            session.task_id,
        )
        return PendingWrite(
            idempotency_key=idempotency_key,
            status="delivered",
            call_id=call_id,
            receipt=receipt,
        )

    def _lookup_idempotency(
        self, session: _SessionState, key: str, payload: dict[str, Any]
    ) -> Optional[PendingWrite]:
        """跨会话按 租户 + 连接器 + 幂等键 查重。"""
        for other in self._sessions.values():
            if (
                other.tenant_id != session.tenant_id
                or other.connector_code != session.connector_code
            ):
                continue
            delivered = other.delivered.get(key)
            if delivered is not None:
                if delivered.payload != payload:
                    raise DuplicateDeliveryError(
                        f"幂等键 {key} 已用于不同的请求体，拒绝写入"
                    )
                return PendingWrite(
                    idempotency_key=key,
                    status="delivered",
                    call_id=delivered.receipt.receipt_id,
                    receipt=delivered.receipt,
                )
            pending = other.pending_writes.get(key)
            if pending is not None:
                if pending.payload != payload:
                    raise DuplicateDeliveryError(
                        f"幂等键 {key} 已用于不同的请求体（且结果未知），拒绝写入"
                    )
                raise IdempotencyPending(
                    f"幂等键 {key} 结果未知，请先 reconcile_writes 对账，禁止重发"
                )
        return None

    # ----------------------------------------------------- 管理员处置

    def isolate_capability(
        self, session_id: str, code: str, reason: str, actor: str = "admin"
    ) -> None:
        """隔离单个能力：该能力立即拒绝所有调用，其他能力不受影响。"""
        session = self._session(session_id)
        cap = self._cap(session, code)
        cap.status = "isolated"
        session.version += 1
        cap.version = session.version
        self._append(
            session,
            "capability.isolated",
            {"capability_code": code, "reason": reason, "session_version": session.version},
            actor,
        )

    def revoke_capability(
        self, session_id: str, code: str, reason: str, actor: str = "admin"
    ) -> None:
        """撤销单项能力（某个资源），同会话其他合法任务继续运行。"""
        session = self._session(session_id)
        cap = self._cap(session, code)
        cap.status = "revoked"
        cap.scope = DataScope.nothing(cap.scope.field)
        session.version += 1
        cap.version = session.version
        self._append(
            session,
            "capability.revoked",
            {"capability_code": code, "reason": reason, "session_version": session.version},
            actor,
        )

    def reduce_capability_scope(
        self,
        session_id: str,
        code: str,
        new_scope: Optional[DataScope] = None,
        valid_until: Optional[datetime] = None,
        reason: str = "owner_reduced",
        actor: str = "admin",
    ) -> None:
        """收缩数据范围或提前有效时间；范围只许变小，不许借道扩大。"""
        session = self._session(session_id)
        cap = self._cap(session, code)
        if new_scope is not None:
            if not cap.scope.covers(new_scope):
                raise AuthorizationError(
                    DenialReason.GRANT_NOT_MINIMAL, "收缩后的范围反而更大，拒绝变更"
                )
            cap.scope = new_scope
        if valid_until is not None:
            if valid_until > cap.valid_until:
                raise AuthorizationError(
                    DenialReason.RESOURCE_OWNER_RECONFIRM_REQUIRED,
                    "延长有效期属于扩权，必须重新确认",
                )
            cap.valid_until = valid_until
        cap.status = "restricted"
        session.version += 1
        cap.version = session.version
        self._append(
            session,
            "scope.reduced",
            {
                "capability_code": code,
                "reason": reason,
                "scope": cap.scope.to_dict(),
                "valid_until": _iso(cap.valid_until),
                "session_version": session.version,
            },
            actor,
        )

    def close_session(self, session_id: str, reason: str = "finished", actor: str = "admin") -> None:
        session = self._session(session_id)
        session.closed = True
        session.version += 1
        self._append(
            session,
            "session.closed",
            {"reason": reason, "session_version": session.version},
            actor,
        )

    def admin_view(self, session_id: str) -> dict[str, Any]:
        """管理员接口：请求范围、实际使用、拒绝原因、剩余额度与会话状态。"""
        session = self._session(session_id)
        return {
            "session_id": session_id,
            "tenant_id": session.tenant_id,
            "connector": session.connector_code,
            "task_id": session.task_id,
            "status": session.status,
            "session_version": session.version,
            "owner": session.owner,
            "requested_scope": [_demand_to_dict(d) for d in session.demands],
            "capabilities": [
                {
                    "code": cap.code,
                    "resource": cap.resource,
                    "action": cap.action,
                    "scope": cap.scope.to_dict(),
                    "valid_until": _iso(cap.valid_until),
                    "version": cap.version,
                    "status": cap.status,
                    "usage": {
                        "reads": cap.reads,
                        "items_requested": cap.items_requested,
                        "items_delivered": cap.items_delivered,
                        "writes": cap.writes,
                    },
                    "quota": {
                        "limit": cap.quota_limit,
                        "used": cap.quota_used,
                        "remaining": cap.quota_remaining,
                    },
                }
                for cap in session.capabilities.values()
            ],
            "denials": [
                {
                    "reason": d.reason.value,
                    "detail": d.detail,
                    "at": d.at,
                    "capability_code": d.capability_code,
                }
                for d in session.denials
            ],
            "pending_writes": [
                {
                    "idempotency_key": k,
                    "requested_at": v.requested_at,
                    "ambiguous": v.ambiguous,
                }
                for k, v in session.pending_writes.items()
            ],
            "retained_audit": session.retained_audit,
        }

    def list_sessions(self) -> list[str]:
        return sorted(self._sessions)

    # ------------------------------------------------------------- 内部

    def _connector(self, session: _SessionState) -> Connector:
        connector = self._connectors.get(session.connector_code)
        if connector is None:
            raise RemoteUnavailable(f"连接器未登记：{session.connector_code}")
        return connector

    def _session(self, session_id: str) -> _SessionState:
        session = self._sessions.get(session_id)
        if session is None or session.closed:
            raise AuthorizationError(DenialReason.NO_SESSION, f"会话不存在或已关闭：{session_id}")
        return session

    def _cap(self, session: _SessionState, code: str) -> _CapabilityState:
        cap = session.capabilities.get(code)
        if cap is None:
            raise AuthorizationError(DenialReason.CAPABILITY_MISSING, f"能力不存在：{code}")
        return cap

    def _check_common(
        self, session: _SessionState, code: str, expected_version: int
    ) -> _CapabilityState:
        if expected_version != session.version:
            self._deny(
                session,
                DenialReason.SESSION_VERSION_STALE,
                f"持有版本 {expected_version}，当前版本 {session.version}",
                code,
            )
            raise AuthorizationError(
                DenialReason.SESSION_VERSION_STALE,
                f"持有版本 {expected_version}，当前版本 {session.version}",
            )
        cap = self._cap(session, code)
        now = self._clock()
        if now >= cap.valid_until:
            self._deny(session, DenialReason.EXPIRED, f"能力 {code} 已过有效期", code)
            raise AuthorizationError(DenialReason.EXPIRED, f"能力 {code} 已过有效期")
        if cap.status == "isolated":
            self._deny(session, DenialReason.CAPABILITY_ISOLATED, f"能力 {code} 已被隔离", code)
            raise AuthorizationError(DenialReason.CAPABILITY_ISOLATED, f"能力 {code} 已被隔离")
        if cap.status == "revoked":
            self._deny(session, DenialReason.CAPABILITY_REVOKED, f"能力 {code} 已被撤销", code)
            raise AuthorizationError(DenialReason.CAPABILITY_REVOKED, f"能力 {code} 已被撤销")
        return cap

    def _deny(
        self,
        session: _SessionState,
        reason: DenialReason,
        detail: str,
        capability_code: Optional[str],
    ) -> None:
        at = _iso(self._clock())
        denial = Denial(
            reason=reason, detail=detail, at=at, capability_code=capability_code
        )
        session.denials.append(denial)
        self._append(
            session,
            "call.denied",
            {
                "reason": reason.value,
                "detail": detail,
                "capability_code": capability_code,
                "at": at,
            },
            session.task_id or "system",
        )

    def _assert_exact_minimum(
        self, accepted: list[GrantSpec], proposed: list[GrantSpec]
    ) -> None:
        def norm(spec: GrantSpec) -> tuple[str, str, str, Optional[frozenset[str]], str]:
            return (
                spec.resource,
                spec.action,
                spec.scope.field,
                spec.scope.allowed,
                _iso(spec.valid_until),
            )

        accepted_set = {norm(s) for s in accepted}
        proposed_set = {norm(s) for s in proposed}
        extra = accepted_set - proposed_set
        missing = proposed_set - accepted_set
        if extra:
            raise AuthorizationError(
                DenialReason.GRANT_NOT_MINIMAL,
                f"授权超出最小集合：{len(extra)} 项",
            )
        if missing:
            raise AuthorizationError(
                DenialReason.CAPABILITY_MISSING,
                f"授权未覆盖最小集合：{len(missing)} 项",
            )

    def _cap_payload(self, cap: _CapabilityState) -> dict[str, Any]:
        return {
            "code": cap.code,
            "resource": cap.resource,
            "action": cap.action,
            "scope": cap.scope.to_dict(),
            "valid_until": _iso(cap.valid_until),
            "version": cap.version,
            "quota_limit": cap.quota_limit,
        }

    def _append(
        self, session: _SessionState, event_type: str, payload: dict[str, Any], actor: str
    ) -> None:
        self._store.append(
            event_id=f"evt-{uuid.uuid4().hex[:16]}",
            aggregate_type=SESSION_TYPE,
            aggregate_id=session.session_id,
            event_type=event_type,
            payload=payload,
            actor_id=actor,
            occurred_at=self._clock(),
        )
        self._apply(session, event_type, payload)

    # ------------------------------------------------------------- 重放

    def _apply(self, session: _SessionState, event_type: str, p: dict[str, Any]) -> None:
        if event_type == "demand.calculated":
            if not session.tenant_id:
                session.tenant_id = p["tenant_id"]
                session.connector_code = p["connector_code"]
                session.owner = p["owner"]
                session.task_id = p["task_id"]
            for entry in p.get("quotas", []):
                session.quotas[(entry["resource"], entry["action"])] = entry["limit"]
            session.demands.extend(_demand_from_dict(d) for d in p["demands"])
            if "missing" in p:
                session.pending_expansion = [_spec_from_demand_dict(g) for g in p["missing"]]
        elif event_type == "consent.confirmed":
            session.version = p["session_version"]
            session.pending_expansion = []
            for raw in p["capabilities"]:
                code = raw["code"]
                if code in session.capabilities:
                    continue
                session.cap_seq += 1
                session.capabilities[code] = _CapabilityState(
                    code=code,
                    resource=raw["resource"],
                    action=raw["action"],
                    scope=DataScope.from_dict(raw["scope"]),
                    valid_until=_dt(raw["valid_until"]),
                    version=raw["version"],
                    quota_limit=raw.get("quota_limit"),
                )
        elif event_type in ("capability.isolated", "capability.revoked", "scope.reduced"):
            cap = session.capabilities[p["capability_code"]]
            session.version = p["session_version"]
            cap.version = p["session_version"]
            if event_type == "capability.isolated":
                cap.status = "isolated"
            elif event_type == "capability.revoked":
                cap.status = "revoked"
                cap.scope = DataScope.nothing(cap.scope.field)
            else:
                cap.status = "restricted"
                cap.scope = DataScope.from_dict(p["scope"])
                cap.valid_until = _dt(p["valid_until"])
        elif event_type == "call.denied":
            session.denials.append(
                Denial(
                    reason=DenialReason(p["reason"]),
                    detail=p.get("detail", ""),
                    at=p.get("at", ""),
                    capability_code=p.get("capability_code"),
                )
            )
        elif event_type == "write.requested":
            session.pending_writes[p["idempotency_key"]] = _PendingWrite(
                call_id=p["call_id"],
                capability_code=p.get("capability_code"),
                resource=p["resource"],
                idempotency_key=p["idempotency_key"],
                payload=dict(p["payload"]),
                requested_at=p["requested_at"],
            )
        elif event_type == "receipt.received":
            key = p["idempotency_key"]
            raw = p["receipt"]
            session.pending_writes.pop(key, None)
            session.delivered[key] = _DeliveredWrite(
                receipt=ExternalReceipt(
                    receipt_id=raw["receipt_id"],
                    connector_code=session.connector_code,
                    resource=p.get("resource", ""),
                    idempotency_key=key,
                    external_ref=raw["external_ref"],
                    status=raw["status"],
                    received_at=raw["received_at"],
                ),
                resource=p.get("resource", ""),
                payload=dict(p.get("payload", {})),
            )
        elif event_type == "page.retained":
            session.retained_audit.append(p)
        elif event_type == "call.authorized":
            return
        elif event_type == "call.completed":
            cap = session.capabilities.get(p.get("capability_code") or "")
            if cap is None:
                return
            if p.get("kind") == WRITE:
                cap.writes += 1
                cap.quota_used += 1
            else:
                cap.reads += 1
                cap.items_requested += p.get("raw_count", 0)
                cap.items_delivered += p.get("delivered_count", 0)
                cap.quota_used += p.get("delivered_count", 0)
        elif event_type == "session.closed":
            session.closed = True
            session.version = p.get("session_version", session.version)
