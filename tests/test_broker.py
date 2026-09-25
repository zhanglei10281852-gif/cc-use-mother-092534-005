"""连接器会话代理的端到端测试。

覆盖场景主管的每一条诉求：
最小能力集合、扩权重确认、调用前后版本校验、分页收缩审计、
幂等写与回执对账、管理员隔离/撤销、管理员视图与额度、崩溃恢复。
"""
from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from broker import (
    AuthorizationError,
    Broker,
    Capability,
    DataScope,
    Demand,
    DenialReason,
    DuplicateDeliveryError,
    EventStore,
    FakeConnector,
    IdempotencyPending,
    ListPages,
    minimum_capabilities,
)
from broker.connectors import Page

UTC = timezone.utc
BASE = datetime(2026, 9, 25, 9, 0, tzinfo=UTC)
END = BASE + timedelta(hours=8)


class Clock:
    """可控时钟。"""

    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, minutes: int = 0, hours: int = 0) -> None:
        self.now += timedelta(minutes=minutes, hours=hours)


def calendar_read_demand(scope=("event",), until=END) -> Demand:
    return Demand("calendar", "read", DataScope.of("kind", list(scope)), until)


def make_broker(clock: Clock | None = None, datasets=None, path=":memory:"):
    clock = clock or Clock(BASE)
    connector = FakeConnector("office", datasets or {})
    broker = Broker(EventStore(path), {"office": connector}, clock=clock)
    return broker, connector, clock


def calendar_only_session(broker, quotas=None):
    demands = [calendar_read_demand()]
    grants = broker.submit_task(
        "tenant-1", "sess-1", "task-1", "office", "alice", demands, quotas=quotas
    )
    broker.confirm_consent("sess-1")
    return grants


class MinimumCapabilityTests(unittest.TestCase):
    def test_demands_merge_into_minimum_set(self) -> None:
        demands = [
            Demand("calendar", "read", DataScope.of("kind", ["event"]), END),
            Demand("calendar", "read", DataScope.of("kind", ["event", "meeting"]), END),
            Demand("mail", "write", DataScope.of("kind", ["draft"]), END),
        ]
        grants = minimum_capabilities(demands)
        self.assertEqual(
            {(g.resource, g.action, frozenset(g.scope.allowed or ())) for g in grants},
            {
                ("calendar", "read", frozenset({"event", "meeting"})),
                ("mail", "write", frozenset({"draft"})),
            },
        )

    def test_reading_calendar_does_not_grant_mail_drive_contacts(self) -> None:
        broker, _, _ = make_broker()
        grants = calendar_only_session(broker)
        self.assertEqual([(g.resource, g.action) for g in grants], [("calendar", "read")])
        view = broker.admin_view("sess-1")
        self.assertEqual(len(view["capabilities"]), 1)
        self.assertEqual(view["capabilities"][0]["scope"]["in"], ["event"])

    def test_consent_with_extra_scope_is_rejected(self) -> None:
        broker, _, _ = make_broker()
        broker.submit_task(
            "tenant-1", "sess-1", "task-1", "office", "alice", [calendar_read_demand()]
        )
        generous = [
            minimum_capabilities([calendar_read_demand()])[0],
            minimum_capabilities(
                [Demand("mail", "read", DataScope.of("kind", ["mail"]), END)]
            )[0],
        ]
        with self.assertRaises(AuthorizationError) as ctx:
            broker.confirm_consent("sess-1", accepted=generous)
        self.assertIs(ctx.exception.reason, DenialReason.GRANT_NOT_MINIMAL)

    def test_consent_with_missing_scope_is_rejected(self) -> None:
        broker, _, _ = make_broker()
        broker.submit_task(
            "tenant-1", "sess-1", "task-1", "office", "alice", [calendar_read_demand()]
        )
        with self.assertRaises(AuthorizationError) as ctx:
            broker.confirm_consent("sess-1", accepted=[])
        self.assertIs(ctx.exception.reason, DenialReason.CAPABILITY_MISSING)


class ExpansionTests(unittest.TestCase):
    def test_expansion_needs_reconfirmation(self) -> None:
        broker, _, _ = make_broker()
        calendar_only_session(broker)
        missing = broker.add_demands(
            "sess-1", [Demand("mail", "read", DataScope.of("kind", ["mail"]), END)]
        )
        self.assertEqual([(g.resource, g.action) for g in missing], [("mail", "read")])
        # 重新确认前，扩大的调用被拒绝并标明需要所有者重新确认。
        with self.assertRaises(AuthorizationError) as ctx:
            broker.start_read("sess-1", "mail", expected_session_version=1)
        self.assertIs(
            ctx.exception.reason, DenialReason.RESOURCE_OWNER_RECONFIRM_REQUIRED
        )
        broker.confirm_expansion("sess-1")
        reader = broker.start_read("sess-1", "mail", expected_session_version=2)
        self.assertEqual(reader.pages_claimed, 0)

    def test_existing_capability_covers_added_demand_without_reconfirm(self) -> None:
        broker, _, _ = make_broker()
        calendar_only_session(broker)
        missing = broker.add_demands(
            "sess-1", [Demand("calendar", "read", DataScope.of("kind", ["event"]), END)]
        )
        self.assertEqual(missing, [])


class VersionAndPaginationTests(unittest.TestCase):
    def _broker_with_pages(self):
        items = [
            {"id": f"e{i}", "kind": "event", "title": f"会议{i}"}
            for i in range(5)
        ]
        # 连接器还混入了非授权类别的数据，验证投影。
        items[2] = {"id": "x1", "kind": "freebusy", "title": "忙闲信息"}
        return make_broker(datasets={"calendar": ListPages.of(items, 2)})

    def test_pagination_projects_scope_and_counts_usage(self) -> None:
        broker, _, _ = self._broker_with_pages()
        calendar_only_session(broker)
        reader = broker.start_read("sess-1", "calendar", expected_session_version=1)
        pages = list(reader.iter_pages(1))
        # 三页原始条数 2/2/1；freebusy 被投影掉后为 2/1/1。
        self.assertEqual([len(p) for p in pages], [2, 1, 1])
        self.assertTrue(reader.finished)
        view = broker.admin_view("sess-1")
        cap = view["capabilities"][0]
        self.assertEqual(cap["usage"]["items_requested"], 5)
        self.assertEqual(cap["usage"]["items_delivered"], 4)

    def test_stale_version_is_rejected_before_call(self) -> None:
        broker, _, _ = self._broker_with_pages()
        calendar_only_session(broker)
        reader = broker.start_read("sess-1", "calendar", expected_session_version=1)
        self.assertEqual(len(reader.next_page(1)), 2)
        # 管理员在任务读取期间收缩范围，会话版本前进。
        broker.reduce_capability_scope(
            "sess-1", "cap-001", new_scope=DataScope.nothing("kind"), reason="调查"
        )
        with self.assertRaises(AuthorizationError) as ctx:
            reader.next_page(expected_session_version=1)  # 任务仍持旧版本
        self.assertIs(ctx.exception.reason, DenialReason.SESSION_VERSION_STALE)

    def test_shrink_during_page_claim_retains_data_and_stops_unclaimed_pages(self) -> None:
        """授权收缩恰好发生在连接器返回一页的过程中：已返回数据留审计，立即停止。"""
        broker, connector, _ = self._broker_with_pages()
        calendar_only_session(broker)

        class ShrinkingConnector:
            code = "office"

            def __init__(self) -> None:
                self.call = 0

            def list_page(self, resource, page_token):  # noqa: ANN001
                page = connector.list_page(resource, page_token)
                self.call += 1
                if self.call == 2:
                    # 第二页在途期间，所有者撤销该能力（任务仍持版本 1）。
                    broker.revoke_capability("sess-1", "cap-001", reason="紧急撤销")
                return page

            def deliver_write(self, *a):  # pragma: no cover
                raise AssertionError("本测试不写")

            def lookup_delivery(self, *a):  # pragma: no cover
                return None

        wrapper = ShrinkingConnector()
        broker._connectors["office"] = wrapper
        reader = broker.start_read("sess-1", "calendar", expected_session_version=1)
        self.assertEqual(len(reader.next_page(1)), 2)
        with self.assertRaises(AuthorizationError) as ctx:
            reader.next_page(expected_session_version=1)
        self.assertIs(ctx.exception.reason, DenialReason.CAPABILITY_REVOKED)
        self.assertTrue(reader.stopped)
        # 后续页不再领取：连接器只被访问了两次。
        self.assertEqual(wrapper.call, 2)
        with self.assertRaises(AuthorizationError):
            reader.next_page(expected_session_version=2)
        view = broker.admin_view("sess-1")
        retained = view["retained_audit"]
        self.assertEqual(len(retained), 1)
        self.assertEqual(retained[0]["reason"], "capability_revoked")
        self.assertEqual(retained[0]["item_count"], 2)
        self.assertEqual(len(retained[0]["items"]), 2)  # 已返回数据保留审计
        reasons = {d["reason"] for d in view["denials"]}
        self.assertIn("capability_revoked", reasons)

    def test_scope_reduction_during_page_claim_stops_with_audit(self) -> None:
        broker, connector, _ = self._broker_with_pages()
        calendar_only_session(broker)

        class NarrowingConnector:
            code = "office"

            def __init__(self) -> None:
                self.call = 0

            def list_page(self, resource, page_token):  # noqa: ANN001
                page = connector.list_page(resource, page_token)
                self.call += 1
                if self.call == 2:
                    broker.reduce_capability_scope(
                        "sess-1",
                        "cap-001",
                        new_scope=DataScope.nothing("kind"),
                        reason="所有者收回全部数据范围",
                    )
                return page

            def deliver_write(self, *a):  # pragma: no cover
                raise AssertionError("本测试不写")

            def lookup_delivery(self, *a):  # pragma: no cover
                return None

        broker._connectors["office"] = NarrowingConnector()
        reader = broker.start_read("sess-1", "calendar", expected_session_version=1)
        reader.next_page(1)
        with self.assertRaises(AuthorizationError) as ctx:
            reader.next_page(expected_session_version=1)
        self.assertIs(ctx.exception.reason, DenialReason.SCOPE_EXCEEDED)
        self.assertTrue(reader.stopped)
        view = broker.admin_view("sess-1")
        self.assertEqual(view["retained_audit"][0]["reason"], "scope_exceeded")

    def test_connector_failure_keeps_page_token_for_retry(self) -> None:
        broker, connector, _ = self._broker_with_pages()
        calendar_only_session(broker)
        reader = broker.start_read("sess-1", "calendar", expected_session_version=1)
        reader.next_page(1)
        connector.set_unavailable(True)
        with self.assertRaises(Exception):
            reader.next_page(1)
        self.assertEqual(reader.pages_claimed, 1)
        connector.set_unavailable(False)
        page = reader.next_page(1)  # 重试的仍是同一页，不丢不重
        self.assertEqual(len(page), 1)
        self.assertEqual(reader.pages_claimed, 2)


class WriteIdempotencyTests(unittest.TestCase):
    def _write_session(self, broker, quotas=None):
        demands = [Demand("mail", "write", DataScope.of("kind", ["draft"]), END)]
        broker.submit_task(
            "tenant-1", "sess-w", "task-w", "office", "alice", demands, quotas=quotas
        )
        broker.confirm_consent("sess-w")

    def test_write_uses_idempotency_key_and_saves_receipt(self) -> None:
        broker, connector, _ = make_broker()
        self._write_session(broker)
        result = broker.submit_write(
            "sess-w", "mail", {"kind": "draft", "to": "bob"}, "idem-1", 1
        )
        self.assertEqual(result.status, "delivered")
        self.assertIsNotNone(result.receipt)
        self.assertEqual(result.receipt.external_ref, "ext-office-000001")

    def test_replay_same_key_returns_same_receipt_without_second_send(self) -> None:
        broker, connector, _ = make_broker()
        self._write_session(broker)
        payload = {"kind": "draft", "to": "bob"}
        first = broker.submit_write("sess-w", "mail", payload, "idem-1", 1)
        second = broker.submit_write("sess-w", "mail", dict(payload), "idem-1", 1)
        self.assertEqual(second.receipt.receipt_id, first.receipt.receipt_id)
        self.assertEqual(len(connector._deliveries), 1)  # 外部只收到一次

    def test_same_key_different_body_is_rejected(self) -> None:
        broker, _, _ = make_broker()
        self._write_session(broker)
        broker.submit_write("sess-w", "mail", {"kind": "draft", "to": "bob"}, "idem-1", 1)
        with self.assertRaises(DuplicateDeliveryError):
            broker.submit_write("sess-w", "mail", {"kind": "draft", "to": "carol"}, "idem-1", 1)

    def test_unknown_result_blocks_resend_until_reconciled(self) -> None:
        broker, connector, _ = make_broker()
        self._write_session(broker)
        connector.lose_write_responses.add("idem-9")
        with self.assertRaises(IdempotencyPending):
            broker.submit_write("sess-w", "mail", {"kind": "draft"}, "idem-9", 1)
        # 外部实际已收到；本地任何重试都被挡住。
        with self.assertRaises(IdempotencyPending):
            broker.submit_write("sess-w", "mail", {"kind": "draft"}, "idem-9", 1)
        self.assertEqual(len(connector._deliveries), 1)
        summary = broker.reconcile_writes("sess-w")
        self.assertEqual(summary["delivered"], 1)
        self.assertEqual(summary["pending"], 0)
        # 对账后同键重放直接返回回执，仍不二次发送。
        replay = broker.submit_write("sess-w", "mail", {"kind": "draft"}, "idem-9", 1)
        self.assertEqual(replay.status, "delivered")
        self.assertEqual(len(connector._deliveries), 1)

    def test_write_scope_is_enforced(self) -> None:
        broker, _, _ = make_broker()
        self._write_session(broker)
        with self.assertRaises(AuthorizationError) as ctx:
            broker.submit_write("sess-w", "mail", {"kind": "mail"}, "idem-2", 1)
        self.assertIs(ctx.exception.reason, DenialReason.SCOPE_EXCEEDED)

    def test_quota_decrements_and_blocks_at_zero(self) -> None:
        broker, _, _ = make_broker()
        self._write_session(broker, quotas={("mail", "write"): 1})
        broker.submit_write("sess-w", "mail", {"kind": "draft"}, "idem-1", 1)
        with self.assertRaises(AuthorizationError) as ctx:
            broker.submit_write("sess-w", "mail", {"kind": "draft"}, "idem-2", 1)
        self.assertIs(ctx.exception.reason, DenialReason.QUOTA_EXCEEDED)
        view = broker.admin_view("sess-w")
        quota = view["capabilities"][0]["quota"]
        self.assertEqual(quota, {"limit": 1, "used": 1, "remaining": 0})


class AdminIsolationTests(unittest.TestCase):
    def _multi_session(self, broker):
        demands = [
            Demand("calendar", "read", DataScope.of("kind", ["event"]), END),
            Demand("mail", "read", DataScope.of("kind", ["mail"]), END),
        ]
        broker.submit_task("tenant-1", "sess-1", "task-1", "office", "alice", demands)
        broker.confirm_consent("sess-1")

    def test_isolating_one_capability_does_not_touch_others(self) -> None:
        broker, _, _ = make_broker(
            datasets={
                "calendar": ListPages([[{"id": "c1", "kind": "event"}]]),
                "mail": ListPages([[{"id": "m1", "kind": "mail"}]]),
            }
        )
        self._multi_session(broker)
        broker.isolate_capability("sess-1", "cap-001", reason="只隔离日历")
        with self.assertRaises(AuthorizationError) as ctx:
            broker.start_read("sess-1", "calendar", expected_session_version=2)
        self.assertIs(ctx.exception.reason, DenialReason.CAPABILITY_ISOLATED)
        mail = broker.start_read("sess-1", "mail", expected_session_version=2)
        self.assertEqual(mail.next_page(2), [{"id": "m1", "kind": "mail"}])
        self.assertTrue(mail.finished)

    def test_revoking_one_resource_keeps_other_tasks_running(self) -> None:
        broker, _, _ = make_broker(
            datasets={"mail": ListPages([[{"id": "m1", "kind": "mail"}]])}
        )
        self._multi_session(broker)
        broker.revoke_capability("sess-1", "cap-001", reason="只撤销日历")
        with self.assertRaises(AuthorizationError) as ctx:
            broker.start_read("sess-1", "calendar", expected_session_version=2)
        self.assertIs(ctx.exception.reason, DenialReason.CAPABILITY_MISSING)
        mail = broker.start_read("sess-1", "mail", expected_session_version=2)
        self.assertEqual(mail.next_page(2), [{"id": "m1", "kind": "mail"}])
        view = broker.admin_view("sess-1")
        statuses = {c["resource"]: c["status"] for c in view["capabilities"]}
        self.assertEqual(statuses["calendar"], "revoked")
        self.assertEqual(statuses["mail"], "granted")

    def test_admin_view_shows_request_usage_denials_and_quota(self) -> None:
        broker, _, _ = make_broker()
        calendar_only_session(broker, quotas={("calendar", "read"): 10})
        view = broker.admin_view("sess-1")
        self.assertEqual(view["requested_scope"][0]["resource"], "calendar")
        broker.isolate_capability("sess-1", "cap-001", reason="核查")
        with self.assertRaises(AuthorizationError):
            broker.start_read("sess-1", "calendar", expected_session_version=2)
        view = broker.admin_view("sess-1")
        self.assertEqual(view["denials"][-1]["reason"], "capability_isolated")
        self.assertEqual(view["capabilities"][0]["quota"]["remaining"], 10)
        self.assertEqual(view["status"], "isolated")


class ExpiryTests(unittest.TestCase):
    def test_expired_capability_is_denied(self) -> None:
        clock = Clock(BASE)
        broker, _, _ = make_broker(clock=clock)
        demands = [
            Demand("calendar", "read", DataScope.of("kind", ["event"]),
                   BASE + timedelta(hours=1))
        ]
        broker.submit_task("tenant-1", "sess-1", "task-1", "office", "alice", demands)
        broker.confirm_consent("sess-1")
        clock.advance(hours=2)
        with self.assertRaises(AuthorizationError) as ctx:
            broker.start_read("sess-1", "calendar", expected_session_version=1)
        self.assertIs(ctx.exception.reason, DenialReason.EXPIRED)

    def test_extending_validity_is_treated_as_expansion(self) -> None:
        broker, _, _ = make_broker()
        calendar_only_session(broker)
        with self.assertRaises(AuthorizationError) as ctx:
            broker.reduce_capability_scope(
                "sess-1", "cap-001", valid_until=END + timedelta(days=1)
            )
        self.assertIs(
            ctx.exception.reason, DenialReason.RESOURCE_OWNER_RECONFIRM_REQUIRED
        )


class RecoveryTests(unittest.TestCase):
    def test_sessions_and_pending_receipts_continue_after_restart(self) -> None:
        clock = Clock(BASE)
        connector = FakeConnector("office")
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "events.db")
            broker = Broker(EventStore(path), {"office": connector}, clock=clock)
            demands = [Demand("mail", "write", DataScope.of("kind", ["draft"]), END)]
            broker.submit_task("tenant-1", "sess-r", "task-r", "office", "alice", demands)
            broker.confirm_consent("sess-r")
            connector.lose_write_responses.add("idem-x")
            with self.assertRaises(IdempotencyPending):
                broker.submit_write("sess-r", "mail", {"kind": "draft"}, "idem-x", 1)
            # 服务在此时崩溃；外部系统（connector 实例）仍保留送达事实。
            broker2 = Broker(EventStore(path), {"office": connector}, clock=clock)
            self.assertIn("sess-r", broker2.list_sessions())
            view = broker2.admin_view("sess-r")
            self.assertEqual(view["status"], "reconciling")
            self.assertEqual(view["pending_writes"][0]["idempotency_key"], "idem-x")
            summary = broker2.reconcile_writes()
            self.assertEqual(summary, {"delivered": 1, "pending": 0, "ambiguous": 0})
            replay = broker2.submit_write(
                "sess-r", "mail", {"kind": "draft"}, "idem-x", 1
            )
            self.assertEqual(replay.status, "delivered")
            self.assertEqual(len(connector._deliveries), 1)

    def test_delivered_receipts_survive_restart_and_stay_idempotent(self) -> None:
        clock = Clock(BASE)
        connector = FakeConnector("office")
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "events.db")
            broker = Broker(EventStore(path), {"office": connector}, clock=clock)
            demands = [Demand("mail", "write", DataScope.of("kind", ["draft"]), END)]
            broker.submit_task("tenant-1", "sess-r", "task-r", "office", "alice", demands)
            broker.confirm_consent("sess-r")
            first = broker.submit_write("sess-r", "mail", {"kind": "draft"}, "idem-1", 1)
            broker2 = Broker(EventStore(path), {"office": connector}, clock=clock)
            second = broker2.submit_write("sess-r", "mail", {"kind": "draft"}, "idem-1", 1)
            self.assertEqual(second.receipt.receipt_id, first.receipt.receipt_id)
            self.assertEqual(len(connector._deliveries), 1)


if __name__ == "__main__":
    unittest.main()
