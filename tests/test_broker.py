"""连接器会话代理的端到端行为测试。"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from broker import (
    AuthorizationDenied,
    Broker,
    Capability,
    DeliveryUncertain,
    FakeConnector,
)
from broker.errors import (
    CAPABILITY_ISOLATED,
    CAPABILITY_NOT_GRANTED,
    CAPABILITY_REVOKED,
    EXPIRED,
    EXPANSION_UNCONFIRMED,
    QUOTA_EXHAUSTED,
    SCOPE_REDUCED,
)
from broker.storage import Store

TZ = timezone(timedelta(hours=8))


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 9, 25, hour, minute, tzinfo=TZ)


def cap(resource: str, action: str, scope: str,
        frm: int = 8, to: int = 18) -> Capability:
    return Capability(resource, action, scope, at(frm), at(to))


class BrokerTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.store = Store()
        self.connector = FakeConnector("collab")
        self.broker = Broker(self.store, {"collab": self.connector})
        self.owner = "owner-1"

    def tearDown(self) -> None:
        self.store.close()

    def _session(self, demands, task: str = "task-1") -> str:
        return self.broker.submit_task(
            "tenant-a", "collab", self.owner, task, demands,
            actor_id="agent-1", now=at(9),
        )


class MinimumSetTest(BrokerTestBase):
    def test_demand_becomes_exact_minimum_capability_set(self) -> None:
        # 任务只要读日历，绝不能顺带拿到邮件/云盘/通讯录
        sid = self._session([cap("calendar", "read", "own_events")])
        self.broker.confirm_consent(sid, self.owner, now=at(9, 1))
        view = self.broker.session_view(sid, now=at(9, 2))
        resources = {c["resource"] for c in view["capabilities"]}
        self.assertEqual(resources, {"calendar"})
        self.assertEqual(len(view["capabilities"]), 1)

    def test_duplicate_demands_collapse(self) -> None:
        demand = cap("calendar", "read", "own_events")
        sid = self._session([demand, demand, demand])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        view = self.broker.session_view(sid, now=at(9))
        self.assertEqual(len(view["capabilities"]), 1)

    def test_non_owner_cannot_confirm(self) -> None:
        sid = self._session([cap("calendar", "read", "own_events")])
        with self.assertRaises(AuthorizationDenied):
            self.broker.confirm_consent(sid, "someone-else", now=at(9))


class ExpansionTest(BrokerTestBase):
    def test_expansion_requires_owner_reconfirmation(self) -> None:
        sid = self._session([cap("calendar", "read", "own_events")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))

        # 智能体随后想读邮件——这是扩大范围
        with self.assertRaises(AuthorizationDenied) as ctx:
            self.broker.execute_read(
                sid, "mail", "read", "inbox", "agent-1", now=at(10),
            )
        self.assertEqual(ctx.exception.reason, CAPABILITY_NOT_GRANTED)

        # 登记扩权需求后，未经所有者确认仍然拒绝
        self.broker.request_expansion(
            sid, [cap("mail", "read", "inbox")], "agent-1", now=at(10, 1),
        )
        with self.assertRaises(AuthorizationDenied) as ctx:
            self.broker.execute_read(
                sid, "mail", "read", "inbox", "agent-1", now=at(10, 2),
            )
        self.assertEqual(ctx.exception.reason, EXPANSION_UNCONFIRMED)

        # 必须资源所有者本人重新确认
        with self.assertRaises(AuthorizationDenied):
            self.broker.confirm_expansion(sid, "agent-1", now=at(10, 3))
        self.broker.confirm_expansion(sid, self.owner, now=at(10, 4))

        # 扩权是新版本追加，历史版本不被覆盖
        view = self.broker.session_view(sid, now=at(10, 5))
        self.assertEqual(view["version"], 2)
        granted_versions = sorted(c["granted_version"] for c in view["capabilities"])
        self.assertEqual(granted_versions, [1, 2])


class VersionCheckTest(BrokerTestBase):
    def test_call_before_and_after_version_check_recorded(self) -> None:
        self.connector.seed_pages("calendar", "own_events", ["e1", "e2"])
        sid = self._session([cap("calendar", "read", "own_events")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        result = self.broker.execute_read(
            sid, "calendar", "read", "own_events", "agent-1", now=at(10),
        )
        self.assertEqual(result["version"], 1)
        self.assertTrue(result["exhausted"])
        call = self.store.calls_of_session(sid)[0]
        self.assertEqual(call["pre_version"], 1)
        self.assertEqual(call["post_version"], 1)

    def test_unstarted_call_blocked_after_revocation(self) -> None:
        sid = self._session([
            cap("calendar", "read", "own_events"),
            cap("mail", "read", "inbox"),
        ])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        # 撤销邮件这一项资源
        self.broker.revoke_resource(sid, "mail", "admin-1", "incident", now=at(9, 30))
        with self.assertRaises(AuthorizationDenied) as ctx:
            self.broker.execute_read(
                sid, "mail", "read", "inbox", "agent-1", now=at(9, 31),
            )
        self.assertEqual(ctx.exception.reason, CAPABILITY_REVOKED)
        # 拒绝原因进入审计，可在管理视图看到
        view = self.broker.session_view(sid, now=at(9, 32))
        self.assertEqual(view["denials"][0]["reason"], CAPABILITY_REVOKED)

    def test_expired_capability_denied(self) -> None:
        sid = self._session([cap("calendar", "read", "own_events", frm=8, to=9)])
        self.broker.confirm_consent(sid, self.owner, now=at(8, 30))
        with self.assertRaises(AuthorizationDenied) as ctx:
            self.broker.execute_read(
                sid, "calendar", "read", "own_events", "agent-1", now=at(9, 30),
            )
        self.assertEqual(ctx.exception.reason, EXPIRED)


class PaginationContractionTest(BrokerTestBase):
    def test_mid_pagination_scope_reduction_keeps_prior_pages_stops_rest(self) -> None:
        # 三页数据（每页 2 条）
        self.connector.seed_pages(
            "calendar", "own_events", ["e1", "e2", "e3", "e4", "e5", "e6"],
        )
        sid = self._session([cap("calendar", "read", "own_events")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))

        first = self.broker.execute_read(
            sid, "calendar", "read", "own_events", "agent-1", now=at(10),
        )
        self.assertEqual(first["rows"], ["e1", "e2"])

        second = self.broker.fetch_next_page(first["call_id"], "agent-1", now=at(10, 1))
        self.assertEqual(second["rows"], ["e3", "e4"])

        # 管理员在第三页读取请求期间收缩授权
        def reduce_on_page3(resource: str, scope: str, cursor):
            if cursor == "2":
                self.broker.reduce_scope(
                    sid, [cap("calendar", "read", "own_events")],
                    "admin-1", "owner withdrew calendar", now=at(10, 2),
                )
        self.connector.on_read = reduce_on_page3

        with self.assertRaises(AuthorizationDenied) as ctx:
            self.broker.fetch_next_page(second["call_id"], "agent-1", now=at(10, 3))
        self.assertEqual(ctx.exception.reason, SCOPE_REDUCED)

        # 已返回的两页保留审计；第三页外部虽返回但标记为未交付
        pages = self.store.pages_audit(first["call_id"])
        self.assertEqual([p["page_index"] for p in pages], [0, 1, 2])
        self.assertTrue(pages[0]["delivered"])
        self.assertTrue(pages[1]["delivered"])
        self.assertFalse(pages[2]["delivered"])

        # 尚未领取的页立即停止：再取直接拒绝
        with self.assertRaises(AuthorizationDenied):
            self.broker.fetch_next_page(second["call_id"], "agent-1", now=at(10, 4))

        view = self.broker.session_view(sid, now=at(10, 5))
        # 实际使用只统计已交付的 4 行 / 2 页
        self.assertEqual(view["usage"]["delivered_rows"], 4)
        self.assertEqual(view["usage"]["delivered_pages"], 2)
        self.assertEqual(view["denials"][0]["reason"], SCOPE_REDUCED)

    def test_unrelated_capability_version_bump_does_not_stop_pagination(self) -> None:
        self.connector.seed_pages(
            "calendar", "own_events", ["e1", "e2", "e3", "e4"],
        )
        sid = self._session([
            cap("calendar", "read", "own_events"),
            cap("mail", "read", "inbox"),
        ])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        first = self.broker.execute_read(
            sid, "calendar", "read", "own_events", "agent-1", now=at(10),
        )
        # 隔离另一项能力（会升版本），但日历读取不应被误判为收缩
        self.broker.isolate_capability(
            sid, "mail", "read", "inbox", "admin-1", "probe", now=at(10, 1),
        )
        second = self.broker.fetch_next_page(first["call_id"], "agent-1", now=at(10, 2))
        self.assertEqual(second["rows"], ["e3", "e4"])
        self.assertTrue(second["exhausted"])


class IsolationRevocationTest(BrokerTestBase):
    def test_isolate_single_capability_does_not_cut_others(self) -> None:
        self.connector.seed_pages("calendar", "own_events", ["e1"])
        sid = self._session([
            cap("calendar", "read", "own_events"),
            cap("mail", "read", "inbox"),
        ])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.broker.isolate_capability(
            sid, "mail", "read", "inbox", "admin-1", "suspicious", now=at(9, 30),
        )
        # 邮件被隔离
        with self.assertRaises(AuthorizationDenied) as ctx:
            self.broker.execute_read(
                sid, "mail", "read", "inbox", "agent-1", now=at(9, 31),
            )
        self.assertEqual(ctx.exception.reason, CAPABILITY_ISOLATED)
        # 日历任务照常进行，不需要整体断开连接
        result = self.broker.execute_read(
            sid, "calendar", "read", "own_events", "agent-1", now=at(9, 32),
        )
        self.assertEqual(result["rows"], ["e1"])

    def test_revoke_one_resource_preserves_other_tasks(self) -> None:
        sid = self._session([
            cap("calendar", "read", "own_events"),
            cap("drive", "read", "shared"),
        ])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.broker.revoke_resource(sid, "drive", "admin-1", "legal hold", now=at(10))
        view = self.broker.session_view(sid, now=at(10, 1))
        statuses = {c["resource"]: c["status"] for c in view["capabilities"]}
        self.assertEqual(statuses["drive"], "revoked")
        self.assertEqual(statuses["calendar"], "granted")
        self.assertEqual(view["status"], "restricted")

    def test_restore_isolated_capability(self) -> None:
        sid = self._session([cap("mail", "read", "inbox")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.broker.isolate_capability(
            sid, "mail", "read", "inbox", "admin-1", "probe", now=at(10),
        )
        self.broker.restore_capability(
            sid, "mail", "read", "inbox", "admin-1", now=at(11),
        )
        view = self.broker.session_view(sid, now=at(11, 1))
        self.assertEqual(view["capabilities"][0]["status"], "granted")
        self.assertEqual(view["status"], "consented")

    def test_revoked_capability_cannot_be_restored(self) -> None:
        from broker.errors import InvalidTransition
        sid = self._session([cap("mail", "read", "inbox")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.broker.revoke_capability(
            sid, "mail", "read", "inbox", "admin-1", "gone", now=at(10),
        )
        with self.assertRaises(InvalidTransition):
            self.broker.restore_capability(
                sid, "mail", "read", "inbox", "admin-1", now=at(11),
            )


class IdempotentWriteTest(BrokerTestBase):
    def test_write_persists_receipt_and_local_retry_does_not_duplicate(self) -> None:
        sid = self._session([cap("mail", "send", "inbox")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        result = self.broker.execute_write(
            sid, "mail", {"to": "a@x", "subject": "hi"}, "idem-1",
            "agent-1", now=at(10),
        )
        self.assertFalse(result["duplicate"])

        # 本地用同一幂等键重试：直接回放原回执，外部零新增副作用
        replay = self.broker.execute_write(
            sid, "mail", {"to": "a@x", "subject": "hi"}, "idem-1",
            "agent-1", now=at(10, 1),
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["external_id"], result["external_id"])
        self.assertEqual(self.connector.send_attempts["idem-1"], 1)

    def test_timeout_then_retry_reconciles_without_double_send(self) -> None:
        sid = self._session([cap("mail", "send", "inbox")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.connector.fail_send("idem-2", times=1)
        with self.assertRaises(DeliveryUncertain):
            self.broker.execute_write(
                sid, "mail", {"subject": "x"}, "idem-2", "agent-1", now=at(10),
            )
        # 首次已受理，回执丢失；外部只承认一次
        self.assertEqual(self.connector.send_attempts["idem-2"], 1)

        # 重试凭同一幂等键拿到的是同一张外部回执，标记 duplicate，不产生第二封
        retried = self.broker.retry_write("idem-2", "agent-1", now=at(10, 5))
        self.assertTrue(retried["duplicate"])
        self.assertEqual(retried["external_id"], "ext-idem-2")
        self.assertEqual(self.connector.send_attempts["idem-2"], 2)
        # 服务端只留下一封（首次受理的那张）
        receipts = self.store.pending_receipts()
        self.assertEqual(receipts, [])

    def test_pending_receipt_survives_and_recover_reconciles(self) -> None:
        sid = self._session([cap("mail", "send", "inbox")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.connector.fail_send("idem-3", times=1)
        with self.assertRaises(DeliveryUncertain):
            self.broker.execute_write(
                sid, "mail", {"subject": "y"}, "idem-3", "agent-1", now=at(10),
            )
        # 模拟服务重启：用同一个底层存储新建 Broker
        recovered_broker = Broker(self.store, {"collab": self.connector})
        outcome = recovered_broker.recover("system", now=at(10, 30))
        self.assertEqual(len(outcome["reconciled_receipts"]), 1)
        self.assertEqual(outcome["reconciled_receipts"][0]["idempotency_key"], "idem-3")
        # 外部没有重复发送
        self.assertEqual(self.connector.send_attempts["idem-3"], 2)
        view = recovered_broker.session_view(sid, now=at(10, 31))
        self.assertIn("receipt.reconciled",
                      [e["type"] for e in view["audit_events"]])
        self.assertEqual(view["status"], "consented")

    def test_cannot_close_session_with_pending_receipt(self) -> None:
        from broker.errors import InvalidTransition
        sid = self._session([cap("mail", "send", "inbox")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.connector.fail_send("idem-4", times=1)
        with self.assertRaises(DeliveryUncertain):
            self.broker.execute_write(
                sid, "mail", {"subject": "z"}, "idem-4", "agent-1", now=at(10),
            )
        with self.assertRaises(InvalidTransition):
            self.broker.close_session(sid, "owner-1", now=at(10, 1))
        self.broker.retry_write("idem-4", "agent-1", now=at(10, 2))
        self.broker.close_session(sid, "owner-1", now=at(10, 3))
        self.assertEqual(self.broker.session_view(sid)["status"], "closed")


class QuotaTest(BrokerTestBase):
    def test_quota_remaining_visible_and_enforced(self) -> None:
        # 三页数据（每页 2 条），当日额度只给 2 次
        self.connector.seed_pages(
            "calendar", "own_events", ["e1", "e2", "e3", "e4", "e5"],
        )
        sid = self._session([cap("calendar", "read", "own_events")])
        self.broker.set_quota("tenant-a", "collab", "calendar", "read", 2)
        self.broker.confirm_consent(sid, self.owner, now=at(9))

        first = self.broker.execute_read(
            sid, "calendar", "read", "own_events", "agent-1", now=at(10),
        )  # 用掉 1
        self.assertEqual(first["rows"], ["e1", "e2"])
        view = self.broker.session_view(sid, now=at(10, 1))
        cal = next(c for c in view["capabilities"] if c["resource"] == "calendar")
        self.assertEqual(cal["quota_remaining"], 1)
        self.broker.fetch_next_page(first["call_id"], "agent-1", now=at(10, 2))  # 用掉第 2
        # 第三页在调用前校验时即被额度拦截，不会发往外部
        with self.assertRaises(AuthorizationDenied) as ctx:
            self.broker.fetch_next_page(first["call_id"], "agent-1", now=at(10, 3))
        self.assertEqual(ctx.exception.reason, QUOTA_EXHAUSTED)
        view = self.broker.session_view(sid, now=at(10, 4))
        self.assertEqual(view["denials"][0]["reason"], QUOTA_EXHAUSTED)


class AuditTest(BrokerTestBase):
    def test_event_trail_is_append_only_and_versioned(self) -> None:
        sid = self._session([cap("calendar", "read", "own_events")])
        self.broker.confirm_consent(sid, self.owner, now=at(9))
        self.broker.isolate_capability(
            sid, "calendar", "read", "own_events", "admin", "x", now=at(10),
        )
        events = self.broker.audit_trail(sid)
        types = [e["event_type"] for e in events]
        self.assertEqual(
            types,
            ["demand.calculated", "consent.confirmed", "capability.isolated"],
        )
        # 隔离事件带新版本号
        self.assertEqual(events[-1]["version"], 2)


if __name__ == "__main__":
    unittest.main()
