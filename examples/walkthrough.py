"""端到端业务演练：只读日历的智能体、授权收缩、幂等写与崩溃恢复。

直接运行：

    python3 examples/walkthrough.py

不需要任何外部服务；连接器使用内存桩，事件存储使用临时 SQLite 文件。
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from broker import (
    AuthorizationError,
    Broker,
    DataScope,
    Demand,
    DenialReason,
    EventStore,
    FakeConnector,
    IdempotencyPending,
    ListPages,
)

UTC = timezone.utc
NOW = datetime(2026, 9, 25, 9, 0, tzinfo=UTC)
END = NOW + timedelta(hours=8)


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now


def hr(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def show(view: dict) -> None:
    print(json.dumps(view, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    clock = Clock()
    connector = FakeConnector("office-platform")
    calendar_items = [
        {"id": "evt-1", "kind": "event", "title": "周一评审会"},
        {"id": "evt-2", "kind": "event", "title": "周二客户会"},
        {"id": "fb-1", "kind": "freebusy", "title": "忙闲（不应被智能体看到）"},
        {"id": "evt-3", "kind": "event", "title": "周四复盘会"},
    ]
    connector.seed("calendar", ListPages.of(calendar_items, page_size=2))
    connector.seed("mail", ListPages([[{"id": "m-1", "kind": "mail", "title": "季度汇报"}]]))

    with tempfile.TemporaryDirectory() as tmp:
        store = EventStore(str(Path(tmp) / "broker.db"))
        broker = Broker(store, {"office-platform": connector}, clock=clock)

        # 1. 智能体任务：只读日历里的会议事件。最小能力集合只有一条。
        hr("1. 任务提交：计算最小能力集合（资源/动作/数据范围/有效时间）")
        demands = [Demand("calendar", "read", DataScope.of("kind", ["event"]), END)]
        grants = broker.submit_task(
            tenant_id="tenant-a",
            session_id="sess-agent-1",
            task_id="briefing-agent",
            connector_code="office-platform",
            owner="alice",
            demands=demands,
            quotas={("calendar", "read"): 100},
        )
        for g in grants:
            print(f"最小授权：{g.resource}:{g.action} 范围={g.scope.to_dict()} 截止={g.valid_until}")
        caps = broker.confirm_consent("sess-agent-1")  # 资源所有者确认
        print(f"所有者已确认 {len(caps)} 项能力；邮件/云盘/通讯录一律未授权")

        # 2. 智能体想顺手读邮件：缺能力被拒，且有拒绝原因。
        hr("2. 越界读取邮件：直接拒绝并记录原因")
        try:
            broker.start_read("sess-agent-1", "mail", expected_session_version=1)
        except AuthorizationError as exc:
            print(f"拒绝：{exc}（原因={exc.reason.value}）")

        # 3. 分页读日历；连接器混入的 freebusy 被资源投影挡掉。
        hr("3. 分页读取：逐页校验版本，超范围条目被投影过滤")
        reader = broker.start_read("sess-agent-1", "calendar", expected_session_version=1)
        for page_no, items in enumerate(reader.iter_pages(1), start=1):
            print(f"第 {page_no} 页实际交付：{[i['title'] for i in items]}")

        # 4. 任务中途想扩大到邮件：必须由资源所有者重新确认。
        hr("4. 扩大范围：先生成待确认扩权，未确认前调用被拒")
        missing = broker.add_demands(
            "sess-agent-1",
            [Demand("mail", "read", DataScope.of("kind", ["mail"]), END)],
        )
        print("待所有者重新确认：", [(g.resource, g.action) for g in missing])
        try:
            broker.start_read("sess-agent-1", "mail", expected_session_version=1)
        except AuthorizationError as exc:
            print(f"确认前调用拒绝：原因={exc.reason.value}（须所有者重新确认）")
        broker.confirm_expansion("sess-agent-1")  # 所有者重新确认
        mail_reader = broker.start_read("sess-agent-1", "mail", expected_session_version=2)
        print("重新确认后读到：", [i["title"] for i in mail_reader.next_page(2)])

        # 5. 读取过程中授权收缩：已返回数据留审计，未领取的页立即停止。
        hr("5. 分页途中授权收缩：已返回留存审计，未领取立即停止")
        broker.submit_task(
            "tenant-a", "sess-agent-2", "calendar-export", "office-platform", "alice",
            [Demand("calendar", "read", DataScope.of("kind", ["event"]), END)],
        )
        broker.confirm_consent("sess-agent-2")

        class InterceptConnector:
            code = "office-platform"

            def __init__(self, inner: FakeConnector) -> None:
                self.inner = inner
                self.calls = 0

            def list_page(self, resource, page_token):
                page = self.inner.list_page(resource, page_token)
                self.calls += 1
                if self.calls == 2:
                    broker.revoke_capability("sess-agent-2", "cap-001", reason="风险应急")
                return page

            def deliver_write(self, *a):
                return self.inner.deliver_write(*a)

            def lookup_delivery(self, *a):
                return self.inner.lookup_delivery(*a)

        wired = InterceptConnector(connector)
        broker._connectors["office-platform"] = wired
        reader2 = broker.start_read("sess-agent-2", "calendar", expected_session_version=1)
        print("第一页：", [i["title"] for i in reader2.next_page(1)])
        try:
            reader2.next_page(expected_session_version=1)
        except AuthorizationError as exc:
            print(f"第二页返回时发现撤销：{exc.reason.value}，读取器停止={reader2.stopped}")
        view2 = broker.admin_view("sess-agent-2")
        retained = view2["retained_audit"][0]
        print(f"留存审计：{retained['item_count']} 条，原因={retained['reason']}，"
              f"连接器实际被调用 {wired.calls} 次（未领取的页不再请求）")

        # 6. 写操作幂等：响应丢失后禁止重发，只能对账核销。
        hr("6. 写操作幂等键 + 外部回执：故障后不重复发送")
        broker._connectors["office-platform"] = connector
        broker.submit_task(
            "tenant-a", "sess-write", "send-digest", "office-platform", "alice",
            [Demand("mail", "write", DataScope.of("kind", ["draft"]), END)],
        )
        broker.confirm_consent("sess-write")
        connector.lose_write_responses.add("digest-2026-09-25")
        try:
            broker.submit_write(
                "sess-write", "mail",
                {"kind": "draft", "subject": "每日简报"},
                idempotency_key="digest-2026-09-25",
                expected_session_version=1,
            )
        except IdempotencyPending as exc:
            print("本地重试被拦截：", str(exc).split("：", 1)[0])
        summary = broker.reconcile_writes("sess-write")
        print("服务恢复后对账：", summary)
        replay = broker.submit_write(
            "sess-write", "mail",
            {"kind": "draft", "subject": "每日简报"},
            idempotency_key="digest-2026-09-25",
            expected_session_version=1,
        )
        print(f"同键重放拿到原回执 {replay.receipt.receipt_id}，"
              f"外部送达次数={len(connector._deliveries)}")

        # 7. 管理员视图：请求范围、实际使用、拒绝原因、剩余额度。
        hr("7. 管理员接口")
        view = broker.admin_view("sess-agent-1")
        print(f"会话状态={view['status']}，版本={view['session_version']}")
        for cap in view["capabilities"]:
            print(f"  能力 {cap['code']} {cap['resource']}:{cap['action']} "
                  f"状态={cap['status']} 使用={cap['usage']} 额度剩余={cap['quota']['remaining']}")
        print("最近拒绝原因：", sorted({d["reason"] for d in view["denials"]}))
        print("全部会话：", broker.list_sessions())


if __name__ == "__main__":
    main()
