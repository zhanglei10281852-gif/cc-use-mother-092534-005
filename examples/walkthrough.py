"""端到端走读：主管提出的完整场景。

    python3 examples/walkthrough.py

演示：只授权日历 → 扩权必须所有者重认 → 分页途中收缩 → 幂等写超时不重发
→ 进程重启后回执对账 → 管理视图（请求范围/实际使用/拒绝原因/剩余额度）。
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from broker import AuthorizationDenied, Broker, Capability, DeliveryUncertain, FakeConnector
from broker.storage import Store

TZ = timezone(timedelta(hours=8))
T = lambda h, m=0: datetime(2026, 9, 25, h, m, tzinfo=TZ)  # noqa: E731


def show(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def main() -> None:
    db_path = Path(tempfile.gettempdir()) / "connector_broker_demo.sqlite3"
    db_path.unlink(missing_ok=True)

    store = Store(str(db_path))
    connector = FakeConnector("collab")
    connector.seed_pages(
        "calendar", "own_events",
        [f"event-{i}" for i in range(1, 7)],  # 三页，每页两条
    )
    broker = Broker(store, {"collab": connector})
    broker.set_quota("tenant-a", "collab", "calendar", "read", 10)

    show("1) 任务提交：只读日历 → 最小能力集合（不含邮件/云盘/通讯录）")
    sid = broker.submit_task(
        "tenant-a", "collab", "owner-zhang", "read-my-calendar",
        [Capability("calendar", "read", "own_events", T(8), T(18))],
        actor_id="agent", now=T(9),
    )
    broker.confirm_consent(sid, "owner-zhang", now=T(9, 1))
    view = broker.session_view(sid, now=T(9, 2))
    print("会话", sid, "状态", view["status"], "版本", view["version"])
    print("授权范围：", json.dumps(view["capabilities"], ensure_ascii=False, indent=2))

    show("2) 智能体想顺带读邮件 → 拒绝；扩权登记后仍拒绝，必须所有者重新确认")
    try:
        broker.execute_read(sid, "mail", "read", "inbox", "agent", now=T(10))
    except AuthorizationDenied as exc:
        print("直接读邮件被拒：", exc.reason)
    broker.request_expansion(
        sid, [Capability("mail", "read", "inbox", T(8), T(18))],
        "agent", now=T(10, 1),
    )
    try:
        broker.execute_read(sid, "mail", "read", "inbox", "agent", now=T(10, 2))
    except AuthorizationDenied as exc:
        print("扩权未确认被拒：", exc.reason)
    broker.confirm_expansion(sid, "owner-zhang", now=T(10, 3))
    print("资源所有者重新确认后，会话版本 ->", broker.session_view(sid)["version"])

    show("3) 分页读取日历；读到第 3 页请求期间，所有者收缩授权")
    page1 = broker.execute_read(sid, "calendar", "read", "own_events", "agent", now=T(11))
    page2 = broker.fetch_next_page(page1["call_id"], "agent", now=T(11, 1))
    print("已交付：", page1["rows"], page2["rows"])

    def shrink_during_page3(resource, scope, cursor):
        if cursor == "2":
            broker.reduce_scope(
                sid, [Capability("calendar", "read", "own_events", T(8), T(18))],
                "owner-zhang", "日程不再共享", now=T(11, 2),
            )
    connector.on_read = shrink_during_page3
    try:
        broker.fetch_next_page(page2["call_id"], "agent", now=T(11, 3))
    except AuthorizationDenied as exc:
        print("第 3 页被停止：", exc.reason, "——已取两页保留审计，未领取页停止")

    show("4) 写操作：幂等键 + 回执超时 → 本地重试绝不重复发送")
    broker.request_expansion(
        sid, [Capability("mail", "send", "inbox", T(8), T(18))],
        "agent", now=T(13),
    )
    broker.confirm_expansion(sid, "owner-zhang", now=T(13, 1))
    connector.fail_send("mail-letter-1", times=1)
    try:
        broker.execute_write(
            sid, "mail", {"to": "boss@x", "subject": "周报"},
            "mail-letter-1", "agent", now=T(13, 2),
        )
    except DeliveryUncertain as exc:
        print("送达状态不明：", exc)
    retry = broker.retry_write("mail-letter-1", "agent", now=T(13, 3))
    print("重试结果：external_id =", retry["external_id"],
          " duplicate =", retry["duplicate"])
    print("外部服务端实际受理次数：", connector.send_attempts["mail-letter-1"], "（只有 1 次副作用）")

    show("5) 再来一封：回执超时后进程崩溃 → 重启后凭原幂等键继续对账")
    connector.fail_send("mail-letter-2", times=1)
    try:
        broker.execute_write(
            sid, "mail", {"to": "team@x", "subject": "通知"},
            "mail-letter-2", "agent", now=T(14),
        )
    except DeliveryUncertain:
        print("第二封回执超时，pending 回执已先落盘；模拟进程结束……")
    store.close()

    store2 = Store(str(db_path))  # 重新打开同一个数据库 = 服务恢复
    broker2 = Broker(store2, {"collab": connector})
    outcome = broker2.recover("system", now=T(14, 30))
    print("恢复对账：", json.dumps(outcome, ensure_ascii=False))

    show("6) 管理视图：请求范围 / 实际使用 / 拒绝原因 / 剩余额度")
    view = broker2.session_view(sid, now=T(15))
    print("状态：", view["status"])
    print("实际使用：", json.dumps(view["usage"], ensure_ascii=False))
    print("拒绝原因：", json.dumps(view["denials"], ensure_ascii=False, indent=2))
    print("能力与剩余额度：")
    for c in view["capabilities"]:
        print(f"  - {c['resource']}/{c['action']}/{c['data_scope']} "
              f"状态={c['status']} 剩余额度={c['quota_remaining']}")
    print("审计事件：")
    for e in view["audit_events"]:
        print(f"  v{e['version']}  {e['at']}  {e['type']}  by {e['actor']}")

    show("7) 管理员单点隔离：只切断邮件发送，日历等其他合法任务不受影响")
    broker2.isolate_capability(
        sid, "mail", "send", "inbox", "admin", "异常外发调查", now=T(16),
    )
    try:
        broker2.execute_write(
            sid, "mail", {"to": "x@y"}, "mail-letter-3", "agent", now=T(16, 1),
        )
    except AuthorizationDenied as exc:
        print("隔离后外发被拒：", exc.reason)
    print("邮件读取能力仍为：",
          next(c["status"] for c in broker2.session_view(sid)["capabilities"]
               if c["resource"] == "mail" and c["action"] == "read"))
    store2.close()


if __name__ == "__main__":
    main()
