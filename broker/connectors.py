"""连接器抽象与离线测试桩。

真实部署中连接器封装外部办公平台（邮件、日历、云盘、通讯录）的 API。
这里只定义会话代理依赖的最小接口，并用 :class:`FakeConnector`
提供可离线运行的实现，支持故障注入与响应丢失模拟。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Protocol


@dataclass(frozen=True, slots=True)
class ExternalReceipt:
    """外部平台对一次写操作的回执。"""

    receipt_id: str
    connector_code: str
    resource: str
    idempotency_key: str
    external_ref: str
    status: str  # accepted / delivered / rejected
    received_at: str
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Page:
    """分页读取的一页。``next_page_token`` 为 None 表示已读完。"""

    items: list[dict[str, Any]]
    next_page_token: Optional[str]


class Connector(Protocol):
    code: str

    def list_page(self, resource: str, page_token: Optional[str]) -> Page: ...

    def deliver_write(
        self, resource: str, idempotency_key: str, payload: dict[str, Any]
    ) -> ExternalReceipt: ...

    def lookup_delivery(
        self, resource: str, idempotency_key: str
    ) -> Optional[ExternalReceipt]: ...


@dataclass
class ListPages:
    """预置分页数据：按页给出条目，每条目带 ``kind`` 等属性供范围匹配。"""

    pages: list[list[dict[str, Any]]]

    @classmethod
    def of(cls, items: list[dict[str, Any]], page_size: int) -> "ListPages":
        return cls([items[i : i + page_size] for i in range(0, len(items), page_size)])


class FakeConnector:
    """内存连接器桩。

    - ``set_unavailable(True)`` 后所有调用抛 :class:`RemoteUnavailable`；
    - ``lose_next_write_response``：写请求实际已落库，但响应丢失，
      用于验证本地重试不会造成重复发送，只能通过对账核销。
    """

    def __init__(self, code: str, datasets: Optional[dict[str, ListPages]] = None) -> None:
        self.code = code
        self._datasets = datasets or {}
        self._deliveries: dict[tuple[str, str], ExternalReceipt] = {}
        self._receipt_seq = 0
        self.unavailable = False
        self.lose_write_responses: set[str] = set()

    def seed(self, resource: str, pages: ListPages) -> None:
        self._datasets[resource] = pages

    def set_unavailable(self, flag: bool) -> None:
        self.unavailable = flag

    def list_page(self, resource: str, page_token: Optional[str]) -> Page:
        if self.unavailable:
            from .errors import RemoteUnavailable

            raise RemoteUnavailable(f"连接器 {self.code} 不可用")
        dataset = self._datasets.get(resource)
        if dataset is None:
            return Page(items=[], next_page_token=None)
        index = 0 if page_token is None else int(page_token)
        if index >= len(dataset.pages):
            return Page(items=[], next_page_token=None)
        nxt = None if index + 1 >= len(dataset.pages) else str(index + 1)
        return Page(items=[dict(item) for item in dataset.pages[index]], next_page_token=nxt)

    def deliver_write(
        self, resource: str, idempotency_key: str, payload: dict[str, Any]
    ) -> ExternalReceipt:
        if self.unavailable:
            from .errors import RemoteUnavailable

            raise RemoteUnavailable(f"连接器 {self.code} 不可用")
        key = (resource, idempotency_key)
        existing = self._deliveries.get(key)
        if existing is not None:
            # 外部侧同样按幂等键去重：重放只返回原回执，不会二次发送。
            return existing
        self._receipt_seq += 1
        receipt = ExternalReceipt(
            receipt_id=f"rcpt-{self.code}-{self._receipt_seq:04d}",
            connector_code=self.code,
            resource=resource,
            idempotency_key=idempotency_key,
            external_ref=f"ext-{self.code}-{self._receipt_seq:06d}",
            status="accepted",
            received_at=payload["_at"],
            raw={"resource": resource, "payload": {k: v for k, v in payload.items() if k != "_at"}},
        )
        self._deliveries[key] = receipt
        if idempotency_key in self.lose_write_responses:
            from .errors import RemoteUnavailable

            raise RemoteUnavailable("响应丢失：外部已接收但回执未送达")
        return receipt

    def lookup_delivery(
        self, resource: str, idempotency_key: str
    ) -> Optional[ExternalReceipt]:
        if self.unavailable:
            from .errors import RemoteUnavailable

            raise RemoteUnavailable(f"连接器 {self.code} 不可用")
        return self._deliveries.get((resource, idempotency_key))
