"""外部连接器的内存模拟器：分页读取、写超时与服务端幂等去重。

生产环境中这里替换为真实连接器的 HTTP 客户端；会话代理只依赖它的两个方法：
read_page（分页读）与 send（带幂等键的写）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True)
class Page:
    rows: list[Any]
    next_cursor: str | None


@dataclass(frozen=True)
class ExternalReceipt:
    external_id: str
    idempotency_key: str
    duplicate: bool = False
    payload: dict[str, Any] = field(default_factory=dict)


class FakeConnector:
    """按 (resource, data_scope) 预置分页数据；写操作按幂等键服务端去重。

    超时语义与真实连接器一致：请求可能已经送达，只是回执丢失。因此首次发送时
    外部侧就已登记幂等键并分配外部单号；之后凭同一键重试，得到的是同一张回执
    （duplicate=True），不会产生第二次副作用。
    """

    def __init__(self, connector_id: str = "collab") -> None:
        self.connector_id = connector_id
        self._pages: dict[tuple[str, str], list[list[Any]]] = {}
        self._seen_writes: dict[str, ExternalReceipt] = {}
        self._fail_remaining: dict[str, int] = {}
        self.send_attempts: dict[str, int] = {}
        # 全局：接下来 n 次 send 超时（无论键），模拟服务短暂不可用
        self.fail_next: int = 0
        # 每次 read_page 前触发的回调，用于在请求途中收缩授权
        self.on_read: Callable[[str, str, str | None], None] | None = None

    def seed_pages(self, resource: str, data_scope: str, rows: list[Any],
                   page_size: int = 2) -> None:
        pages = [rows[i:i + page_size] for i in range(0, len(rows), page_size)]
        self._pages[(resource, data_scope)] = pages or [[]]

    def fail_send(self, idempotency_key: str, times: int = 1) -> None:
        """该幂等键接下来 times 次发送超时（但外部侧已受理），之后返回重复回执。"""
        self._fail_remaining[idempotency_key] = times

    def read_page(self, resource: str, data_scope: str,
                  cursor: str | None) -> Page:
        if self.on_read is not None:
            self.on_read(resource, data_scope, cursor)
        pages = self._pages.get((resource, data_scope))
        if pages is None:
            raise KeyError(f"连接器 {self.connector_id} 没有 {resource}/{data_scope} 的数据")
        index = 0 if cursor is None else int(cursor)
        rows = pages[index]
        next_cursor = str(index + 1) if index + 1 < len(pages) else None
        return Page(rows=rows, next_cursor=next_cursor)

    def send(self, resource: str, payload: Any,
             idempotency_key: str) -> ExternalReceipt:
        """服务端按幂等键去重：重复键返回首张回执，不产生第二次副作用。"""
        existing = self._seen_writes.get(idempotency_key)
        if existing is not None:
            if self._fail_remaining.get(idempotency_key, 0) > 0:
                self._fail_remaining[idempotency_key] -= 1
                self.send_attempts[idempotency_key] += 1
                raise TimeoutError(
                    f"写请求已受理但回执读取超时（resource={resource}）"
                )
            # 重试到达服务端：同键返回首张回执，不产生第二次副作用，但计入一次到达
            self.send_attempts[idempotency_key] += 1
            return ExternalReceipt(
                external_id=existing.external_id,
                idempotency_key=idempotency_key,
                duplicate=True,
                payload=existing.payload,
            )

        receipt = ExternalReceipt(
            external_id=f"ext-{idempotency_key}",
            idempotency_key=idempotency_key,
            payload={"resource": resource, "body": payload},
        )
        # 先登记，再决定是否回执超时：即便超时，外部侧也只承认这一次写入
        self._seen_writes[idempotency_key] = receipt
        self.send_attempts[idempotency_key] = 1
        failures = max(
            self._fail_remaining.get(idempotency_key, 0),
            1 if self.fail_next > 0 else 0,
        )
        if self.fail_next > 0:
            self.fail_next -= 1
        if failures > 0:
            self._fail_remaining[idempotency_key] = failures - 1
            raise TimeoutError(
                f"写请求已发出但在读取回执前超时（resource={resource}）"
            )
        return receipt
