"""错误与拒绝原因编码。管理接口中的“拒绝原因”直接使用这里的编码。"""
from __future__ import annotations


class BrokerError(Exception):
    """所有会话代理错误的基类。"""


class InvalidTransition(BrokerError):
    """状态机不允许的流转。"""


class AuthorizationDenied(BrokerError):
    def __init__(self, reason: str, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason}: {detail}" if detail else reason)


class DeliveryUncertain(BrokerError):
    """外部写请求可能已经送达，本地尚无回执；禁止盲目重发，等待对账。"""

    def __init__(self, call_id: str, idempotency_key: str) -> None:
        self.call_id = call_id
        self.idempotency_key = idempotency_key
        super().__init__(
            f"写操作 {call_id}（幂等键 {idempotency_key}）送达状态不明，已转为待对账"
        )


# 拒绝原因编码
SESSION_NOT_ACTIVE = "session_not_active"
SESSION_RECONCILING = "session_reconciling"
SESSION_CLOSED = "session_closed"
OWNER_MISMATCH = "owner_mismatch"
CAPABILITY_NOT_GRANTED = "capability_not_granted"
EXPANSION_UNCONFIRMED = "expansion_unconfirmed"
CAPABILITY_ISOLATED = "capability_isolated"
CAPABILITY_REVOKED = "capability_revoked"
SCOPE_REDUCED = "scope_reduced"
NOT_YET_VALID = "not_yet_valid"
EXPIRED = "expired"
QUOTA_EXHAUSTED = "quota_exhausted"
