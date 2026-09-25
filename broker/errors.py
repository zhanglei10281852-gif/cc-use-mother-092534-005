"""异常与拒绝原因。"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class BrokerError(Exception):
    """所有会话代理错误的基类。"""


class AuthorizationError(BrokerError):
    """调用未通过授权校验。"""

    def __init__(self, reason: "DenialReason", detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        message = reason.value
        if detail:
            message = f"{message}：{detail}"
        super().__init__(message)


class IdempotencyPending(BrokerError):
    """同一幂等键的写操作结果未知，禁止按新请求重发。

    典型场景：请求已送达外部但回执尚未落库时进程崩溃或连接器不可用。
    调用方必须走 :meth:`Broker.reconcile_writes` 核对结果，
    而不是重新提交。
    """


class RemoteUnavailable(BrokerError):
    """连接器暂时不可用（5xx / 超时），调用可稍后重试或对账。"""


class AmbiguousDeliveryError(BrokerError):
    """连接器无法确认幂等键是否已送达，存在重复发送风险，需人工介入。"""


class DuplicateDeliveryError(BrokerError):
    """连接器侧已存在相同幂等键的不同请求体，拒绝写入。"""


class DenialReason(str, Enum):
    """授权拒绝原因，直接出现在管理员接口的审计视图中。"""

    NO_SESSION = "no_session"                 # 会话不存在或已关闭
    SESSION_VERSION_STALE = "session_version_stale"  # 调用方持有的会话版本过期
    CAPABILITY_MISSING = "capability_missing"         # 任务需求未获授权
    SCOPE_EXCEEDED = "scope_exceeded"                # 调用参数超出授权数据范围
    CAPABILITY_ISOLATED = "capability_isolated"      # 能力已被管理员隔离
    CAPABILITY_REVOKED = "capability_revoked"        # 能力已被撤销
    EXPIRED = "expired"                              # 授权已过有效时间
    GRANT_NOT_MINIMAL = "grant_not_minimal"          # 拟授权范围超出任务最小集合
    RESOURCE_OWNER_RECONFIRM_REQUIRED = "resource_owner_reconfirm_required"  # 扩范围需重新确认
    REMOTE_FAILURE = "remote_failure"                # 连接器故障（不计入拒绝，单独统计）
    QUOTA_EXCEEDED = "quota_exceeded"                # 剩余额度不足


@dataclass(frozen=True, slots=True)
class Denial:
    """一次拒绝的审计记录。"""

    reason: DenialReason
    detail: str
    at: str
    capability_code: str | None = None
