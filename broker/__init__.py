"""连接器最小权限会话代理。

公开对象：

- :class:`Broker`：会话代理服务入口。
- :class:`Demand` / :class:`Capability` / :class:`GrantSpec`：
  资源、动作、数据范围、有效时间四维模型。
- :class:`Reader`：分页读取器，逐页领取、逐页校验授权版本。
- :class:`ExternalReceipt` / :class:`PendingWrite`：写操作结果。
- :class:`FakeConnector` / :class:`ListPages`：测试与离线演练用的连接器桩。
- :class:`AuthorizationError` / :class:`IdempotencyPending` /
  :class:`DuplicateDeliveryError` 及 :class:`DenialReason`。
"""
from __future__ import annotations

from .errors import (
    AuthorizationError,
    BrokerError,
    Denial,
    DenialReason,
    DuplicateDeliveryError,
    IdempotencyPending,
    RemoteUnavailable,
)
from .catalog import Capability, DataScope, Demand, GrantSpec, minimum_capabilities
from .connectors import ExternalReceipt, FakeConnector, ListPages, Page
from .service import Broker, PendingWrite, Reader
from .storage import EventStore

__all__ = [
    "AuthorizationError",
    "Broker",
    "BrokerError",
    "Capability",
    "DataScope",
    "Demand",
    "Denial",
    "DenialReason",
    "DuplicateDeliveryError",
    "EventStore",
    "ExternalReceipt",
    "FakeConnector",
    "GrantSpec",
    "IdempotencyPending",
    "ListPages",
    "Page",
    "PendingWrite",
    "Reader",
    "RemoteUnavailable",
    "minimum_capabilities",
]
