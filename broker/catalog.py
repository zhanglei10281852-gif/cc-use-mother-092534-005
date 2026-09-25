"""四维能力模型与最小能力集合计算。

一个能力（capability）由四个维度构成：

- resource：外部资源类型，如 calendar / mail / drive / contacts。
- action：read 或 write。
- data_scope：数据范围，形如 ``{"field": "kind", "in": ["event"]}``，
  与连接器约定的资源属性做子集匹配。
- valid_until：有效时间（带时区），到期自动失效。

任务需求 :class:`Demand` 同样按四维描述。会话实际授予的能力必须是
任务需求的最小覆盖集合：不允许出现需求之外的资源、动作或数据范围，
有效期也不得晚于任务要求的最晚时刻。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

# 动作全集：读、写。
READ = "read"
WRITE = "write"


@dataclass(frozen=True, slots=True)
class DataScope:
    """数据范围：要求资源属性 ``field`` 的取值落在 ``allowed`` 内。

    - ``allowed is None``：通配，表示该资源登记目录中的全部取值；
    - ``allowed`` 为空集合：零授权，任何数据都不匹配；
    - 否则按集合包含关系匹配。

    使用单一等值字段足以覆盖日历/邮件/云盘/通讯录的类别隔离；
    更复杂的谓词可以在这里扩展，最小性判定保持同样的集合语义。
    """

    field: str
    allowed: Optional[frozenset[str]]

    @classmethod
    def of(cls, field: str, values: Iterable[str]) -> DataScope:
        return cls(field=field, allowed=frozenset(values))

    @classmethod
    def everything(cls, field: str = "kind") -> DataScope:
        """不做类别收窄的通配范围。"""
        return cls(field=field, allowed=None)

    @classmethod
    def nothing(cls, field: str = "kind") -> DataScope:
        return cls(field=field, allowed=frozenset())

    def covers(self, other: DataScope) -> bool:
        """``self`` 是否覆盖 ``other``：字段相同且取值集合为超集。"""
        if self.field != other.field:
            return False
        if self.allowed is None:
            return True
        if other.allowed is None:
            return False
        return other.allowed <= self.allowed

    def intersection(self, other: DataScope) -> DataScope:
        """范围收缩时使用：两个范围的交集。"""
        if self.field != other.field:
            raise ValueError("数据范围字段不一致，无法求交")
        if self.allowed is None:
            return other
        if other.allowed is None:
            return self
        return DataScope(self.field, self.allowed & other.allowed)

    def is_empty(self) -> bool:
        return self.allowed is not None and len(self.allowed) == 0

    def to_dict(self) -> dict[str, Any]:
        return {"field": self.field, "in": None if self.allowed is None else sorted(self.allowed)}

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> DataScope:
        values = raw.get("in")
        return cls(field=raw["field"], allowed=None if values is None else frozenset(values))


@dataclass(frozen=True, slots=True)
class Demand:
    """任务对单个资源的一次使用需求。"""

    resource: str
    action: str
    scope: DataScope
    valid_until: datetime

    def __post_init__(self) -> None:
        if self.action not in (READ, WRITE):
            raise ValueError(f"未知动作：{self.action}")
        if self.valid_until.tzinfo is None:
            raise ValueError("valid_until 必须带时区")

    def key(self) -> tuple[str, str, str]:
        return (self.resource, self.action, self.scope.field)


@dataclass(frozen=True, slots=True)
class GrantSpec:
    """去版本化的能力描述，用于最小集合计算与变更比较。"""

    resource: str
    action: str
    scope: DataScope
    valid_until: datetime

    def __post_init__(self) -> None:
        if self.valid_until.tzinfo is None:
            raise ValueError("valid_until 必须带时区")

    def key(self) -> tuple[str, str, str]:
        return (self.resource, self.action, self.scope.field)

    def covers(self, demand: Demand, now: datetime) -> bool:
        return (
            self.resource == demand.resource
            and self.action == demand.action
            and self.scope.covers(demand.scope)
            and now < self.valid_until
            and self.valid_until <= demand.valid_until
        )


@dataclass(frozen=True, slots=True)
class Capability:
    """会话持有的单项能力，带授权版本号。"""

    code: str
    resource: str
    action: str
    scope: DataScope
    valid_until: datetime
    version: int = 1

    def __post_init__(self) -> None:
        if self.action not in (READ, WRITE):
            raise ValueError(f"未知动作：{self.action}")
        if self.valid_until.tzinfo is None:
            raise ValueError("valid_until 必须带时区")

    @property
    def granted(self) -> GrantSpec:
        return GrantSpec(self.resource, self.action, self.scope, self.valid_until)

    def covers_demand(self, demand: Demand, now: datetime) -> bool:
        return self.granted.covers(demand, now)


def minimum_capabilities(demands: Iterable[Demand]) -> list[GrantSpec]:
    """把任务需求压缩成最小能力集合。

    合并规则（保持最小性）：

    - 资源、动作、范围字段相同的需求合并为一条；
    - 范围取并集（覆盖全部所需类别，不多给任何一个类别）；
    - 有效期取该组内最晚的需求截止时间——恰能覆盖最晚的那次调用，
      再晚一刻都会超出所有任务的实际需要。
    """
    groups: dict[tuple[str, str, str], list[Demand]] = {}
    for demand in demands:
        groups.setdefault(demand.key(), []).append(demand)
    grants: list[GrantSpec] = []
    for (resource, action, field), group in groups.items():
        wildcard = False
        allowed: set[str] = set()
        deadline = group[0].valid_until
        for demand in group:
            if demand.scope.allowed is None:
                wildcard = True
            else:
                allowed.update(demand.scope.allowed)
            if demand.valid_until > deadline:
                deadline = demand.valid_until
        scope = DataScope(field, None) if wildcard else DataScope(field, frozenset(allowed))
        grants.append(GrantSpec(resource=resource, action=action, scope=scope, valid_until=deadline))
    grants.sort(key=lambda g: (g.resource, g.action, g.scope.field))
    return grants
