"""领域值对象：能力（资源、动作、数据范围、有效时间四维）与最小集合计算。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Sequence


@dataclass(frozen=True)
class Capability:
    """一次授权的最小单元，四个维度缺一不可。"""

    resource: str
    action: str
    data_scope: str
    valid_from: datetime
    valid_to: datetime

    def __post_init__(self) -> None:
        if self.valid_from.tzinfo is None or self.valid_to.tzinfo is None:
            raise ValueError("能力的有效时间必须带时区")
        if self.valid_to <= self.valid_from:
            raise ValueError("能力的失效时间必须晚于生效时间")

    @property
    def key(self) -> str:
        return "|".join(
            [
                self.resource,
                self.action,
                self.data_scope,
                self.valid_from.isoformat(),
                self.valid_to.isoformat(),
            ]
        )

    def active_at(self, moment: datetime) -> bool:
        if moment.tzinfo is None:
            raise ValueError("校验时间必须带时区")
        return self.valid_from <= moment <= self.valid_to

    def to_dict(self) -> dict:
        return {
            "resource": self.resource,
            "action": self.action,
            "data_scope": self.data_scope,
            "valid_from": self.valid_from.isoformat(),
            "valid_to": self.valid_to.isoformat(),
        }


def minimum_set(demands: Iterable[Capability]) -> list[Capability]:
    """把任务需求折叠成去重后的最小能力集合——不多给任何一维。"""
    by_key: dict[str, Capability] = {}
    for demand in demands:
        by_key.setdefault(demand.key, demand)
    return sorted(by_key.values(), key=lambda item: item.key)


def diff_capabilities(
    granted: Sequence[Capability], requested: Iterable[Capability]
) -> tuple[list[Capability], list[Capability]]:
    """返回 (新增能力, 被收窄掉的能力)。新增必须资源所有者重新确认。"""
    wanted = minimum_set(requested)
    granted_by_key = {item.key: item for item in granted}
    wanted_by_key = {item.key: item for item in wanted}
    added = [item for item in wanted if item.key not in granted_by_key]
    removed = [item for item in granted if item.key not in wanted_by_key]
    return added, removed
