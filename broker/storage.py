"""基于 SQLite 的追加式事件存储。

所有状态变更都以事件形式追加，不覆盖历史（领域合同的版本策略）。
进程崩溃或服务重启后，:class:`Broker` 从事件流重放即可恢复
进行中的会话与待核销回执。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class Event:
    event_id: str
    aggregate_type: str
    aggregate_id: str
    seq: int
    event_type: str
    payload: dict[str, Any]
    occurred_at: str
    actor_id: str


class EventStore:
    """单文件 SQLite 事件日志；``:memory:`` 用于测试。"""

    SCHEMA = """
    create table if not exists event_log (
        event_id       text not null,
        aggregate_type text not null,
        aggregate_id   text not null,
        seq            integer not null,
        event_type     text not null,
        payload        text not null,
        occurred_at    text not null,
        actor_id       text not null,
        primary key (aggregate_type, aggregate_id, seq)
    );
    create table if not exists event_id_index (
        event_id text primary key
    );
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.executescript(self.SCHEMA)
        self._connection.commit()
        self._seq: dict[tuple[str, str], int] = {}
        for row in self._connection.execute(
            "select aggregate_type, aggregate_id, max(seq) as m from event_log group by 1, 2"
        ):
            self._seq[(row["aggregate_type"], row["aggregate_id"])] = row["m"] or 0

    @property
    def path(self) -> str:
        return self._path

    def append(
        self,
        event_id: str,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, Any],
        actor_id: str,
        occurred_at: datetime | None = None,
    ) -> Event:
        """追加一个事件；event_id 全局唯一，聚合内 seq 严格递增。"""
        stamp = (occurred_at or utc_now()).isoformat()
        with self._lock, self._connection:
            key = (aggregate_type, aggregate_id)
            seq = self._seq.get(key, 0) + 1
            # event_id 幂等：同一事件编号重复提交直接报错，不允许静默覆盖。
            exists = self._connection.execute(
                "select 1 from event_id_index where event_id = ?", (event_id,)
            ).fetchone()
            if exists is not None:
                raise ValueError(f"事件编号已存在：{event_id}")
            event = Event(event_id, aggregate_type, aggregate_id, seq, event_type, payload, stamp, actor_id)
            self._connection.execute(
                "insert into event_log values (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event.event_id,
                    event.aggregate_type,
                    event.aggregate_id,
                    event.seq,
                    event.event_type,
                    json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
                    event.occurred_at,
                    event.actor_id,
                ),
            )
            self._connection.execute(
                "insert into event_id_index(event_id) values (?)", (event_id,)
            )
            self._seq[key] = seq
            return event

    def stream(self, aggregate_type: str, aggregate_id: str) -> Iterator[Event]:
        with self._lock:
            rows = self._connection.execute(
                "select * from event_log where aggregate_type = ? and aggregate_id = ? order by seq",
                (aggregate_type, aggregate_id),
            ).fetchall()
        for row in rows:
            yield Event(
                event_id=row["event_id"],
                aggregate_type=row["aggregate_type"],
                aggregate_id=row["aggregate_id"],
                seq=row["seq"],
                event_type=row["event_type"],
                payload=json.loads(row["payload"]),
                occurred_at=row["occurred_at"],
                actor_id=row["actor_id"],
            )

    def scan(self, event_type: str | None = None) -> list[Event]:
        with self._lock:
            if event_type is None:
                rows = self._connection.execute(
                    "select * from event_log order by rowid"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "select * from event_log where event_type = ? order by rowid",
                    (event_type,),
                ).fetchall()
        return [
            Event(
                event_id=row["event_id"],
                aggregate_type=row["aggregate_type"],
                aggregate_id=row["aggregate_id"],
                seq=row["seq"],
                event_type=row["event_type"],
                payload=json.loads(row["payload"]),
                occurred_at=row["occurred_at"],
                actor_id=row["actor_id"],
            )
            for row in rows
        ]

    def list_aggregates(self, aggregate_type: str) -> list[str]:
        with self._lock:
            rows = self._connection.execute(
                "select distinct aggregate_id from event_log where aggregate_type = ? order by aggregate_id",
                (aggregate_type,),
            ).fetchall()
        return [row["aggregate_id"] for row in rows]

    def close(self) -> None:
        with self._lock:
            self._connection.commit()
            self._connection.close()
