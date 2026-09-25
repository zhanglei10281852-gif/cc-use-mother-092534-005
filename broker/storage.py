"""SQLite 持久化：仅使用标准库。事件日志只追加，状态表便于查询。"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

SCHEMA = """
create table if not exists event_log (
  seq integer primary key autoincrement,
  event_id text unique not null,
  event_type text not null,
  aggregate_id text not null,
  session_id text,
  version integer,
  payload text not null,
  occurred_at text not null,
  actor_id text not null
);

create table if not exists sessions (
  session_id text primary key,
  tenant_id text not null,
  connector_id text not null,
  owner_id text not null,
  task_id text not null,
  status text not null,
  version integer not null default 0,
  created_at text not null,
  updated_at text not null
);

create table if not exists session_demands (
  session_id text not null,
  resource text not null,
  action text not null,
  data_scope text not null,
  valid_from text not null,
  valid_to text not null,
  status text not null default 'requested',
  unique(session_id, resource, action, data_scope, valid_from, valid_to)
);

create table if not exists grants (
  id integer primary key autoincrement,
  session_id text not null,
  version integer not null,
  granted_by text not null,
  confirmed_at text not null,
  unique(session_id, version)
);

create table if not exists grant_entries (
  grant_id integer not null,
  resource text not null,
  action text not null,
  data_scope text not null,
  valid_from text not null,
  valid_to text not null
);

create table if not exists capabilities (
  id integer primary key autoincrement,
  session_id text not null,
  resource text not null,
  action text not null,
  data_scope text not null,
  valid_from text not null,
  valid_to text not null,
  status text not null default 'granted',
  reason text,
  granted_version integer not null,
  status_version integer not null,
  unique(session_id, resource, action, data_scope, valid_from, valid_to)
);

create table if not exists calls (
  call_id text primary key,
  session_id text not null,
  tenant_id text not null,
  connector_id text not null,
  resource text not null,
  action text not null,
  data_scope text not null,
  is_write integer not null,
  idempotency_key text,
  status text not null,
  pre_version integer,
  post_version integer,
  deny_reason text,
  payload text,
  result text,
  page_cursor text,
  exhausted integer not null default 0,
  attempts integer not null default 0,
  created_at text not null,
  updated_at text not null
);

create table if not exists pages_audit (
  id integer primary key autoincrement,
  call_id text not null,
  page_index integer not null,
  row_count integer not null,
  data text not null,
  delivered integer not null,
  received_version integer not null,
  fetched_at text not null,
  unique(call_id, page_index)
);

create table if not exists receipts (
  idempotency_key text primary key,
  tenant_id text not null,
  connector_id text not null,
  call_id text not null,
  external_id text,
  status text not null,
  payload text,
  attempts integer not null default 0,
  sent_at text,
  received_at text,
  reconciled_at text
);

create unique index if not exists idx_receipts_scope
  on receipts(tenant_id, connector_id, idempotency_key);

create table if not exists quota_limits (
  tenant_id text not null,
  connector_id text not null,
  resource text not null,
  action text not null,
  daily_limit integer not null,
  primary key (tenant_id, connector_id, resource, action)
);

create table if not exists quota_usage (
  tenant_id text not null,
  connector_id text not null,
  resource text not null,
  action text not null,
  bucket text not null,
  used_count integer not null default 0,
  primary key (tenant_id, connector_id, resource, action, bucket)
);
"""


def utc_now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self._lock = threading.RLock()
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("pragma foreign_keys = on")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    @contextmanager
    def lock(self):
        with self._lock:
            yield

    def commit(self) -> None:
        with self._lock:
            self.conn.commit()

    def rollback(self) -> None:
        with self._lock:
            self.conn.rollback()

    # ---- 事件 ----------------------------------------------------------
    def append_event(
        self,
        event_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        occurred_at: datetime,
        actor_id: str,
        session_id: str | None = None,
        version: int | None = None,
    ) -> str:
        event_id = new_id("evt")
        with self._lock:
            self.conn.execute(
                "insert into event_log(event_id, event_type, aggregate_id, session_id, "
                "version, payload, occurred_at, actor_id) values (?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    event_type,
                    aggregate_id,
                    session_id,
                    version,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    occurred_at.isoformat(),
                    actor_id,
                ),
            )
        return event_id

    def events(self, session_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "select * from event_log where session_id = ? order by seq", (session_id,)
            ).fetchall()
        return [self._event_row(r) for r in rows]

    @staticmethod
    def _event_row(r: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": r["event_id"],
            "event_type": r["event_type"],
            "aggregate_id": r["aggregate_id"],
            "version": r["version"],
            "payload": json.loads(r["payload"]),
            "occurred_at": r["occurred_at"],
            "actor_id": r["actor_id"],
        }

    def commit(self) -> None:
        with self._lock:
            self.conn.commit()

    # ---- 会话 ----------------------------------------------------------
    def create_session(self, s: dict[str, Any], now: datetime) -> None:
        with self._lock:
            self.conn.execute(
                "insert into sessions(session_id, tenant_id, connector_id, owner_id, task_id, "
                "status, version, created_at, updated_at) values (?,?,?,?,?,?,0,?,?)",
                (
                    s["session_id"], s["tenant_id"], s["connector_id"], s["owner_id"],
                    s["task_id"], "proposed", now.isoformat(), now.isoformat(),
                ),
            )

    def get_session(self, session_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "select * from sessions where session_id = ?", (session_id,)
            ).fetchone()

    def update_session_status(self, session_id: str, status: str, now: datetime) -> None:
        with self._lock:
            self.conn.execute(
                "update sessions set status = ?, updated_at = ? where session_id = ?",
                (status, now.isoformat(), session_id),
            )

    def bump_version(self, session_id: str, now: datetime) -> int:
        """调用方必须已持有锁的逻辑事务；返回新版本号。"""
        row = self.conn.execute(
            "select version from sessions where session_id = ?", (session_id,)
        ).fetchone()
        new_version = row["version"] + 1
        self.conn.execute(
            "update sessions set version = ?, updated_at = ? where session_id = ?",
            (new_version, now.isoformat(), session_id),
        )
        return new_version

    # ---- 需求与授权 -----------------------------------------------------
    def add_demands(self, session_id: str, caps: Iterable[tuple], status: str = "requested") -> None:
        with self._lock:
            for c in caps:
                self.conn.execute(
                    "insert or ignore into session_demands(session_id, resource, action, "
                    "data_scope, valid_from, valid_to, status) values (?,?,?,?,?,?,?)",
                    (session_id, c.resource, c.action, c.data_scope,
                     c.valid_from.isoformat(), c.valid_to.isoformat(), status),
                )

    def demands(self, session_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "select * from session_demands where session_id = ?", (session_id,)
            ).fetchall()

    def add_grant(self, session_id: str, version: int, granted_by: str,
                  caps: Iterable[tuple], now: datetime) -> None:
        with self._lock:
            cur = self.conn.execute(
                "insert into grants(session_id, version, granted_by, confirmed_at) values (?,?,?,?)",
                (session_id, version, granted_by, now.isoformat()),
            )
            grant_id = cur.lastrowid
            self.conn.executemany(
                "insert into grant_entries(grant_id, resource, action, data_scope, "
                "valid_from, valid_to) values (?,?,?,?,?,?)",
                [
                    (grant_id, c.resource, c.action, c.data_scope,
                     c.valid_from.isoformat(), c.valid_to.isoformat())
                    for c in caps
                ],
            )

    def add_capabilities(self, session_id: str, caps: Iterable[tuple], version: int) -> None:
        with self._lock:
            for c in caps:
                self.conn.execute(
                    "insert into capabilities(session_id, resource, action, data_scope, "
                    "valid_from, valid_to, status, granted_version, status_version) "
                    "values (?,?,?,?,?,?,'granted',?,?) "
                    "on conflict do nothing",
                    (session_id, c.resource, c.action, c.data_scope,
                     c.valid_from.isoformat(), c.valid_to.isoformat(), version, version),
                )

    def capabilities(self, session_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "select * from capabilities where session_id = ? order by id", (session_id,)
            ).fetchall()

    def find_capability(self, session_id: str, resource: str, action: str,
                        data_scope: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "select * from capabilities where session_id = ? and resource = ? "
                "and action = ? and data_scope = ?",
                (session_id, resource, action, data_scope),
            ).fetchone()

    def set_capability_status(self, cap_id: int, status: str, version: int,
                              reason: str | None) -> None:
        with self._lock:
            self.conn.execute(
                "update capabilities set status = ?, status_version = ?, reason = ? where id = ?",
                (status, version, reason, cap_id),
            )

    def has_non_granted(self, session_id: str) -> bool:
        with self._lock:
            row = self.conn.execute(
                "select count(*) as n from capabilities where session_id = ? and status != 'granted'",
                (session_id,),
            ).fetchone()
            return row["n"] > 0

    # ---- 调用 ----------------------------------------------------------
    def insert_call(self, call: dict[str, Any], now: datetime) -> None:
        with self._lock:
            self.conn.execute(
                "insert into calls(call_id, session_id, tenant_id, connector_id, resource, "
                "action, data_scope, is_write, idempotency_key, status, pre_version, "
                "post_version, deny_reason, payload, result, page_cursor, exhausted, "
                "attempts, created_at, updated_at) values "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    call["call_id"], call["session_id"], call["tenant_id"], call["connector_id"],
                    call["resource"], call["action"], call["data_scope"],
                    1 if call.get("is_write") else 0, call.get("idempotency_key"),
                    call["status"], call.get("pre_version"), call.get("post_version"),
                    call.get("deny_reason"),
                    json.dumps(call["payload"], ensure_ascii=False) if call.get("payload") is not None else None,
                    json.dumps(call.get("result"), ensure_ascii=False) if call.get("result") is not None else None,
                    call.get("page_cursor"), 1 if call.get("exhausted") else 0,
                    call.get("attempts", 0), now.isoformat(), now.isoformat(),
                ),
            )

    def get_call(self, call_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute("select * from calls where call_id = ?", (call_id,)).fetchone()

    def update_call(self, call_id: str, **fields: Any) -> None:
        fields.pop("created_at", None)
        if not fields:
            return
        if "payload" in fields:
            fields["payload"] = json.dumps(fields["payload"], ensure_ascii=False)
        if "result" in fields and fields["result"] is not None:
            fields["result"] = json.dumps(fields["result"], ensure_ascii=False)
        if "exhausted" in fields:
            fields["exhausted"] = 1 if fields["exhausted"] else 0
        assignments = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [utc_now_iso(), call_id]
        with self._lock:
            self.conn.execute(
                f"update calls set {assignments}, updated_at = ? where call_id = ?", values
            )

    def calls_of_session(self, session_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "select * from calls where session_id = ? order by created_at", (session_id,)
            ).fetchall()

    def add_page_audit(self, call_id: str, page_index: int, rows: list[Any],
                       delivered: bool, version: int, now: datetime) -> None:
        with self._lock:
            self.conn.execute(
                "insert into pages_audit(call_id, page_index, row_count, data, delivered, "
                "received_version, fetched_at) values (?,?,?,?,?,?,?)",
                (call_id, page_index, len(rows),
                 json.dumps(rows, ensure_ascii=False), 1 if delivered else 0,
                 version, now.isoformat()),
            )

    def pages_audit(self, call_id: str) -> list[sqlite3.Row]:
        with self._lock:
            return self.conn.execute(
                "select * from pages_audit where call_id = ? order by page_index", (call_id,)
            ).fetchall()

    def next_page_index(self, call_id: str) -> int:
        with self._lock:
            row = self.conn.execute(
                "select count(*) as n from pages_audit where call_id = ?", (call_id,)
            ).fetchone()
            return row["n"]

    # ---- 回执 ----------------------------------------------------------
    def insert_receipt(self, receipt: dict[str, Any], now: datetime) -> None:
        with self._lock:
            self.conn.execute(
                "insert into receipts(idempotency_key, tenant_id, connector_id, call_id, "
                "external_id, status, payload, attempts, sent_at, received_at, reconciled_at) "
                "values (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    receipt["idempotency_key"], receipt["tenant_id"], receipt["connector_id"],
                    receipt["call_id"], receipt.get("external_id"), receipt["status"],
                    json.dumps(receipt.get("payload"), ensure_ascii=False)
                    if receipt.get("payload") is not None else None,
                    receipt.get("attempts", 0),
                    now.isoformat() if receipt["status"] != "pending" else None,
                    now.isoformat() if receipt.get("received") else None,
                    None,
                ),
            )

    def get_receipt(self, idempotency_key: str) -> sqlite3.Row | None:
        with self._lock:
            return self.conn.execute(
                "select * from receipts where idempotency_key = ?", (idempotency_key,)
            ).fetchone()

    def pending_receipts(self) -> list[sqlite3.Row]:
        with self._lock:
            return self.conn.execute("select * from receipts where status = 'pending'").fetchall()

    def mark_receipt_received(self, key: str, external_id: str, payload: Any,
                              now: datetime, increment_attempt: bool = True) -> None:
        with self._lock:
            self.conn.execute(
                "update receipts set status = 'received', external_id = ?, payload = ?, "
                "attempts = attempts + ?, sent_at = coalesce(sent_at, ?), received_at = ? "
                "where idempotency_key = ?",
                (external_id, json.dumps(payload, ensure_ascii=False),
                 1 if increment_attempt else 0, now.isoformat(), now.isoformat(), key),
            )

    def mark_receipt_reconciled(self, key: str, now: datetime) -> None:
        with self._lock:
            self.conn.execute(
                "update receipts set status = 'reconciled', reconciled_at = ? "
                "where idempotency_key = ?",
                (now.isoformat(), key),
            )

    def touch_receipt_attempt(self, key: str) -> None:
        with self._lock:
            self.conn.execute(
                "update receipts set attempts = attempts + 1 where idempotency_key = ?", (key,)
            )

    # ---- 额度 ----------------------------------------------------------
    def set_quota(self, tenant_id: str, connector_id: str, resource: str,
                  action: str, daily_limit: int) -> None:
        with self._lock:
            self.conn.execute(
                "insert into quota_limits(tenant_id, connector_id, resource, action, daily_limit) "
                "values (?,?,?,?,?) on conflict(tenant_id, connector_id, resource, action) "
                "do update set daily_limit = excluded.daily_limit",
                (tenant_id, connector_id, resource, action, daily_limit),
            )

    def quota_remaining(self, tenant_id: str, connector_id: str, resource: str,
                        action: str, bucket: str) -> int | None:
        """None 表示不限制；否则返回当日剩余额度。"""
        with self._lock:
            limit_row = self.conn.execute(
                "select daily_limit from quota_limits where tenant_id=? and connector_id=? "
                "and resource=? and action=?",
                (tenant_id, connector_id, resource, action),
            ).fetchone()
            if limit_row is None:
                return None
            used_row = self.conn.execute(
                "select used_count from quota_usage where tenant_id=? and connector_id=? "
                "and resource=? and action=? and bucket=?",
                (tenant_id, connector_id, resource, action, bucket),
            ).fetchone()
            used = used_row["used_count"] if used_row else 0
            return max(0, limit_row["daily_limit"] - used)

    def consume_quota(self, tenant_id: str, connector_id: str, resource: str,
                      action: str, bucket: str) -> None:
        with self._lock:
            self.conn.execute(
                "insert into quota_usage(tenant_id, connector_id, resource, action, bucket, "
                "used_count) values (?,?,?,?,?,1) on conflict(tenant_id, connector_id, "
                "resource, action, bucket) do update set used_count = used_count + 1",
                (tenant_id, connector_id, resource, action, bucket),
            )
