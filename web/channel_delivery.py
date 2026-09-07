"""Presentation delivery receipts, separate from Pi Task state and channel events."""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from diagnostics import current_request_id, record


class DeliveryBusy(RuntimeError):
    pass


class DeliveryReceiptStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)

    @contextmanager
    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=10)
        try:
            db.row_factory = sqlite3.Row
            with db:
                db.execute("""CREATE TABLE IF NOT EXISTS channel_delivery_receipts (
                    delivery_id TEXT PRIMARY KEY, task_run_id TEXT NOT NULL, command_id TEXT NOT NULL,
                    target_id TEXT NOT NULL, conversation_id TEXT NOT NULL, message_id TEXT,
                    status TEXT NOT NULL, error_code TEXT, request_id TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, lease_until REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    UNIQUE(command_id, task_run_id)
                )""")
                db.execute("CREATE INDEX IF NOT EXISTS delivery_task ON channel_delivery_receipts(task_run_id)")
            with db:
                yield db
        finally:
            db.close()

    def create(self, *, task_run_id: str, command_id: str, target_id: str, conversation_id: str, message_id: str | None = None) -> dict:
        now = time.time()
        with self._connect() as db:
            db.execute("""INSERT OR IGNORE INTO channel_delivery_receipts
                (delivery_id, task_run_id, command_id, target_id, conversation_id, message_id, status, request_id, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)""",
                (f"delivery_{uuid4().hex}", task_run_id, command_id, target_id, conversation_id, message_id, current_request_id(), now, now))
            return dict(db.execute("SELECT * FROM channel_delivery_receipts WHERE command_id = ? AND task_run_id = ?", (command_id, task_run_id)).fetchone())

    def get(self, delivery_id: str) -> dict | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM channel_delivery_receipts WHERE delivery_id = ?", (delivery_id,)).fetchone()
            return dict(row) if row else None

    def for_task(self, task_run_id: str) -> list[dict]:
        with self._connect() as db:
            return [self.public(dict(row)) for row in db.execute("SELECT * FROM channel_delivery_receipts WHERE task_run_id = ? ORDER BY updated_at DESC LIMIT 50", (task_run_id,))]

    def claim(self, delivery_id: str) -> dict:
        now = time.time()
        with self._connect() as db:
            changed = db.execute("""UPDATE channel_delivery_receipts SET status='sending', attempts=attempts+1,
                lease_until=?, updated_at=?, request_id=? WHERE delivery_id=? AND status != 'delivered' AND lease_until <= ?""",
                (now + 400, now, current_request_id(), delivery_id, now)).rowcount
            if not changed:
                raise DeliveryBusy("Delivery is complete or currently sending")
            return dict(db.execute("SELECT * FROM channel_delivery_receipts WHERE delivery_id=?", (delivery_id,)).fetchone())

    def finish(self, receipt: dict, status: str, *, message_id: str | None = None, error_code: str | None = None) -> dict:
        with self._connect() as db:
            db.execute("""UPDATE channel_delivery_receipts SET status=?, message_id=COALESCE(?,message_id), error_code=?,
                lease_until=0, updated_at=? WHERE delivery_id=? AND attempts=? AND status='sending'""",
                (status, message_id, error_code, time.time(), receipt["delivery_id"], receipt["attempts"]))
            result = dict(db.execute("SELECT * FROM channel_delivery_receipts WHERE delivery_id=?", (receipt["delivery_id"],)).fetchone())
        record("channel_delivery", delivery_id=receipt["delivery_id"], task_run_id=receipt["task_run_id"],
               command_id=receipt["command_id"], delivery_status=result["status"], error_code=result["error_code"])
        return result

    @staticmethod
    def public(receipt: dict) -> dict:
        fields = ("delivery_id", "task_run_id", "command_id", "status", "error_code", "request_id", "attempts", "created_at", "updated_at")
        result = {key: receipt[key] for key in fields}
        if result["status"] == "sending" and receipt["lease_until"] <= time.time():
            result["status"] = "unknown"
        result["retry_available"] = result["status"] in {"pending", "failed", "unknown"}
        return result


def get_delivery_store() -> DeliveryReceiptStore:
    from config import cfg
    return DeliveryReceiptStore(Path(cfg.QUERY_RUN_DB_PATH).parent / "channel_delivery.db")
