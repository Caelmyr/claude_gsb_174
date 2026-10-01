"""Durable bus for cluster-level events that are not owned by one job.

Unlike :class:`LogBus`, these records describe the Master's view of the whole
cluster: worker registration/reregistration, heartbeat loss, resource reaping,
and task reassignment.  Keeping them in one cluster-scoped JSONL stream lets the
UI reconstruct a complete chronological timeline even when the affected tasks
belong to different jobs (or to no job at all).
"""

from __future__ import annotations

import itertools
import threading
from typing import Any, Optional

from . import constants as C
from .ids import new_id
from .jsonutil import now_ms
from .models import ClusterEvent
from .storage import Storage, read_jsonl_stream


class EventBus:
    """Append and query cluster lifecycle events."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self._seq_lock = threading.Lock()
        self._seq = itertools.count(1)

    @property
    def path(self) -> str:
        return self.storage.path("cluster", "events.jsonl")

    def emit(
        self,
        kind: str,
        message: str,
        *,
        level: str = C.LOG_INFO,
        worker_id: str = "",
        job_id: str = "",
        task_id: str = "",
        detail: Optional[dict[str, Any]] = None,
    ) -> dict:
        with self._seq_lock:
            seq = next(self._seq)
            created_ms = now_ms()
        event = ClusterEvent(
            event_id=new_id("event"),
            kind=kind,
            message=message,
            created_ms=created_ms,
            seq=seq,
            level=level,
            worker_id=worker_id,
            job_id=job_id,
            task_id=task_id,
            detail=detail or {},
        )
        record = event.to_dict()
        self.storage.append(record, "cluster", "events.jsonl")
        return record

    def list_events(
        self,
        *,
        kind: str = "",
        worker_id: str = "",
        job_id: str = "",
        search: str = "",
        limit: int = 1000,
    ) -> list[dict]:
        """Return matching events in chronological order."""
        needle = (search or "").lower()
        events: list[dict] = []
        for rec in read_jsonl_stream(self.path):
            if kind and rec.get("kind") != kind:
                continue
            if worker_id and rec.get("worker_id") != worker_id:
                continue
            if job_id and rec.get("job_id") != job_id:
                continue
            if needle and needle not in _haystack(rec):
                continue
            events.append(rec)

        events.sort(
            key=lambda e: (e.get("created_ms", 0), e.get("seq", 0), e.get("event_id", ""))
        )
        if limit > 0:
            events = events[-limit:]
        return events


def _haystack(rec: dict) -> str:
    parts = [
        str(rec.get("message", "")),
        str(rec.get("kind", "")),
        str(rec.get("worker_id", "")),
        str(rec.get("job_id", "")),
        str(rec.get("task_id", "")),
    ]
    for value in (rec.get("detail") or {}).values():
        parts.append(str(value))
    return " ".join(parts).lower()
