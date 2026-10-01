"""Structured JSON logging shared by Master and Worker.

Logs are the observability backbone of a MapReduce run.  Each record is one
JSON line appended to ``jobs/{job_id}/logs/{stage}/{task_id}.jsonl``; because
records are appended atomically (via ``storage.append_jsonl``) many workers and
the master can emit concurrently without corruption.  ``query`` streams every
file under a job's log directory and filters, so the log-search page has a
single, simple backend.

Cluster-level events (node registered / lost / reaped / recovered, tasks
reassigned) do not belong to any job, so they are appended to a single
cluster-wide ``cluster/events.jsonl`` instead; ``query_cluster`` powers the
cluster-events timeline page.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from . import constants as C
from .jsonutil import now_ms
from .storage import Storage, list_files, read_jsonl_stream


class LogBus:
    """Emit and query structured log records stored as JSONL shards."""

    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    # -- emit ---------------------------------------------------------
    def emit(
        self,
        job_id: str,
        level: str,
        message: str,
        stage: str = "master",
        task_id: str = "job",
        worker_id: str = "",
        **extra: Any,
    ) -> dict:
        record: dict[str, Any] = {
            "ts_ms": now_ms(),
            "level": level,
            "stage": stage,
            "task_id": task_id,
            "worker_id": worker_id,
            "message": message,
        }
        record.update(extra)
        # Stage directory keeps the log shards grouped exactly as the frontend
        # log-search page expects (by stage and task).
        self.storage.append(record, "jobs", job_id, "logs", stage, f"{task_id}.jsonl")
        return record

    def debug(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_DEBUG, message, **kw)

    def info(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_INFO, message, **kw)

    def warn(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_WARN, message, **kw)

    def error(self, job_id: str, message: str, **kw: Any) -> dict:
        return self.emit(job_id, C.LOG_ERROR, message, **kw)

    # -- query --------------------------------------------------------
    def _log_root(self, job_id: str) -> str:
        return self.storage.path("jobs", job_id, "logs")

    def stages(self, job_id: str) -> list[str]:
        root = self._log_root(job_id)
        if not os.path.isdir(root):
            return []
        return sorted(e for e in os.listdir(root) if os.path.isdir(os.path.join(root, e)))

    def query(
        self,
        job_id: str,
        search: str = "",
        stage: str = "",
        task_id: str = "",
        level: str = "",
        limit: int = 500,
    ) -> dict:
        """Return matching log records (newest last) plus a summary count."""
        needle = (search or "").lower()
        records: list[dict] = []
        total_scanned = 0
        for path in list_files(self._log_root(job_id), suffix=".jsonl", recursive=True):
            rel = os.path.relpath(path, self._log_root(job_id))
            parts = rel.split(os.sep)
            f_stage = parts[0] if len(parts) > 1 else "master"
            f_task = os.path.basename(path)[: -len(".jsonl")]
            if stage and f_stage != stage:
                continue
            if task_id and f_task != task_id:
                continue
            for rec in read_jsonl_stream(path):
                total_scanned += 1
                if level and rec.get("level") != level:
                    continue
                if needle:
                    hay = _lower_record(rec)
                    if needle not in hay:
                        continue
                rec.setdefault("stage", f_stage)
                rec.setdefault("task_id", f_task)
                # Older shards stamped the timestamp as "ts"; normalise so
                # sorting and the frontend see one field.
                rec.setdefault("ts_ms", rec.get("ts", 0))
                records.append(rec)

        records.sort(key=lambda r: r.get("ts_ms", 0))
        total = len(records)
        return {
            "total": total,
            "scanned": total_scanned,
            "stages": self.stages(job_id),
            "records": records[:limit],
        }

    # -- cluster events -------------------------------------------------
    def _cluster_log_path(self) -> str:
        return self.storage.path("cluster", "events.jsonl")

    def emit_cluster(
        self,
        kind: str,
        message: str,
        level: str = C.LOG_INFO,
        worker_id: str = "",
        worker_name: str = "",
        job_id: str = "",
        task_id: str = "",
        **extra: Any,
    ) -> dict:
        """Append one cluster-level event (node lifecycle / reassignment).

        These events are job-independent, so they live in a single
        cluster-wide JSONL log that the events page queries as a timeline.
        """
        record: dict[str, Any] = {
            "ts_ms": now_ms(),
            "kind": kind,
            "level": level,
            "message": message,
            "worker_id": worker_id,
            "worker_name": worker_name,
            "job_id": job_id,
            "task_id": task_id,
        }
        record.update(extra)
        self.storage.append(record, "cluster", "events.jsonl")
        return record

    # Convenience emitters for node lifecycle transitions; the Master wires
    # them straight into the registry's on_register / on_recover callbacks.
    def cluster_worker_registered(self, worker: Any, is_new: bool, was_dead: bool) -> dict:
        if is_new:
            kind = C.EVENT_WORKER_REGISTERED
            msg = f"worker {worker.name} registered ({worker.host}:{worker.port})"
        elif was_dead:
            kind = C.EVENT_WORKER_RECOVERED
            msg = f"worker {worker.name} re-registered after being marked dead"
        else:
            kind = C.EVENT_WORKER_REGISTERED
            msg = f"worker {worker.name} re-registered ({worker.host}:{worker.port})"
        return self.emit_cluster(kind, msg, worker_id=worker.worker_id,
                                 worker_name=worker.name, rejoin=not is_new)

    def cluster_worker_recovered(self, worker: Any) -> dict:
        return self.emit_cluster(
            C.EVENT_WORKER_RECOVERED,
            f"worker {worker.name} recovered (heartbeat resumed)",
            worker_id=worker.worker_id, worker_name=worker.name,
        )

    def query_cluster(
        self,
        search: str = "",
        kind: str = "",
        worker: str = "",
        level: str = "",
        limit: int = 500,
    ) -> dict:
        """Return matching cluster events in chronological order.

        When more than ``limit`` events match, the *most recent* ones are
        kept — the timeline is for reviewing what just happened.
        """
        needle = (search or "").lower()
        worker_needle = (worker or "").lower()
        records: list[dict] = []
        kinds: set[str] = set()
        total_scanned = 0
        for rec in read_jsonl_stream(self._cluster_log_path()):
            total_scanned += 1
            rec_kind = rec.get("kind", "")
            if rec_kind:
                kinds.add(rec_kind)
            if kind and rec_kind != kind:
                continue
            if level and rec.get("level") != level:
                continue
            if worker_needle:
                hay = f"{rec.get('worker_id', '')} {rec.get('worker_name', '')}".lower()
                if worker_needle not in hay:
                    continue
            if needle and needle not in _lower_record(rec):
                continue
            records.append(rec)

        records.sort(key=lambda r: r.get("ts_ms", 0))
        total = len(records)
        if limit and total > limit:
            records = records[-limit:]
        return {
            "total": total,
            "scanned": total_scanned,
            "kinds": sorted(kinds),
            "records": records,
        }


def _lower_record(rec: dict) -> str:
    """Cheap case-insensitive haystack over a record's textual fields."""
    parts = [str(rec.get("message", ""))]
    for key in ("level", "stage", "task_id", "worker_id", "kind", "worker_name", "job_id"):
        parts.append(str(rec.get(key, "")))
    return " ".join(parts).lower()
