"""Tests for cluster-level events: node lifecycle + reassignment timeline.

Cluster events (node registered / lost / reaped / recovered, tasks reassigned)
are job-independent, so they live in a single cluster-wide log queried via
``LogBus.query_cluster``.  These tests wire the components exactly like
``Master.__init__`` does and replay a full node lifecycle, asserting the
timeline matches what actually happened.
"""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.registry import WorkerRegistry


def make_cluster(tmp):
    """Build the master-side components wired exactly like Master.__init__."""
    storage = Storage(tmp)
    config = ClusterConfig()
    logbus = LogBus(storage)
    jm = JobManager(storage, config, logbus)
    ft = FaultTolerance(storage, jm, config, logbus)
    registry = WorkerRegistry(storage, config)
    registry.on_death = ft.handle_worker_death
    registry.on_register = logbus.cluster_worker_registered
    registry.on_recover = logbus.cluster_worker_recovered
    return storage, logbus, jm, ft, registry


def register(registry, wid, name=None, port=9001):
    return registry.register({
        "worker_id": wid, "name": name or wid, "host": "127.0.0.1", "port": port,
        "cpu_cores": 4, "mem_total_mb": 4096,
    })


class TestClusterEventLog(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.logbus = LogBus(self.storage)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_emit_and_query_chronological(self):
        self.logbus.emit_cluster(C.EVENT_WORKER_REGISTERED, "w1 registered", worker_id="w1")
        self.logbus.emit_cluster(C.EVENT_WORKER_DEAD, "w1 lost", level=C.LOG_WARN, worker_id="w1")
        self.logbus.emit_cluster(C.EVENT_WORKER_RECOVERED, "w1 back", worker_id="w1")

        res = self.logbus.query_cluster()
        self.assertEqual(res["total"], 3)
        self.assertEqual(res["scanned"], 3)
        self.assertEqual([r["kind"] for r in res["records"]], [
            C.EVENT_WORKER_REGISTERED, C.EVENT_WORKER_DEAD, C.EVENT_WORKER_RECOVERED,
        ])
        ts = [r["ts_ms"] for r in res["records"]]
        self.assertEqual(ts, sorted(ts))
        self.assertEqual(res["kinds"], sorted([
            C.EVENT_WORKER_REGISTERED, C.EVENT_WORKER_DEAD, C.EVENT_WORKER_RECOVERED,
        ]))

    def test_query_filters(self):
        self.logbus.emit_cluster(C.EVENT_WORKER_REGISTERED, "worker-1 registered",
                                 worker_id="w1", worker_name="worker-1")
        self.logbus.emit_cluster(C.EVENT_WORKER_DEAD, "worker-1 lost", level=C.LOG_WARN,
                                 worker_id="w1", worker_name="worker-1")
        self.logbus.emit_cluster(C.EVENT_TASK_REASSIGNED, "task m-0001 reassigned off dead worker-1",
                                 level=C.LOG_WARN, worker_id="w1", worker_name="worker-1",
                                 job_id="j1", task_id="m-0001")
        self.logbus.emit_cluster(C.EVENT_WORKER_RECOVERED, "worker-2 recovered",
                                 worker_id="w2", worker_name="worker-2")

        by_kind = self.logbus.query_cluster(kind=C.EVENT_WORKER_DEAD)
        self.assertEqual(by_kind["total"], 1)
        self.assertEqual(by_kind["records"][0]["message"], "worker-1 lost")

        warns = self.logbus.query_cluster(level=C.LOG_WARN)
        self.assertEqual(warns["total"], 2)

        by_worker = self.logbus.query_cluster(worker="w1")
        self.assertEqual(by_worker["total"], 3)
        # worker filter also matches the display name, case-insensitively
        by_name = self.logbus.query_cluster(worker="WORKER-2")
        self.assertEqual(by_name["total"], 1)

        by_search = self.logbus.query_cluster(search="m-0001")
        self.assertEqual(by_search["total"], 1)
        self.assertEqual(by_search["records"][0]["kind"], C.EVENT_TASK_REASSIGNED)

    def test_limit_keeps_most_recent(self):
        for i in range(5):
            self.logbus.emit_cluster(C.EVENT_WORKER_REGISTERED, f"event {i}")
        res = self.logbus.query_cluster(limit=3)
        self.assertEqual(res["total"], 5)
        self.assertEqual([r["message"] for r in res["records"]],
                         ["event 2", "event 3", "event 4"])

    def test_job_logs_carry_sortable_timestamps(self):
        self.logbus.info("job1", "first")
        self.logbus.info("job1", "second")
        res = self.logbus.query("job1")
        self.assertEqual([r["message"] for r in res["records"]], ["first", "second"])
        for rec in res["records"]:
            self.assertGreater(rec["ts_ms"], 0)

        # Legacy shards stamped "ts" instead of "ts_ms" still sort correctly.
        self.storage.append({"ts": 5, "level": "INFO", "message": "old"},
                            "jobs", "job1", "logs", "master", "legacy.jsonl")
        res = self.logbus.query("job1")
        self.assertEqual(res["records"][0]["message"], "old")
        self.assertEqual(res["records"][0]["ts_ms"], 5)


class TestNodeLifecycleTimeline(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage, self.logbus, self.jm, self.ft, self.registry = make_cluster(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _events(self):
        return self.logbus.query_cluster(limit=1000)["records"]

    def test_full_lifecycle_matches_reality(self):
        # 1. Two workers register.
        register(self.registry, "w1", "worker-1", 9001)
        register(self.registry, "w2", "worker-2", 9002)

        # 2. A job runs one of its map tasks on w1.
        job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 2, "num_reduce_tasks": 1, "input_rows": 50, "params": {},
        })
        task = self.jm.tasks_for(job.job_id, C.TASK_MAP)[0]
        self.jm.update_task(job.job_id, task.task_id,
                            status=C.TASK_RUNNING, worker_id="w1")

        # 3. w1 stops heartbeating and gets reaped.
        self.registry.get("w1").last_heartbeat_ms = 0
        newly_dead = self.registry.reap()
        self.assertEqual([w.worker_id for w in newly_dead], ["w1"])

        # The in-flight task was reassigned (back to RETRYING, no worker).
        task = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(task.status, C.TASK_RETRYING)
        self.assertIsNone(task.worker_id)

        # 4. w1 comes back (heartbeat resumes).
        self.registry.heartbeat({"worker_id": "w1"})
        self.assertTrue(self.registry.get("w1").is_alive)

        # The cluster timeline replays exactly what happened, in order.
        events = self._events()
        self.assertEqual([e["kind"] for e in events], [
            C.EVENT_WORKER_REGISTERED,
            C.EVENT_WORKER_REGISTERED,
            C.EVENT_WORKER_DEAD,
            C.EVENT_TASK_REASSIGNED,
            C.EVENT_WORKER_RECOVERED,
        ])
        ts = [e["ts_ms"] for e in events]
        self.assertEqual(ts, sorted(ts))

        dead = events[2]
        self.assertEqual(dead["worker_id"], "w1")
        self.assertEqual(dead["worker_name"], "worker-1")
        self.assertEqual(dead["level"], C.LOG_WARN)
        self.assertEqual(dead["reassigned"], 1)

        moved = events[3]
        self.assertEqual(moved["job_id"], job.job_id)
        self.assertEqual(moved["task_id"], task.task_id)
        self.assertEqual(moved["worker_id"], "w1")

        # The per-job fault view still works as before.
        faults = self.ft.list_faults(job.job_id)
        self.assertEqual(len(faults), 1)
        self.assertEqual(faults[0]["kind"], "worker_dead")

    def test_dead_worker_without_tasks_is_recorded(self):
        register(self.registry, "w1", "worker-1")
        self.registry.get("w1").last_heartbeat_ms = 0
        self.registry.reap()

        events = self._events()
        self.assertEqual([e["kind"] for e in events],
                         [C.EVENT_WORKER_REGISTERED, C.EVENT_WORKER_DEAD])
        self.assertEqual(events[1]["reassigned"], 0)

    def test_rejoin_and_recovery_variants(self):
        register(self.registry, "w1", "worker-1")
        # Alive worker re-registers (e.g. restarted with the same id).
        register(self.registry, "w1", "worker-1")
        # Worker dies, then re-registers instead of just heartbeating.
        self.registry.get("w1").last_heartbeat_ms = 0
        self.registry.reap()
        register(self.registry, "w1", "worker-1")

        events = self._events()
        self.assertEqual([e["kind"] for e in events], [
            C.EVENT_WORKER_REGISTERED,   # first join
            C.EVENT_WORKER_REGISTERED,   # rejoin while alive
            C.EVENT_WORKER_DEAD,         # heartbeat timeout
            C.EVENT_WORKER_RECOVERED,    # re-register after death
        ])
        self.assertFalse(events[0]["rejoin"])
        self.assertTrue(events[1]["rejoin"])

    def test_reassignment_event_only_for_active_tasks(self):
        register(self.registry, "w1", "worker-1")
        job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 2, "num_reduce_tasks": 1, "input_rows": 50, "params": {},
        })
        done, running = self.jm.tasks_for(job.job_id, C.TASK_MAP)[:2]
        self.jm.update_task(job.job_id, done.task_id,
                            status=C.TASK_SUCCEEDED, worker_id="w1")
        self.jm.update_task(job.job_id, running.task_id,
                            status=C.TASK_RUNNING, worker_id="w1")

        self.registry.get("w1").last_heartbeat_ms = 0
        self.registry.reap()

        events = self._events()
        reassign = [e for e in events if e["kind"] == C.EVENT_TASK_REASSIGNED]
        self.assertEqual(len(reassign), 1)
        self.assertEqual(reassign[0]["task_id"], running.task_id)
        dead = [e for e in events if e["kind"] == C.EVENT_WORKER_DEAD][0]
        self.assertEqual(dead["reassigned"], 1)


if __name__ == "__main__":
    unittest.main()
