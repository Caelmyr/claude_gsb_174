"""Tests for the job-independent cluster event timeline."""

import shutil
import tempfile
import time
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.eventbus import EventBus
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.registry import WorkerRegistry


class TestClusterEvents(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(heartbeat_timeout_sec=0.001)
        self.logbus = LogBus(self.storage)
        self.eventbus = EventBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.ft = FaultTolerance(
            self.storage, self.jm, self.config, self.logbus, self.eventbus,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_registration_distinguishes_first_and_reregistration(self):
        payload = {
            "worker_id": "w-1", "name": "worker-1", "host": "127.0.0.1",
            "port": 9001, "cpu_cores": 2, "mem_total_mb": 1024,
        }
        worker, was_new = self.registry.register(payload)
        self.assertTrue(was_new)
        self.eventbus.emit(
            C.EVENT_NODE_REGISTERED, "registered", worker_id=worker.worker_id,
        )

        worker, was_new = self.registry.register(payload)
        self.assertFalse(was_new)
        self.eventbus.emit(
            C.EVENT_NODE_REREGISTERED, "reregistered", worker_id=worker.worker_id,
        )

        kinds = [e["kind"] for e in self.eventbus.list_events()]
        self.assertEqual(kinds, [C.EVENT_NODE_REGISTERED, C.EVENT_NODE_REREGISTERED])

    def test_loss_reclaim_and_reassign_events_are_chronological_and_cross_job(self):
        jobs = [
            self.jm.submit({
                "name": f"job-{i}", "mapper": "wordcount_mapper",
                "reducer": "count_reducer", "num_map_tasks": 1,
                "num_reduce_tasks": 1, "input_rows": 10, "params": {},
            })
            for i in range(2)
        ]
        worker, _ = self.registry.register({
            "worker_id": "w-lost", "name": "worker-lost", "host": "127.0.0.1",
            "port": 9001, "cpu_cores": 2, "mem_total_mb": 1024,
        })
        worker.last_heartbeat_ms = int(time.time() * 1000) - 10_000
        self.registry._save(worker)

        lost_events = []
        self.registry.on_lost = lambda w: lost_events.append(
            self.eventbus.emit(C.EVENT_NODE_LOST, "lost", level=C.LOG_WARN, worker_id=w.worker_id)
        )
        self.registry.on_death = self.ft.handle_worker_death

        # Put one active task from each job on the timed-out worker.
        for job in jobs:
            task = self.jm.tasks_for(job.job_id, C.TASK_MAP)[0]
            self.jm.update_task(
                job.job_id, task.task_id, status=C.TASK_RUNNING,
                worker_id=worker.worker_id,
            )

        dead = self.registry.reap()
        self.assertEqual([w.worker_id for w in dead], ["w-lost"])

        events = self.eventbus.list_events()
        kinds = [e["kind"] for e in events]
        self.assertEqual(kinds[0], C.EVENT_NODE_LOST)
        self.assertEqual(kinds[1], C.EVENT_NODE_RECLAIMED)
        self.assertEqual(kinds[2:], [C.EVENT_TASK_REASSIGNED, C.EVENT_TASK_REASSIGNED])

        affected_jobs = {e["job_id"] for e in events if e["kind"] == C.EVENT_TASK_REASSIGNED}
        self.assertEqual(affected_jobs, {j.job_id for j in jobs})

        created = [e["created_ms"] for e in events]
        self.assertEqual(created, sorted(created))
        for event in events:
            self.assertEqual(event["worker_id"], "w-lost")

    def test_cluster_event_stream_is_not_stored_under_a_job(self):
        self.eventbus.emit(C.EVENT_NODE_REGISTERED, "cluster only")
        self.assertEqual(self.storage.files("cluster", suffix=".jsonl"),
                         [self.storage.path("cluster", "events.jsonl")])
        self.assertEqual(self.storage.subdirs("jobs"), [])


if __name__ == "__main__":
    unittest.main()
