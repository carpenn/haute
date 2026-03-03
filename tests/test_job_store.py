"""Tests for the shared in-memory JobStore.

Covers basic CRUD operations, TTL eviction, and concurrent access patterns
to verify dict-backed mutation doesn't lose data under threading.
"""

from __future__ import annotations

import threading
import time

import pytest

from haute.routes._job_store import JobStore

# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------


class TestJobStoreCRUD:
    """Unit tests for create, read, update, and list operations."""

    def test_create_job_returns_id(self) -> None:
        store = JobStore()
        job_id = store.create_job({"status": "pending"})
        assert isinstance(job_id, str)
        assert len(job_id) >= 8
        assert job_id.isalnum()

    def test_get_job_returns_stored_data(self) -> None:
        store = JobStore()
        job_id = store.create_job({"status": "running", "model": "glm"})
        result = store.get_job(job_id)
        assert result is not None
        assert result["status"] == "running"
        assert result["model"] == "glm"
        assert "created_at" in result

    def test_get_job_returns_none_for_unknown_id(self) -> None:
        store = JobStore()
        assert store.get_job("nonexistent") is None

    def test_update_job_merges_fields(self) -> None:
        store = JobStore()
        job_id = store.create_job({"status": "pending", "progress": 0})
        store.update_job(job_id, status="running", progress=50)
        result = store.get_job(job_id)
        assert result is not None
        assert result["status"] == "running"
        assert result["progress"] == 50

    def test_update_job_raises_for_unknown_id(self) -> None:
        store = JobStore()
        with pytest.raises(KeyError):
            store.update_job("nonexistent", status="done")

    def test_list_jobs_via_property(self) -> None:
        store = JobStore()
        id1 = store.create_job({"status": "a"})
        id2 = store.create_job({"status": "b"})
        assert id1 in store.jobs
        assert id2 in store.jobs
        assert len(store.jobs) == 2

    def test_create_job_sets_created_at_if_missing(self) -> None:
        store = JobStore()
        job_id = store.create_job({"status": "new"})
        result = store.get_job(job_id)
        assert result is not None
        assert "created_at" in result
        assert isinstance(result["created_at"], float)

    def test_create_job_preserves_explicit_created_at(self) -> None:
        store = JobStore()
        ts = time.time()  # must be recent enough to survive TTL eviction
        job_id = store.create_job({"status": "new", "created_at": ts})
        result = store.get_job(job_id)
        assert result is not None
        assert result["created_at"] == ts

    def test_unique_ids_across_many_jobs(self) -> None:
        store = JobStore()
        ids = {store.create_job({"status": "pending"}) for _ in range(100)}
        assert len(ids) == 100  # all unique


# ---------------------------------------------------------------------------
# TTL eviction
# ---------------------------------------------------------------------------


class TestJobStoreTTL:
    """Tests for time-based eviction."""

    def test_stale_jobs_are_evicted_on_create(self) -> None:
        store = JobStore(ttl_seconds=1)
        old_id = store.create_job({"status": "old", "created_at": time.time() - 10})
        # Creating a new job triggers eviction
        _new_id = store.create_job({"status": "new"})
        assert store.get_job(old_id) is None

    def test_stale_jobs_are_evicted_on_get(self) -> None:
        store = JobStore(ttl_seconds=1)
        old_id = store.create_job({"status": "old", "created_at": time.time() - 10})
        # get_job triggers eviction
        assert store.get_job(old_id) is None

    def test_fresh_jobs_survive_eviction(self) -> None:
        store = JobStore(ttl_seconds=60)
        job_id = store.create_job({"status": "fresh"})
        _trigger = store.create_job({"status": "trigger"})
        assert store.get_job(job_id) is not None

    def test_mixed_stale_and_fresh(self) -> None:
        store = JobStore(ttl_seconds=5)
        stale_id = store.create_job({"status": "stale", "created_at": time.time() - 100})
        fresh_id = store.create_job({"status": "fresh"})
        # Trigger eviction via a new create
        store.create_job({"status": "trigger"})
        assert store.get_job(stale_id) is None
        assert store.get_job(fresh_id) is not None


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestJobStoreConcurrency:
    """Concurrency tests using threading to verify dict mutation safety."""

    def test_concurrent_creates_all_tracked(self) -> None:
        """Submit 10 jobs simultaneously and verify all are tracked."""
        store = JobStore()
        job_ids: list[str] = []
        lock = threading.Lock()
        barrier = threading.Barrier(10)

        def create_one(idx: int) -> None:
            barrier.wait()  # all threads start together
            jid = store.create_job({"status": "pending", "index": idx})
            with lock:
                job_ids.append(jid)

        threads = [threading.Thread(target=create_one, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert len(job_ids) == 10
        assert len(set(job_ids)) == 10  # all unique

        # Every job should be retrievable
        for jid in job_ids:
            result = store.get_job(jid)
            assert result is not None
            assert result["status"] == "pending"

    def test_concurrent_updates_no_data_loss(self) -> None:
        """Concurrent status updates to different jobs don't cause data loss."""
        store = JobStore()
        n_jobs = 10
        ids = [store.create_job({"status": "pending", "counter": 0}) for _ in range(n_jobs)]
        barrier = threading.Barrier(n_jobs)

        def update_one(job_id: str, value: int) -> None:
            barrier.wait()
            store.update_job(job_id, status="done", counter=value)

        threads = [
            threading.Thread(target=update_one, args=(ids[i], i + 1))
            for i in range(n_jobs)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # All jobs should have been updated
        for i, jid in enumerate(ids):
            result = store.get_job(jid)
            assert result is not None
            assert result["status"] == "done"
            assert result["counter"] == i + 1

    def test_concurrent_updates_to_same_job(self) -> None:
        """Multiple threads updating the same job's fields concurrently.

        Each thread increments a different field, so no updates should be lost.
        """
        store = JobStore()
        job_id = store.create_job({"status": "running"})
        n_threads = 10
        barrier = threading.Barrier(n_threads)

        def update_field(idx: int) -> None:
            barrier.wait()
            store.update_job(job_id, **{f"field_{idx}": idx})

        threads = [
            threading.Thread(target=update_field, args=(i,))
            for i in range(n_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        result = store.get_job(job_id)
        assert result is not None
        for i in range(n_threads):
            assert result[f"field_{i}"] == i

    def test_concurrent_create_and_read(self) -> None:
        """Interleave creates and reads without errors."""
        store = JobStore()
        n_ops = 20
        created_ids: list[str] = []
        read_results: list[bool] = []
        lock = threading.Lock()
        barrier = threading.Barrier(n_ops)

        def create_and_read(idx: int) -> None:
            barrier.wait()
            if idx % 2 == 0:
                jid = store.create_job({"status": "new", "idx": idx})
                with lock:
                    created_ids.append(jid)
            else:
                # Read a job that may or may not exist yet
                with lock:
                    target = created_ids[-1] if created_ids else "nonexistent"
                result = store.get_job(target)
                with lock:
                    read_results.append(result is not None or target == "nonexistent")

        threads = [
            threading.Thread(target=create_and_read, args=(i,))
            for i in range(n_ops)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # No exceptions raised, all created jobs are accessible after threads finish
        for jid in created_ids:
            assert store.get_job(jid) is not None

    def test_concurrent_creates_with_eviction(self) -> None:
        """Concurrent creates with a short TTL trigger eviction under contention."""
        store = JobStore(ttl_seconds=1)
        # Pre-populate with stale jobs that will be evicted
        for _ in range(5):
            store.create_job({"status": "stale", "created_at": time.time() - 10})

        n_threads = 10
        new_ids: list[str] = []
        lock = threading.Lock()
        barrier = threading.Barrier(n_threads)

        def create_with_eviction(idx: int) -> None:
            barrier.wait()
            jid = store.create_job({"status": "fresh", "idx": idx})
            with lock:
                new_ids.append(jid)

        threads = [
            threading.Thread(target=create_with_eviction, args=(i,))
            for i in range(n_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        # All new jobs should be present; stale ones evicted
        assert len(new_ids) == n_threads
        for jid in new_ids:
            result = store.get_job(jid)
            assert result is not None
            assert result["status"] == "fresh"
