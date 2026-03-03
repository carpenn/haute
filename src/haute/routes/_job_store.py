"""Shared in-memory job store for background tasks (training, optimisation).

Fine for a single-server dev tool.  Jobs older than ``ttl_seconds`` are
evicted on each ``create_job`` / ``get_job`` call to bound memory usage.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

_DEFAULT_TTL_SECONDS = 24 * 60 * 60  # 24 hours


class JobStore:
    """Thread-safe-enough dict-backed job store with TTL eviction.

    Each route module creates its own instance so job-ID namespaces stay
    independent (a training job ID will never collide with an optimiser
    job ID, just as before the refactor).
    """

    def __init__(self, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> None:
        self._jobs: dict[str, dict[str, Any]] = {}
        self._ttl_seconds = ttl_seconds

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict_stale(self) -> None:
        """Remove jobs older than TTL to bound memory usage."""
        cutoff = time.time() - self._ttl_seconds
        stale = [
            jid for jid, j in self._jobs.items()
            if j.get("created_at", 0) < cutoff
        ]
        for jid in stale:
            del self._jobs[jid]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_job(self, initial_status: dict[str, Any]) -> str:
        """Generate a UUID, store *initial_status* with a timestamp, return the ID.

        Automatically evicts stale jobs before inserting.
        """
        self._evict_stale()
        job_id = uuid.uuid4().hex[:12]
        initial_status.setdefault("created_at", time.time())
        self._jobs[job_id] = initial_status
        return job_id

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        """Return the job dict for *job_id*, or ``None`` if not found.

        Evicts stale jobs first so callers never see expired entries.
        """
        self._evict_stale()
        return self._jobs.get(job_id)

    def update_job(self, job_id: str, **fields: Any) -> None:
        """Merge *fields* into the stored job dict.

        Raises ``KeyError`` if *job_id* does not exist.
        """
        self._jobs[job_id].update(fields)

    @property
    def jobs(self) -> dict[str, dict[str, Any]]:
        """Direct access to the underlying dict.

        Provided for callsites that need to iterate (e.g. checking for
        running jobs).  Prefer ``get_job`` / ``update_job`` for single-key
        access.
        """
        return self._jobs
