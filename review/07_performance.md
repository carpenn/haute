# Performance Issues

---

## P1: N+1 subprocess calls in git `list_branches`

**File:** `src/haute/_git.py:414-420`
**Severity:** MEDIUM
**Agent:** Secondary Routes (9)

For every branch returned by `git for-each-ref`, a separate `git rev-list --count` subprocess is spawned. With 50 branches = 50 subprocess spawns. Similarly, `get_history` (line 543) spawns a `diff-tree` per commit.

**Fix:** Use `git for-each-ref` with `%(ahead-behind:default)` format (Git 2.36+) for single-call counts. For history, use `git log --name-only --format=...` instead of per-commit `diff-tree`.

---

## P2: `get_status` does a `git fetch` on every call

**File:** `src/haute/_git.py:334`
**Severity:** MEDIUM
**Agent:** Secondary Routes (9)

Every `GET /api/git/status` triggers `git fetch origin <default> --quiet`. If the frontend polls frequently, this creates constant network traffic.

**Fix:** Cache with 30-60s TTL, or move to a background periodic task.

---

## P3: `_get_default_branch` called 7 times with subprocess each time

**File:** `src/haute/_git.py:160-174`
**Severity:** LOW-MEDIUM
**Agent:** Secondary Routes (9)

Spawns up to 3 subprocesses per call, called from 7 different operations. Value doesn't change within a single request. Similarly, `_has_remote()` is called 10 times.

**Fix:** Cache per-request via a `GitContext` object, or module-level cache with short TTL.

---

## P4: `_apply_rating_table` calls `collect_schema()` per factor

**File:** `src/haute/_rating.py:143`
**Severity:** LOW
**Agent:** Rating (15)

`collect_schema()` called inside a list comprehension iterating over every factor. For 3-factor tables, called 3 times.

**Fix:** Call once before the comprehension, store as a set: `existing_cols = set(lf.collect_schema().names())`.

---

## P5: Optimiser solve results held in memory for 24h TTL

**File:** `src/haute/routes/_optimiser_service.py:139-148`
**Severity:** LOW-MEDIUM
**Agent:** Optimisation (16)

Completed job dict holds the full solver object, solve result (entire scored DataFrame), and QuoteGrid. For large datasets, significant memory retention.

**Fix:** Persist results to temp parquet and free in-memory objects after save/MLflow-log, or shorten TTL for completed jobs.

---

## P6: `_json_flatten.py` read_json_flat uses older/slower flatten strategy

**File:** `src/haute/_json_flatten.py`
**Severity:** LOW-MEDIUM
**Agent:** Data Sources (14)

`read_json_flat()` (called by codegen at runtime) uses `_flatten_and_write_streaming` directly, while `build_json_cache()` (called by the UI button) uses the superior two-step pipeline. Users running pipeline from code get slower flattening.

**Fix:** Unify both paths to use the two-step pipeline for JSONL files.

---

## P7: Thread safety concern — solver mutates job dict while status reads it

**File:** `src/haute/routes/_optimiser_service.py:539-576`
**Severity:** LOW-MEDIUM
**Agent:** Optimisation (16)

Background thread mutates job dict via `job.update({...})` while status endpoint reads it. No locking. A status poll could see partially-updated state.

**Fix:** Assign a new dict atomically: `self._store.jobs[job_id] = {**job, **updates}`.
