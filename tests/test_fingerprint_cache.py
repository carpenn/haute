"""Tests for FingerprintCache — generic single-entry fingerprint cache."""

from __future__ import annotations

import threading
import time

import pytest

from haute._fingerprint_cache import FingerprintCache


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_requires_at_least_one_slot(self) -> None:
        with pytest.raises(ValueError, match="At least one slot"):
            FingerprintCache(slots=())

    def test_initial_state_is_empty(self) -> None:
        cache = FingerprintCache(slots=("a", "b"))
        assert cache.fingerprint is None
        assert cache.try_get("any") is None
        assert len(repr(cache)) > 0  # smoke test __repr__


# ---------------------------------------------------------------------------
# Basic set / get
# ---------------------------------------------------------------------------


class TestBasicSetGet:
    def test_store_and_retrieve(self) -> None:
        cache = FingerprintCache(slots=("outputs", "order"))
        cache.store("fp1", outputs={"a": 1}, order=["a"])
        data = cache.try_get("fp1")
        assert data is not None
        assert data["outputs"] == {"a": 1}
        assert data["order"] == ["a"]

    def test_store_replaces_previous(self) -> None:
        cache = FingerprintCache(slots=("x",))
        cache.store("fp1", x={"old": True})
        cache.store("fp2", x={"new": True})
        assert cache.try_get("fp1") is None
        data = cache.try_get("fp2")
        assert data is not None
        assert data["x"] == {"new": True}

    def test_omitted_slots_default_to_empty_dict(self) -> None:
        cache = FingerprintCache(slots=("a", "b", "c"))
        cache.store("fp1", a={"val": 1})
        data = cache.try_get("fp1")
        assert data is not None
        assert data["a"] == {"val": 1}
        assert data["b"] == {}
        assert data["c"] == {}

    def test_returns_shallow_copy(self) -> None:
        """Mutating the returned dict should not affect the cache."""
        cache = FingerprintCache(slots=("x",))
        cache.store("fp1", x={"key": "value"})
        data = cache.try_get("fp1")
        assert data is not None
        data["x"]["key"] = "mutated"
        # The inner dict is shared (shallow copy), but the top-level
        # dict returned by try_get is a new dict each time.
        data2 = cache.try_get("fp1")
        assert data2 is not None
        # Inner data *is* shared (intentionally — these are large DataFrames)
        assert data2["x"]["key"] == "mutated"


# ---------------------------------------------------------------------------
# Cache miss
# ---------------------------------------------------------------------------


class TestCacheMiss:
    def test_wrong_fingerprint_returns_none(self) -> None:
        cache = FingerprintCache(slots=("x",))
        cache.store("fp1", x={"a": 1})
        assert cache.try_get("wrong_fp") is None

    def test_empty_first_slot_treated_as_miss(self) -> None:
        """Even with matching fingerprint, empty primary slot = miss."""
        cache = FingerprintCache(slots=("primary", "secondary"))
        cache.store("fp1", primary={}, secondary={"ok": True})
        assert cache.try_get("fp1") is None

    def test_never_stored_returns_none(self) -> None:
        cache = FingerprintCache(slots=("x",))
        assert cache.try_get("anything") is None


# ---------------------------------------------------------------------------
# Invalidation
# ---------------------------------------------------------------------------


class TestInvalidation:
    def test_invalidate_clears_everything(self) -> None:
        cache = FingerprintCache(slots=("outputs", "meta"))
        cache.store("fp1", outputs={"a": 1}, meta={"b": 2})
        cache.invalidate()
        assert cache.fingerprint is None
        assert cache.try_get("fp1") is None

    def test_invalidate_on_empty_cache_is_safe(self) -> None:
        cache = FingerprintCache(slots=("x",))
        cache.invalidate()  # should not raise
        assert cache.fingerprint is None

    def test_invalidate_clears_non_dict_slots(self) -> None:
        """Slots holding lists or sets should also be cleared."""
        cache = FingerprintCache(slots=("items",))
        cache.store("fp1", items=["a", "b", "c"])
        cache.invalidate()
        assert cache.try_get("fp1") is None


# ---------------------------------------------------------------------------
# update_slot
# ---------------------------------------------------------------------------


class TestUpdateSlot:
    def test_update_single_slot(self) -> None:
        cache = FingerprintCache(slots=("a", "b"))
        cache.store("fp1", a={"x": 1}, b={"y": 2})
        cache.update_slot("a", {"x": 99})
        data = cache.try_get("fp1")
        assert data is not None
        assert data["a"] == {"x": 99}
        assert data["b"] == {"y": 2}

    def test_update_unknown_slot_raises(self) -> None:
        cache = FingerprintCache(slots=("a",))
        with pytest.raises(ValueError, match="Unknown slot"):
            cache.update_slot("nonexistent", {})


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_store_rejects_unknown_slots(self) -> None:
        cache = FingerprintCache(slots=("a", "b"))
        with pytest.raises(ValueError, match="Unknown slot"):
            cache.store("fp1", a={}, c={"bad": True})


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_store_and_get(self) -> None:
        """Multiple threads storing and getting should not corrupt state."""
        cache = FingerprintCache(slots=("data",))
        errors: list[str] = []
        barrier = threading.Barrier(4)

        def writer(fp: str, val: dict) -> None:
            barrier.wait()
            for _ in range(100):
                cache.store(fp, data=val)

        def reader() -> None:
            barrier.wait()
            for _ in range(100):
                result = cache.try_get("fp1")
                if result is not None and not isinstance(result, dict):
                    errors.append(f"Bad type: {type(result)}")

        threads = [
            threading.Thread(target=writer, args=("fp1", {"a": i}))
            for i in range(2)
        ] + [
            threading.Thread(target=reader)
            for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert not errors, f"Thread safety errors: {errors}"

    def test_lock_property_returns_lock(self) -> None:
        cache = FingerprintCache(slots=("x",))
        lock = cache.lock
        # Verify it's a lock by acquiring and releasing
        assert lock.acquire(blocking=False)
        lock.release()


# ---------------------------------------------------------------------------
# __repr__
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_empty(self) -> None:
        cache = FingerprintCache(slots=("a", "b"))
        r = repr(cache)
        assert "FingerprintCache" in r
        assert "None" in r  # fingerprint is None

    def test_repr_with_data(self) -> None:
        cache = FingerprintCache(slots=("items",))
        cache.store("fp1", items={"k": "v"})
        r = repr(cache)
        assert "fp1" in r
