"""Thread-safe single-entry fingerprint cache.

Replaces the duplicate ``_TraceCache`` and ``_PreviewCache`` classes in
``trace.py`` and ``executor.py`` with a single generic implementation.

Both caches follow the same pattern:

1. A *fingerprint* (hash of graph structure) determines cache validity.
2. On hit, the cached data dicts are returned without re-execution.
3. On miss, the caller executes and stores new data.
4. ``invalidate()`` clears everything.

Usage::

    cache = FingerprintCache(
        slots=("eager_outputs", "order", "errors"),
    )

    # Write
    cache.store(fp, eager_outputs={...}, order=[...], errors={})

    # Read
    data = cache.try_get(fp)       # returns dict of slots or None
    data["eager_outputs"]          # typed by caller

    # Invalidate
    cache.invalidate()
"""

from __future__ import annotations

import threading
from typing import Any


class FingerprintCache:
    """Thread-safe single-entry cache keyed by a fingerprint string.

    Parameters
    ----------
    slots:
        Names of the data fields this cache stores.  Each slot is
        initialised to an empty dict.  ``store()`` accepts keyword
        arguments matching these names.
    """

    __slots__ = ("_slots", "fingerprint", "_data", "_lock")

    def __init__(self, slots: tuple[str, ...]) -> None:
        if not slots:
            raise ValueError("At least one slot name is required")
        self._slots = slots
        self.fingerprint: str | None = None
        self._data: dict[str, Any] = {name: {} for name in slots}
        self._lock = threading.Lock()

    # -- public API --------------------------------------------------------

    def try_get(self, fingerprint: str) -> dict[str, Any] | None:
        """Return a *shallow copy* of all slot data if *fingerprint* matches.

        Returns ``None`` on miss (fingerprint mismatch or empty cache).
        The first slot is used as the "non-empty" check -- if it's falsy
        (e.g. empty dict/list), the cache is treated as empty.
        """
        with self._lock:
            first_slot = self._slots[0]
            if fingerprint == self.fingerprint and self._data[first_slot]:
                return {name: self._data[name] for name in self._slots}
            return None

    def store(self, fingerprint: str, **slot_data: Any) -> None:
        """Replace all cached data with new values.

        Every key in *slot_data* must be a declared slot name.
        Any declared slot not provided is reset to an empty dict.
        """
        unknown = set(slot_data) - set(self._slots)
        if unknown:
            raise ValueError(
                f"Unknown slot(s): {sorted(unknown)}. "
                f"Declared slots: {sorted(self._slots)}"
            )
        with self._lock:
            self.fingerprint = fingerprint
            for name in self._slots:
                self._data[name] = slot_data.get(name, {})

    def update_slot(self, slot: str, value: Any) -> None:
        """Replace a single slot's value while keeping the rest intact.

        Useful for the preview cache's "extend" path where only some
        slots are merged.
        """
        if slot not in self._slots:
            raise ValueError(
                f"Unknown slot: {slot!r}. "
                f"Declared slots: {sorted(self._slots)}"
            )
        with self._lock:
            self._data[slot] = value

    def invalidate(self) -> None:
        """Clear the fingerprint and all slot data."""
        with self._lock:
            self.fingerprint = None
            for name in self._slots:
                val = self._data[name]
                # If it has a .clear() method (dict, list), use it to
                # release references eagerly.  Otherwise just replace.
                if hasattr(val, "clear"):
                    val.clear()
                else:
                    self._data[name] = {}

    @property
    def lock(self) -> threading.Lock:
        """Expose the lock for callers that need atomic read-modify-write."""
        return self._lock

    def __repr__(self) -> str:
        sizes = {
            name: len(v) if hasattr(v, "__len__") else "?"
            for name, v in self._data.items()
        }
        return (
            f"FingerprintCache(fingerprint={self.fingerprint!r}, "
            f"slots={sizes})"
        )
