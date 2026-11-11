"""Injectable clocks. Production uses time.monotonic(); tests use ManualClock."""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class MonotonicClock:
    def now(self) -> float:
        return time.monotonic()


class ManualClock:
    """Deterministic clock for unit tests; only moves when ``advance`` is called."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def advance(self, dt: float) -> None:
        if dt < 0:
            raise ValueError("clock cannot go backwards")
        self._t += dt
