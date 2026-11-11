"""Structured logging: every line carries node id, role and term (also demo evidence)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any


class StateLogger(logging.LoggerAdapter[logging.Logger]):
    def __init__(self, node_id: str, state_fn: Callable[[], tuple[str, int]]) -> None:
        super().__init__(logging.getLogger(f"dchat.{node_id}"), {})
        self._node_id = node_id
        self._state_fn = state_fn

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        role, term = self._state_fn()
        return f"[{self._node_id} role={role} term={term}] {msg}", kwargs


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
