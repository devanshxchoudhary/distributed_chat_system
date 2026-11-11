"""Length-prefixed JSON framing: 4-byte big-endian length, then UTF-8 JSON object.

Decisions that matter for the protocol:
* ``readexactly`` handles partial reads and several frames per TCP chunk.
* A malformed body (bad UTF-8 / bad JSON / not an object) is fully consumed before
  we raise, so the stream stays in sync and the caller may keep reading.
* An oversize length cannot be skipped safely, so the caller must close the link.
"""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

from .constants import FRAME_HEADER_BYTES, MAX_FRAME_BYTES

_HEADER = struct.Struct(">I")


class FrameError(Exception):
    """Base class for framing problems."""


class OversizeFrame(FrameError):
    """Frame length exceeds the configured maximum; the stream is unusable."""


class MalformedFrame(FrameError):
    """Frame body was read completely but is not a JSON object; stream still in sync."""


def encode_frame(payload: dict[str, Any], max_size: int = MAX_FRAME_BYTES) -> bytes:
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(body) > max_size:
        raise OversizeFrame(f"frame of {len(body)} bytes exceeds limit {max_size}")
    return _HEADER.pack(len(body)) + body


async def read_frame(
    reader: asyncio.StreamReader, max_size: int = MAX_FRAME_BYTES
) -> dict[str, Any]:
    """Read one frame. Raises ``asyncio.IncompleteReadError`` on EOF/peer disconnect."""
    header = await reader.readexactly(FRAME_HEADER_BYTES)
    (length,) = _HEADER.unpack(header)
    if length > max_size:
        raise OversizeFrame(f"peer announced {length} bytes, limit is {max_size}")
    body = await reader.readexactly(length)
    try:
        obj = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedFrame(f"invalid frame body: {exc}") from exc
    if not isinstance(obj, dict):
        raise MalformedFrame("frame body is not a JSON object")
    return obj
