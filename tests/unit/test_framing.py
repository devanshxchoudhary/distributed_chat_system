import asyncio
import random

from pytest import raises

from dchat.protocol.constants import MAX_FRAME_BYTES
from dchat.protocol.framing import MalformedFrame, OversizeFrame, encode_frame, read_frame


def _reader(*chunks: bytes, eof: bool = True) -> asyncio.StreamReader:
    r = asyncio.StreamReader()
    for c in chunks:
        r.feed_data(c)
    if eof:
        r.feed_eof()
    return r


async def test_roundtrip_unicode() -> None:
    payload = {"type": "X", "text": "héllo ✓ 世界", "n": 5}
    assert await read_frame(_reader(encode_frame(payload))) == payload


async def test_partial_reads_byte_by_byte() -> None:
    data = encode_frame({"a": 1, "b": "two"})
    r = asyncio.StreamReader()
    task = asyncio.create_task(read_frame(r))
    for b in data:
        r.feed_data(bytes([b]))
        await asyncio.sleep(0)
    assert await task == {"a": 1, "b": "two"}


async def test_multiple_frames_in_one_chunk() -> None:
    blob = b"".join(encode_frame({"i": i}) for i in range(5))
    r = _reader(blob)
    assert [(await read_frame(r))["i"] for _ in range(5)] == [0, 1, 2, 3, 4]


async def test_oversize_rejected_on_read_and_encode() -> None:
    r = _reader((MAX_FRAME_BYTES + 1).to_bytes(4, "big"))
    with raises(OversizeFrame):
        await read_frame(r)
    with raises(OversizeFrame):
        encode_frame({"x": "y" * (MAX_FRAME_BYTES + 10)})


async def test_malformed_body_keeps_stream_in_sync() -> None:
    bad = b"{not json"
    good = encode_frame({"ok": True})
    r = _reader(len(bad).to_bytes(4, "big") + bad + good)
    with raises(MalformedFrame):
        await read_frame(r)
    assert await read_frame(r) == {"ok": True}


async def test_non_object_and_bad_utf8_are_malformed() -> None:
    for body in (b"[1,2,3]", b"\xff\xfe\xfa"):
        with raises(MalformedFrame):
            await read_frame(_reader(len(body).to_bytes(4, "big") + body))


async def test_truncated_frame_raises_incomplete() -> None:
    data = encode_frame({"a": "b" * 50})[:-5]
    with raises(asyncio.IncompleteReadError):
        await read_frame(_reader(data))


async def test_fuzz_never_crashes_unexpectedly() -> None:
    rng = random.Random(7)
    for _ in range(300):
        blob = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 60)))
        r = _reader(blob)
        try:
            while True:
                await read_frame(r, max_size=1024)
        except (MalformedFrame, OversizeFrame, asyncio.IncompleteReadError):
            pass
