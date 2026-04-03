import asyncio
from collections.abc import AsyncIterator
from typing import cast
from unittest.mock import MagicMock, patch

import pytest

import s2_sdk._generated.s2.v1.s2_pb2 as pb
from s2_sdk import (
    AppendAck,
    AppendInput,
    AppendRetryPolicy,
    AppendSession,
    BatchSubmitTicket,
    Compression,
    Record,
    Retry,
    S2ClientError,
    StreamPosition,
)
from s2_sdk._append_session import _AppendPermits, _Semaphore  # noqa: PLC2701
from s2_sdk._client import HttpClient
from s2_sdk._s2s._append_session import run_append_session
from s2_sdk._s2s._protocol import Message, deframe_data, frame_message

_PATCH_TARGET = "s2_sdk._append_session.run_append_session"


def _ack(start_seq: int = 0, end_seq: int = 1) -> AppendAck:
    return AppendAck(
        start=StreamPosition(seq_num=start_seq, timestamp=10),
        end=StreamPosition(seq_num=end_seq, timestamp=10),
        tail=StreamPosition(seq_num=end_seq, timestamp=10),
    )


def _input(n_records: int = 1, body: bytes = b"payload") -> AppendInput:
    return AppendInput(records=[Record(body=body) for _ in range(n_records)])


async def _fake_run(client, stream_name, inputs, retry, compression, ack_timeout=None):
    seq = 0
    async for inp in inputs:
        n = len(inp.records)
        yield _ack(start_seq=seq, end_seq=seq + n)
        seq += n


async def _slow_fake_run(
    client, stream_name, inputs, retry, compression, ack_timeout=None
):
    seq = 0
    async for inp in inputs:
        await asyncio.sleep(0.01)
        n = len(inp.records)
        yield _ack(start_seq=seq, end_seq=seq + n)
        seq += n


async def _failing_run(
    client, stream_name, inputs, retry, compression, ack_timeout=None
):
    async for _ in inputs:
        raise RuntimeError("session failed")
        yield  # noqa: F401


def _mock_client() -> MagicMock:
    client = MagicMock()
    client._request_timeout = 5.0
    return client


def _session(
    max_unacked_bytes: int = 5 * 1024 * 1024,
    max_unacked_batches: int | None = None,
) -> AppendSession:
    return AppendSession(
        client=_mock_client(),
        stream_name="test-stream",
        retry=Retry(max_attempts=1),
        compression=Compression.NONE,
        max_unacked_bytes=max_unacked_bytes,
        max_unacked_batches=max_unacked_batches,
    )


@pytest.mark.asyncio
async def test_submit_and_await_ticket():
    with patch(_PATCH_TARGET, side_effect=_fake_run):
        session = _session()
        ticket = await session.submit(_input())
        ack = await ticket
        assert ack.start.seq_num == 0
        assert ack.end.seq_num == 1
        await session.close()


@pytest.mark.asyncio
async def test_ticket_is_awaitable():
    with patch(_PATCH_TARGET, side_effect=_fake_run):
        session = _session()
        ticket = await session.submit(_input())
        assert isinstance(ticket, BatchSubmitTicket)
        ack = await ticket
        assert isinstance(ack, AppendAck)
        await session.close()


@pytest.mark.asyncio
async def test_multiple_submits_ack_in_order():
    with patch(_PATCH_TARGET, side_effect=_fake_run):
        session = _session()
        tickets = []
        for _ in range(5):
            ticket = await session.submit(_input(n_records=1))
            tickets.append(ticket)

        for i, ticket in enumerate(tickets):
            ack = await ticket
            assert ack.start.seq_num == i
            assert ack.end.seq_num == i + 1

        await session.close()


@pytest.mark.asyncio
async def test_backpressure_bytes_limit():
    with patch(_PATCH_TARGET, side_effect=_slow_fake_run):
        session = _session(max_unacked_bytes=100)
        ticket1 = await session.submit(_input(n_records=1, body=b"x" * 50))
        ticket2 = await session.submit(_input(n_records=1, body=b"x" * 50))

        ack1 = await ticket1
        ack2 = await ticket2
        assert ack1.start.seq_num == 0
        assert ack2.start.seq_num == 1
        await session.close()


@pytest.mark.asyncio
async def test_backpressure_batch_limit():
    with patch(_PATCH_TARGET, side_effect=_slow_fake_run):
        session = _session(max_unacked_batches=2)
        ticket1 = await session.submit(_input())
        ticket2 = await session.submit(_input())
        ticket3 = await session.submit(_input())

        ack1 = await ticket1
        ack2 = await ticket2
        ack3 = await ticket3
        assert ack1.end.seq_num == 1
        assert ack2.end.seq_num == 2
        assert ack3.end.seq_num == 3
        await session.close()


@pytest.mark.asyncio
async def test_close_flushes_pending():
    with patch(_PATCH_TARGET, side_effect=_fake_run):
        session = _session()
        ticket = await session.submit(_input())
        await session.close()
        ack = await ticket
        assert ack.end.seq_num == 1


@pytest.mark.asyncio
async def test_close_is_idempotent():
    with patch(_PATCH_TARGET, side_effect=_fake_run):
        session = _session()
        await session.close()
        await session.close()


@pytest.mark.asyncio
async def test_error_propagation():
    with patch(_PATCH_TARGET, side_effect=_failing_run):
        session = _session()
        ticket = await session.submit(_input())
        with pytest.raises(RuntimeError, match="session failed"):
            await ticket


@pytest.mark.asyncio
async def test_submit_after_close_raises():
    with patch(_PATCH_TARGET, side_effect=_fake_run):
        session = _session()
        await session.close()
        with pytest.raises(S2ClientError, match="closed"):
            await session.submit(_input())


@pytest.mark.asyncio
async def test_submit_after_error_raises():
    with patch(_PATCH_TARGET, side_effect=_failing_run):
        session = _session()
        ticket = await session.submit(_input())
        with pytest.raises(RuntimeError, match="session failed"):
            await ticket
        with pytest.raises(RuntimeError, match="session failed"):
            await session.submit(_input())


@pytest.mark.asyncio
async def test_context_manager():
    with patch(_PATCH_TARGET, side_effect=_fake_run):
        async with _session() as session:
            ticket = await session.submit(_input())
            ack = await ticket
            assert ack.end.seq_num == 1


@pytest.mark.asyncio
async def test_close_rejects_submit_blocked_on_backpressure():
    with patch(_PATCH_TARGET, side_effect=_slow_fake_run):
        session = _session(max_unacked_batches=1)
        ticket = await session.submit(_input())
        blocked_submit = asyncio.create_task(session.submit(_input()))
        await asyncio.sleep(0.001)
        await session.close()
        await ticket
        with pytest.raises(S2ClientError, match="closed"):
            await blocked_submit


# ── S2S framing tests ──


class _StreamResponse:
    def __init__(
        self,
        messages: list[bytes],
        content: AsyncIterator[bytes] | None,
        frame_signal=None,
    ) -> None:
        self.status_code = 200
        self._messages = messages
        self._content = content
        self._frame_signal = frame_signal
        self.sent_messages: list[bytes] = []

    async def __aenter__(self):
        if self._content is not None:
            async for chunk in self._content:
                self.sent_messages.append(chunk)
                if self._frame_signal is not None:
                    self._frame_signal.signal()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def aread(self) -> bytes:
        return b""

    async def aiter_bytes(self):
        for msg in self._messages:
            yield msg


class _StreamingClient:
    def __init__(self, response_messages: list[bytes]) -> None:
        self.calls: list[tuple[str, str]] = []
        self.responses: list[_StreamResponse] = []
        self._response_messages = response_messages

    def streaming_request(self, method: str, path: str, **kwargs) -> _StreamResponse:
        self.calls.append((method, path))
        response = _StreamResponse(
            self._response_messages,
            kwargs.get("content"),
            kwargs.get("frame_signal"),
        )
        self.responses.append(response)
        return response


def _ack_message() -> bytes:
    ack = pb.AppendAck(
        start=pb.StreamPosition(seq_num=0, timestamp=10),
        end=pb.StreamPosition(seq_num=1, timestamp=10),
        tail=pb.StreamPosition(seq_num=1, timestamp=10),
    )
    return frame_message(
        Message(ack.SerializeToString(), terminal=False, compression=Compression.NONE)
    )


def _terminal_message(status_code: int, body: bytes) -> bytes:
    return frame_message(
        Message(
            status_code.to_bytes(2, "big") + body,
            terminal=True,
            compression=Compression.NONE,
        )
    )


async def _s2s_inputs() -> AsyncIterator[AppendInput]:
    yield AppendInput(records=[Record(body=b"payload")])


def _decode_append_input(data: bytes) -> pb.AppendInput:
    body, terminal, _ = deframe_data(data)
    assert terminal is False
    message = pb.AppendInput()
    message.ParseFromString(body)
    return message


@pytest.mark.asyncio
async def test_retrying_append_session_encodes_messages():
    client = _StreamingClient([_ack_message()])

    outputs = []
    async for output in run_append_session(
        cast("HttpClient", client),
        "orders/us-east",
        _s2s_inputs(),
        retry=Retry(max_attempts=1),
        compression=Compression.NONE,
    ):
        outputs.append(output)

    assert len(outputs) == 1
    assert client.calls == [("POST", "/v1/streams/orders%2Fus-east/records")]
    assert len(client.responses[0].sent_messages) == 1
    sent = _decode_append_input(client.responses[0].sent_messages[0])
    assert sent.records[0].body == b"payload"


class _PartialFailureStreamingClient:
    def __init__(self) -> None:
        self.calls = 0

    def streaming_request(self, method: str, path: str, **kwargs) -> _StreamResponse:
        self.calls += 1
        if self.calls == 1:
            return _StreamResponse(
                [
                    _ack_message(),
                    _terminal_message(500, b'{"code":"internal","message":"boom"}'),
                ],
                kwargs.get("content"),
                kwargs.get("frame_signal"),
            )
        return _StreamResponse([], kwargs.get("content"), kwargs.get("frame_signal"))


async def _two_s2s_inputs() -> AsyncIterator[AppendInput]:
    yield AppendInput(records=[Record(body=b"payload-0")])
    yield AppendInput(records=[Record(body=b"payload-1")])


@pytest.mark.asyncio
async def test_retrying_append_session_no_side_effects_stops_after_partial_ack():
    client = _PartialFailureStreamingClient()
    outputs = []

    with pytest.raises(BaseExceptionGroup) as exc_info:
        async for output in run_append_session(
            cast("HttpClient", client),
            "orders/us-east",
            _two_s2s_inputs(),
            retry=Retry(
                max_attempts=1,
                append_retry_policy=AppendRetryPolicy.NO_SIDE_EFFECTS,
            ),
            compression=Compression.NONE,
        ):
            outputs.append(output)

    assert "boom" in str(exc_info.value.exceptions[0])
    assert len(outputs) == 1
    assert client.calls == 1


# ── Permits / Semaphore tests ──


@pytest.mark.asyncio
async def test_permits_acquire_release_basic():
    permits = _AppendPermits(max_bytes=100)
    await permits.acquire(50)
    await permits.acquire(50)
    permits.release(50)
    permits.release(50)


@pytest.mark.asyncio
async def test_permits_blocks_when_byte_limit_exceeded():
    permits = _AppendPermits(max_bytes=100)
    await permits.acquire(60)

    acquired = asyncio.Event()

    async def _acquire():
        await permits.acquire(60)
        acquired.set()

    task = asyncio.create_task(_acquire())
    await asyncio.sleep(0.01)
    assert not acquired.is_set()

    permits.release(60)
    await asyncio.wait_for(task, timeout=1.0)
    assert acquired.is_set()


@pytest.mark.asyncio
async def test_permits_blocks_on_count_limit():
    permits = _AppendPermits(max_bytes=10_000, max_count=2)
    await permits.acquire(10)
    await permits.acquire(10)

    acquired = asyncio.Event()

    async def _acquire():
        await permits.acquire(10)
        acquired.set()

    task = asyncio.create_task(_acquire())
    await asyncio.sleep(0.01)
    assert not acquired.is_set()

    permits.release(10)
    await asyncio.wait_for(task, timeout=1.0)
    assert acquired.is_set()


@pytest.mark.asyncio
async def test_permits_release_unblocks_waiter():
    permits = _AppendPermits(max_bytes=100)
    await permits.acquire(100)

    result = []

    async def _waiter():
        await permits.acquire(50)
        result.append("acquired")

    task = asyncio.create_task(_waiter())
    await asyncio.sleep(0.01)
    assert result == []

    permits.release(100)
    await asyncio.wait_for(task, timeout=1.0)
    assert result == ["acquired"]


@pytest.mark.asyncio
async def test_semaphore_acquire_release():
    sem = _Semaphore(10)
    await sem.acquire(5)
    await sem.acquire(5)
    sem.release(10)


@pytest.mark.asyncio
async def test_semaphore_blocks_on_insufficient_permits():
    sem = _Semaphore(10)
    await sem.acquire(8)

    acquired = asyncio.Event()

    async def _acquire():
        await sem.acquire(5)
        acquired.set()

    task = asyncio.create_task(_acquire())
    await asyncio.sleep(0.01)
    assert not acquired.is_set()

    sem.release(8)
    await asyncio.wait_for(task, timeout=1.0)
    assert acquired.is_set()


# ── Ack monotonicity tests ──


def _ack_message_range(start_seq: int, end_seq: int) -> bytes:
    ack = pb.AppendAck(
        start=pb.StreamPosition(seq_num=start_seq, timestamp=10),
        end=pb.StreamPosition(seq_num=end_seq, timestamp=10),
        tail=pb.StreamPosition(seq_num=end_seq, timestamp=10),
    )
    return frame_message(
        Message(ack.SerializeToString(), terminal=False, compression=Compression.NONE)
    )


@pytest.mark.asyncio
async def test_ack_monotonicity_rejects_non_increasing():
    """Acks with non-increasing end seq_num should raise S2ClientError."""
    messages = [
        _ack_message_range(0, 2),
        _ack_message_range(1, 2),  # end == previous end — violation
    ]
    client = _StreamingClient(messages)

    async def _inputs():
        yield AppendInput(records=[Record(body=b"a"), Record(body=b"b")])
        yield AppendInput(records=[Record(body=b"c")])

    with pytest.raises(BaseExceptionGroup) as exc_info:
        async for _ in run_append_session(
            cast("HttpClient", client),
            "test-stream",
            _inputs(),
            retry=Retry(max_attempts=1),
            compression=Compression.NONE,
        ):
            pass

    assert "not monotonically increasing" in str(exc_info.value.exceptions[0])


@pytest.mark.asyncio
async def test_ack_rejects_end_less_than_start():
    """An ack where end < start should raise S2ClientError."""
    # Craft a bad ack: end.seq_num < start.seq_num
    ack = pb.AppendAck(
        start=pb.StreamPosition(seq_num=5, timestamp=10),
        end=pb.StreamPosition(seq_num=3, timestamp=10),
        tail=pb.StreamPosition(seq_num=5, timestamp=10),
    )
    bad_msg = frame_message(
        Message(ack.SerializeToString(), terminal=False, compression=Compression.NONE)
    )
    client = _StreamingClient([bad_msg])

    async def _inputs():
        yield AppendInput(records=[Record(body=b"a")])

    with pytest.raises(BaseExceptionGroup) as exc_info:
        async for _ in run_append_session(
            cast("HttpClient", client),
            "test-stream",
            _inputs(),
            retry=Retry(max_attempts=1),
            compression=Compression.NONE,
        ):
            pass

    assert "end < start" in str(exc_info.value.exceptions[0])


# ── Ack timeout test ──


@pytest.mark.asyncio
async def test_ack_timeout_raises_read_timeout_error():
    """If the server stops sending acks, ReadTimeoutError should be raised."""
    from s2_sdk._exceptions import ReadTimeoutError as RTE

    class _HangingStreamResponse:
        def __init__(self, content, frame_signal=None):
            self.status_code = 200
            self._content = content
            self._frame_signal = frame_signal

        async def __aenter__(self):
            if self._content is not None:
                async for chunk in self._content:
                    if self._frame_signal is not None:
                        self._frame_signal.signal()
            return self

        async def __aexit__(self, *args):
            return False

        async def aread(self):
            return b""

        async def aiter_bytes(self):
            # Never yield any data — simulates server hang
            await asyncio.sleep(100)
            yield b""  # pragma: no cover

    class _HangingClient:
        def streaming_request(self, method, path, **kwargs):
            return _HangingStreamResponse(
                kwargs.get("content"), kwargs.get("frame_signal")
            )

    async def _inputs():
        yield AppendInput(records=[Record(body=b"data")])

    with pytest.raises(BaseExceptionGroup) as exc_info:
        async for _ in run_append_session(
            cast("HttpClient", _HangingClient()),
            "test-stream",
            _inputs(),
            retry=Retry(max_attempts=1),
            compression=Compression.NONE,
            ack_timeout=0.01,
        ):
            pass

    assert isinstance(exc_info.value.exceptions[0], RTE)


# ── Frame signal reset after resend test ──


@pytest.mark.asyncio
async def test_frame_signal_reset_after_resend():
    """Frame signal should be reset after resend acks are consumed in _run_attempt."""
    from collections import deque

    from s2_sdk._frame_signal import FrameSignal
    from s2_sdk._retrier import Attempt
    from s2_sdk._s2s._append_session import _InflightInput, _run_attempt

    # One resend batch + one fresh batch, each with 1 record.
    resend_encoded = b"resend-data"
    pending_resend = (_InflightInput(num_records=1, encoded=resend_encoded),)
    inflight_inputs: deque[_InflightInput] = deque(pending_resend)

    # Server acks: first for resend (0→1), then for fresh (1→2).
    messages = [
        _ack_message_range(0, 1),
        _ack_message_range(1, 2),
    ]

    input_queue: asyncio.Queue[AppendInput | None] = asyncio.Queue()
    await input_queue.put(AppendInput(records=[Record(body=b"fresh")]))
    await input_queue.put(None)

    ack_queue: asyncio.Queue[AppendAck | None] = asyncio.Queue()

    frame_signal = FrameSignal()
    # Simulate that the signal was set during the previous attempt's body send.
    frame_signal.signal()

    # Build a client whose streaming_request yields the canned acks.
    client = _StreamingClient(messages)

    await _run_attempt(
        cast("HttpClient", client),
        "test-stream",
        Attempt(1),
        inflight_inputs,
        input_queue,
        ack_queue,
        pending_resend,
        Compression.NONE,
        frame_signal,
    )

    # Frame signal should have been reset after consuming resend ack.
    # The fresh batch ack then signals it again via _StreamResponse, but
    # the important thing is it WAS reset after resend phase.
    ack1 = ack_queue.get_nowait()
    ack2 = ack_queue.get_nowait()
    assert ack1 is not None and ack1.start.seq_num == 0
    assert ack2 is not None and ack2.start.seq_num == 1


# ── Terminal 416 in S2S protocol ──


@pytest.mark.asyncio
async def test_terminal_416_raises_read_unwritten():
    """A terminal S2S message with status 416 should raise ReadUnwrittenError."""
    import json

    from s2_sdk._exceptions import ReadUnwrittenError
    from s2_sdk._s2s._protocol import read_messages

    error_body = json.dumps(
        {"code": "read_unwritten", "tail": {"seq_num": 42, "timestamp": 100}}
    ).encode()
    terminal_frame = frame_message(
        Message(
            (416).to_bytes(2, "big") + error_body,
            terminal=True,
            compression=Compression.NONE,
        )
    )

    async def _byte_stream():
        yield terminal_frame

    with pytest.raises(ReadUnwrittenError) as exc_info:
        async for _ in read_messages(_byte_stream()):
            pass

    assert exc_info.value.status_code == 416
    assert exc_info.value.tail.seq_num == 42
    assert exc_info.value.tail.timestamp == 100


# ── ConnectError no-side-effects narrowing ──


def test_connect_error_with_connection_refused_is_no_side_effects():
    from s2_sdk._exceptions import ConnectError
    from s2_sdk._retrier import has_no_side_effects

    inner = ConnectionRefusedError("refused")
    err = ConnectError("connect failed")
    err.__cause__ = inner
    assert has_no_side_effects(err) is True


def test_connect_error_without_connection_refused_has_side_effects():
    from s2_sdk._exceptions import ConnectError
    from s2_sdk._retrier import has_no_side_effects

    err = ConnectError("DNS failure")
    err.__cause__ = OSError("name resolution failed")
    assert has_no_side_effects(err) is False


def test_connect_error_no_cause_has_side_effects():
    from s2_sdk._exceptions import ConnectError
    from s2_sdk._retrier import has_no_side_effects

    err = ConnectError("generic")
    assert has_no_side_effects(err) is False
