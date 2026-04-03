import asyncio
from collections import deque
from collections.abc import AsyncGenerator, AsyncIterable
from typing import NamedTuple

import s2_sdk._generated.s2.v1.s2_pb2 as pb
from s2_sdk._client import HttpClient
from s2_sdk._exceptions import ReadTimeoutError, S2ClientError
from s2_sdk._frame_signal import FrameSignal
from s2_sdk._mappers import append_ack_from_proto, append_input_to_proto
from s2_sdk._retrier import Attempt, compute_backoffs, is_safe_to_retry_session
from s2_sdk._s2s import _stream_records_path
from s2_sdk._s2s._protocol import (
    Message,
    frame_message,
    maybe_compress,
    parse_error_info,
    read_messages,
)
from s2_sdk._types import (
    AppendAck,
    AppendInput,
    AppendRetryPolicy,
    Compression,
    Retry,
)

_QUEUE_MAX_SIZE = 100


class _InflightInput(NamedTuple):
    num_records: int
    encoded: bytes


async def run_append_session(
    client: HttpClient,
    stream_name: str,
    inputs: AsyncIterable[AppendInput],
    retry: Retry,
    compression: Compression,
    ack_timeout: float | None = None,
) -> AsyncIterable[AppendAck]:
    input_queue: asyncio.Queue[AppendInput | None] = asyncio.Queue(
        maxsize=_QUEUE_MAX_SIZE
    )
    ack_queue: asyncio.Queue[AppendAck | None] = asyncio.Queue(maxsize=_QUEUE_MAX_SIZE)

    frame_signal: FrameSignal | None = None
    if retry.append_retry_policy == AppendRetryPolicy.NO_SIDE_EFFECTS:
        frame_signal = FrameSignal()

    async def pipe_inputs():
        try:
            async for inp in inputs:
                await input_queue.put(inp)
        finally:
            await input_queue.put(None)

    async def retrying_inner():
        inflight_inputs: deque[_InflightInput] = deque()
        backoffs = compute_backoffs(
            retry._max_retries(),
            min_base_delay=retry.min_base_delay.total_seconds(),
            max_base_delay=retry.max_base_delay.total_seconds(),
        )
        attempt = Attempt(0)
        try:
            while True:
                try:
                    pending_resend = tuple(inflight_inputs)
                    if frame_signal is not None:
                        frame_signal.reset()
                    await _run_attempt(
                        client,
                        stream_name,
                        attempt,
                        inflight_inputs,
                        input_queue,
                        ack_queue,
                        pending_resend,
                        compression,
                        frame_signal,
                        ack_timeout,
                    )
                    return
                except Exception as e:
                    has_inflight = len(inflight_inputs) > 0
                    if attempt.value < len(backoffs) and is_safe_to_retry_session(
                        e,
                        retry.append_retry_policy,
                        has_inflight,
                        frame_signal,
                    ):
                        await asyncio.sleep(backoffs[attempt.value])
                        attempt.value += 1
                    else:
                        raise
        finally:
            await ack_queue.put(None)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(retrying_inner())
        tg.create_task(pipe_inputs())
        while True:
            ack = await ack_queue.get()
            if ack is None:
                break
            yield ack


async def _run_attempt(
    client: HttpClient,
    stream_name: str,
    attempt: Attempt,
    inflight_inputs: deque[_InflightInput],
    input_queue: asyncio.Queue[AppendInput | None],
    ack_queue: asyncio.Queue[AppendAck | None],
    pending_resend: tuple[_InflightInput, ...],
    compression: Compression,
    frame_signal: FrameSignal | None,
    ack_timeout: float | None = None,
) -> None:
    async with client.streaming_request(
        "POST",
        _stream_records_path(stream_name),
        headers={
            "content-type": "s2s/proto",
            "accept": "s2s/proto",
        },
        content=_body_gen(inflight_inputs, input_queue, pending_resend, compression),
        frame_signal=frame_signal,
    ) as response:
        if response.status_code != 200:
            body = await response.aread()
            raise parse_error_info(body, response.status_code)

        prev_ack_end: int | None = None
        resend_remaining = len(pending_resend)

        messages = read_messages(response.aiter_bytes())
        while True:
            try:
                msg_body = await asyncio.wait_for(
                    messages.__anext__(), timeout=ack_timeout
                )
            except StopAsyncIteration:
                break
            except asyncio.TimeoutError:
                raise ReadTimeoutError("Append session ack timeout")

            if attempt.value > 0:
                attempt.value = 0
            ack = pb.AppendAck()
            ack.ParseFromString(msg_body)

            if ack.end.seq_num < ack.start.seq_num:
                raise S2ClientError("Invalid ack: end < start")
            if prev_ack_end is not None and ack.end.seq_num <= prev_ack_end:
                raise S2ClientError("Invalid ack: not monotonically increasing")
            prev_ack_end = ack.end.seq_num

            num_records_sent = inflight_inputs.popleft().num_records
            num_records_ackd = ack.end.seq_num - ack.start.seq_num
            if num_records_sent != num_records_ackd:
                raise S2ClientError(
                    "Number of records sent doesn't match the number of acknowledgements received"
                )
            await ack_queue.put(append_ack_from_proto(ack))

            if resend_remaining > 0:
                resend_remaining -= 1
                if resend_remaining == 0 and frame_signal is not None:
                    frame_signal.reset()


async def _body_gen(
    inflight_inputs: deque[_InflightInput],
    input_queue: asyncio.Queue[AppendInput | None],
    pending_resend: tuple[_InflightInput, ...],
    compression: Compression,
) -> AsyncGenerator[bytes]:
    for resend_inp in pending_resend:
        yield resend_inp.encoded

    while True:
        inp = await input_queue.get()
        if inp is None:
            return
        encoded = _encode_input(inp, compression)
        inflight_inputs.append(
            _InflightInput(num_records=len(inp.records), encoded=encoded)
        )
        yield encoded


def _encode_input(inp: AppendInput, compression: Compression) -> bytes:
    proto = append_input_to_proto(inp)
    body = proto.SerializeToString()
    body, compression = maybe_compress(body, compression)
    return frame_message(Message(body, terminal=False, compression=compression))
