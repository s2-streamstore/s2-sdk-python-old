import asyncio
import json
import math
import time
from typing import Any, AsyncIterable

import s2_sdk._generated.s2.v1.s2_pb2 as pb
from s2_sdk._client import HttpClient
from s2_sdk._exceptions import UNKNOWN_CODE, ReadTimeoutError, raise_for_416
from s2_sdk._mappers import read_batch_from_proto, read_limit_params, read_start_params
from s2_sdk._retrier import Attempt, compute_backoffs, http_retry_on
from s2_sdk._s2s import _stream_records_path
from s2_sdk._s2s._protocol import parse_error_info, read_messages
from s2_sdk._types import (
    ReadBatch,
    ReadLimit,
    Retry,
    SeqNum,
    TailOffset,
    Timestamp,
    metered_bytes,
)

_HEARTBEAT_TIMEOUT = 20.0  # seconds


async def run_read_session(
    client: HttpClient,
    stream_name: str,
    start: SeqNum | Timestamp | TailOffset,
    limit: ReadLimit | None,
    until_timestamp: int | None,
    clamp_to_tail: bool,
    wait: int | None,
    ignore_command_records: bool,
    retry: Retry,
) -> AsyncIterable[ReadBatch]:
    params = _build_read_params(start, limit, until_timestamp, clamp_to_tail, wait)
    backoffs = compute_backoffs(
        retry._max_retries(),
        min_base_delay=retry.min_base_delay.total_seconds(),
        max_base_delay=retry.max_base_delay.total_seconds(),
    )
    attempt = Attempt(0)

    remaining_count = limit.count if limit and limit.count is not None else None
    remaining_bytes = limit.bytes if limit and limit.bytes is not None else None

    last_tail_at: float | None = None

    while True:
        if wait is not None:
            params["wait"] = _remaining_wait(wait, last_tail_at)

        try:
            async with client.streaming_request(
                "GET",
                _stream_records_path(stream_name),
                params=params,
                headers={"content-type": "s2s/proto"},
            ) as response:
                if response.status_code == 416:
                    body = await response.aread()
                    data = json.loads(body)
                    code = (
                        data.get("code", UNKNOWN_CODE)
                        if isinstance(data, dict)
                        else UNKNOWN_CODE
                    )
                    raise_for_416(data, code)

                if response.status_code != 200:
                    body = await response.aread()
                    raise parse_error_info(body, response.status_code)

                messages = read_messages(response.aiter_bytes())
                while True:
                    try:
                        message_body = await asyncio.wait_for(
                            messages.__anext__(), timeout=_HEARTBEAT_TIMEOUT
                        )
                    except StopAsyncIteration:
                        break
                    except asyncio.TimeoutError:
                        raise ReadTimeoutError("Read session heartbeat timeout")

                    if attempt.value > 0:
                        attempt.value = 0

                    proto_batch = pb.ReadBatch()
                    proto_batch.ParseFromString(message_body)
                    batch = read_batch_from_proto(proto_batch, ignore_command_records)

                    if batch.tail is not None:
                        last_tail_at = time.monotonic()

                    if not batch.records and batch.tail is None:
                        continue

                    if batch.records:
                        last_record = batch.records[-1]
                        params["seq_num"] = last_record.seq_num + 1
                        params.pop("timestamp", None)
                        params.pop("tail_offset", None)

                        if remaining_count is not None:
                            remaining_count = max(
                                remaining_count - len(batch.records), 0
                            )
                            params["count"] = remaining_count
                        if remaining_bytes is not None:
                            remaining_bytes = max(
                                remaining_bytes - metered_bytes(batch.records), 0
                            )
                            params["bytes"] = remaining_bytes

                    yield batch

            return
        except Exception as e:
            if attempt.value < len(backoffs) and http_retry_on(e):
                await asyncio.sleep(backoffs[attempt.value])
                attempt.value += 1
            else:
                raise e


def _build_read_params(
    start: SeqNum | Timestamp | TailOffset,
    limit: ReadLimit | None,
    until_timestamp: int | None,
    clamp_to_tail: bool,
    wait: int | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    params.update(read_start_params(start))
    params.update(read_limit_params(limit))
    if until_timestamp is not None:
        params["until"] = until_timestamp
    if clamp_to_tail:
        params["clamp"] = "true"
    if wait is not None:
        params["wait"] = wait
    return params


def _remaining_wait(baseline: int, last_tail_at: float | None) -> int:
    if last_tail_at is None:
        return baseline
    elapsed = math.ceil(time.monotonic() - last_tail_at)
    return max(0, baseline - elapsed)
