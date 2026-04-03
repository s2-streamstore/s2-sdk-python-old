from __future__ import annotations

import asyncio
from typing import AsyncIterable

from s2_sdk._types import AppendInput, Batching, Record, metered_bytes
from s2_sdk._validators import validate_batching


class BatchAccumulator:
    __slots__ = ("_batching", "_bytes", "_records")

    def __init__(self, batching: Batching) -> None:
        self._batching = batching
        self._records: list[Record] = []
        self._bytes = 0

    def add(self, record: Record) -> None:
        self._records.append(record)
        self._bytes += metered_bytes((record,))

    def take(self) -> list[Record]:
        records = list(self._records)
        self._records.clear()
        self._bytes = 0
        return records

    def is_full(self) -> bool:
        return (
            len(self._records) >= self._batching.max_records
            or self._bytes >= self._batching.max_bytes
        )

    def is_empty(self) -> bool:
        return len(self._records) == 0

    @property
    def linger(self) -> float:
        return self._batching.linger.total_seconds()


async def append_record_batches(
    records: AsyncIterable[Record],
    *,
    batching: Batching | None = None,
) -> AsyncIterable[list[Record]]:
    """Group records into batches based on count, bytes, and linger time."""
    if batching is None:
        batching = Batching()
    validate_batching(batching.max_records, batching.max_bytes)
    acc = BatchAccumulator(batching)
    linger_secs = batching.linger.total_seconds()
    aiter = records.__aiter__()

    while True:
        try:
            record = await anext(aiter)
        except StopAsyncIteration:
            break

        acc.add(record)
        if acc.is_full():
            yield acc.take()
            continue

        try:
            while not acc.is_full():
                if linger_secs > 0:
                    record = await asyncio.wait_for(anext(aiter), timeout=linger_secs)
                else:
                    record = await anext(aiter)
                acc.add(record)
        except StopAsyncIteration:
            pass
        except TimeoutError:
            pass

        if not acc.is_empty():
            yield acc.take()


async def append_inputs(
    records: AsyncIterable[Record],
    *,
    match_seq_num: int | None = None,
    fencing_token: str | None = None,
    batching: Batching | None = None,
) -> AsyncIterable[AppendInput]:
    """Group records into :class:`AppendInput` batches based on count, bytes, and linger time.

    If ``match_seq_num`` is set, it applies to the first input and is auto-incremented for subsequent ones.
    """
    if batching is None:
        batching = Batching()
    async for batch in append_record_batches(records, batching=batching):
        if not batch:
            continue
        append_input = AppendInput(
            records=batch,
            match_seq_num=match_seq_num,
            fencing_token=fencing_token,
        )
        if match_seq_num is not None:
            match_seq_num += len(batch)
        yield append_input
