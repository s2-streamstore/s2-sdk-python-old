from datetime import timedelta

import pytest

from s2_sdk import AppendInput, Batching, Record
from s2_sdk._batching import append_inputs, append_record_batches


async def _async_iter(records: list[Record]):
    for r in records:
        yield r


@pytest.mark.asyncio
async def test_empty_input():
    batches = []
    async for batch in append_record_batches(
        _async_iter([]), batching=Batching(linger=timedelta(0))
    ):
        batches.append(batch)
    assert batches == []


@pytest.mark.asyncio
async def test_count_limit():
    records = [Record(body=f"r{i}".encode()) for i in range(5)]
    batches = []
    async for batch in append_record_batches(
        _async_iter(records), batching=Batching(max_records=2, linger=timedelta(0))
    ):
        batches.append(batch)
    assert len(batches) == 3
    assert len(batches[0]) == 2
    assert len(batches[1]) == 2
    assert len(batches[2]) == 1


@pytest.mark.asyncio
async def test_bytes_limit():
    # Each record: 8 bytes overhead + body. Body of 10 bytes → 18 metered bytes.
    records = [Record(body=b"x" * 10) for _ in range(3)]
    batches = []
    async for batch in append_record_batches(
        _async_iter(records),
        batching=Batching(max_bytes=36, linger=timedelta(0)),
    ):
        batches.append(batch)
    # 36 bytes limit: first 2 records fit (36 bytes), third goes in next batch
    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 1


@pytest.mark.asyncio
async def test_oversized_record_passes():
    records = [Record(body=b"x" * 100)]
    batches = []
    async for batch in append_record_batches(
        _async_iter(records), batching=Batching(max_bytes=10, linger=timedelta(0))
    ):
        batches.append(batch)
    assert len(batches) == 1
    assert len(batches[0]) == 1


@pytest.mark.asyncio
async def test_append_inputs_skips_empty_batches():
    inputs = []
    async for append_input in append_inputs(
        _async_iter([Record(body=b"x" * 100)]),
        batching=Batching(max_bytes=10, linger=timedelta(0)),
    ):
        inputs.append(append_input)
    assert inputs == [AppendInput(records=[Record(body=b"x" * 100)])]
