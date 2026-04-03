import asyncio
import time
import uuid
from datetime import timedelta

import pytest

from s2_sdk import (
    S2,
    AppendInput,
    BasinConfig,
    Batching,
    CommandRecord,
    Compression,
    Endpoints,
    FencingTokenMismatchError,
    ReadLimit,
    ReadUnwrittenError,
    Record,
    S2Basin,
    S2ServerError,
    S2Stream,
    SeqNum,
    SeqNumMismatchError,
    StorageClass,
    StreamConfig,
    TailOffset,
    Timestamp,
    Timestamping,
    TimestampingMode,
    metered_bytes,
)


def now_ms() -> int:
    return int(time.time() * 1000)


@pytest.mark.stream
class TestStreamOperations:
    async def test_check_tail_empty_stream(self, stream: S2Stream):
        tail = await stream.check_tail()

        assert tail.seq_num == 0
        assert tail.timestamp == 0

    async def test_check_tail_after_multiple_appends(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"a")]))
        await stream.append(AppendInput(records=[Record(body=b"b"), Record(body=b"c")]))
        tail = await stream.check_tail()
        assert tail.seq_num == 3

    async def test_check_tail_nonexistent_stream_errors(self, shared_basin: S2Basin):
        nonexistent = shared_basin.stream("nonexistent-stream-xyz")
        with pytest.raises(S2ServerError) as exc_info:
            await nonexistent.check_tail()
        assert exc_info.value.code == "stream_not_found"

    async def test_append_single_record(self, stream: S2Stream):
        ack = await stream.append(AppendInput(records=[Record(body=b"record-0")]))

        assert ack.start.seq_num == 0
        assert ack.end.seq_num == 1
        assert ack.tail.seq_num == 1

    async def test_append_multiple_records(self, stream: S2Stream):
        input = AppendInput(
            records=[
                Record(body=f"record-{i}".encode(), headers=[(b"k1", b"v1")])
                for i in range(3)
            ]
        )
        ack = await stream.append(input)

        assert ack.start.seq_num == 0
        assert ack.end.seq_num == 3
        assert ack.tail.seq_num == 3

    async def test_append_empty_body(self, stream: S2Stream):
        ack = await stream.append(AppendInput(records=[Record(body=b"")]))
        assert ack.start.seq_num == 0
        assert ack.end.seq_num == 1

    async def test_append_with_headers(self, stream: S2Stream):
        headers = [(b"k1", b"v1"), (b"k2", b"v2")]
        await stream.append(
            AppendInput(records=[Record(body=b"data", headers=headers)])
        )

        batch = await stream.read(start=SeqNum(0))
        assert len(batch.records) == 1
        assert batch.records[0].headers == headers

    async def test_append_with_empty_header_value(self, stream: S2Stream):
        headers = [(b"k1", b""), (b"k2", b"")]
        await stream.append(
            AppendInput(records=[Record(body=b"lorem", headers=headers)])
        )

        batch = await stream.read(start=SeqNum(0))
        assert len(batch.records) == 1
        assert batch.records[0].headers == headers

    async def test_append_mixed_records_with_and_without_headers(
        self, stream: S2Stream
    ):
        records_in = [
            Record(body=b"lorem", headers=[(b"k1", b"v1")]),
            Record(body=b"ipsum", headers=[(b"k2", b""), (b"k3", b"v3")]),
            Record(body=b"dolor"),
        ]
        ack = await stream.append(AppendInput(records=records_in))
        assert ack.start.seq_num == 0
        assert ack.end.seq_num == 3

        batch = await stream.read(start=SeqNum(0))
        assert len(batch.records) == 3
        assert len(batch.records[0].headers) == 1
        assert len(batch.records[1].headers) == 2
        assert len(batch.records[2].headers) == 0

    async def test_append_with_match_seq_num(self, stream: S2Stream):
        input_0 = AppendInput(records=[Record(body=b"record-0")])
        ack_0 = await stream.append(input_0)

        input_1 = AppendInput(
            records=[Record(body=b"record-1")],
            match_seq_num=ack_0.tail.seq_num,
        )
        ack_1 = await stream.append(input_1)

        assert ack_1.start.seq_num == 1
        assert ack_1.end.seq_num == 2
        assert ack_1.tail.seq_num == 2

    async def test_append_mismatched_seq_num_errors(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"first")]))

        with pytest.raises(SeqNumMismatchError) as exc_info:
            await stream.append(
                AppendInput(records=[Record(body=b"wrong")], match_seq_num=999)
            )
        assert exc_info.value.expected_seq_num == 1

    async def test_append_with_timestamp(self, stream: S2Stream):
        timestamp_0 = now_ms()
        await asyncio.sleep(0.1)
        timestamp_1 = now_ms()

        input = AppendInput(
            records=[
                Record(body=b"record-0", timestamp=timestamp_0),
                Record(body=b"record-1", timestamp=timestamp_1),
            ]
        )
        ack = await stream.append(input)

        assert ack.start.seq_num == 0
        assert ack.start.timestamp == timestamp_0
        assert ack.end.seq_num == 2
        assert ack.end.timestamp == timestamp_1
        assert ack.tail.seq_num == 2
        assert ack.tail.timestamp == timestamp_1

    async def test_append_with_past_timestamp_adjusts_monotonic(self, stream: S2Stream):
        base = now_ms() - 10_000
        first_timestamp = base + 10
        past_timestamp = base

        ack_1 = await stream.append(
            AppendInput(records=[Record(body=b"first", timestamp=first_timestamp)])
        )
        ack_2 = await stream.append(
            AppendInput(records=[Record(body=b"second", timestamp=past_timestamp)])
        )
        assert ack_2.start.timestamp >= ack_1.end.timestamp

    async def test_append_without_timestamp_client_require_errors(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(
            timestamping=Timestamping(mode=TimestampingMode.CLIENT_REQUIRE)
        )
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            stream = shared_basin.stream(stream_name)
            with pytest.raises(S2ServerError) as exc_info:
                await stream.append(AppendInput(records=[Record(body=b"lorem")]))
            assert exc_info.value.code == "invalid"
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_append_with_future_timestamp_uncapped_false_caps(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(timestamping=Timestamping(uncapped=False))
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            stream = shared_basin.stream(stream_name)
            now = now_ms()
            future = now + 3_600_000
            ack = await stream.append(
                AppendInput(records=[Record(body=b"lorem", timestamp=future)])
            )
            assert ack.start.timestamp < future
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_append_with_future_timestamp_uncapped_true_preserves(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(timestamping=Timestamping(uncapped=True))
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            stream = shared_basin.stream(stream_name)
            now = now_ms()
            future = now + 3_600_000
            ack = await stream.append(
                AppendInput(records=[Record(body=b"lorem", timestamp=future)])
            )
            assert ack.start.timestamp == future
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_append_matching_fencing_token(self, stream: S2Stream):
        await stream.append(AppendInput(records=[CommandRecord.fence("my-token")]))

        ack = await stream.append(
            AppendInput(records=[Record(body=b"data")], fencing_token="my-token")
        )
        assert ack.end.seq_num > 0

    async def test_append_mismatched_fencing_token_errors(self, stream: S2Stream):
        await stream.append(AppendInput(records=[CommandRecord.fence("correct-token")]))

        with pytest.raises(FencingTokenMismatchError) as exc_info:
            await stream.append(
                AppendInput(records=[Record(body=b"data")], fencing_token="wrong-token")
            )
        assert exc_info.value.expected_fencing_token == "correct-token"

    async def test_fence_set_and_clear(self, stream: S2Stream):
        await stream.append(AppendInput(records=[CommandRecord.fence("fence-1")]))

        await stream.append(
            AppendInput(records=[Record(body=b"data")], fencing_token="fence-1")
        )

        await stream.append(
            AppendInput(records=[CommandRecord.fence("")], fencing_token="fence-1")
        )

        ack = await stream.append(AppendInput(records=[Record(body=b"after-clear")]))
        assert ack.end.seq_num > 0

    async def test_trim_command_is_accepted(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"r{i}".encode()) for i in range(3)])
        )

        ack = await stream.append(AppendInput(records=[CommandRecord.trim(2)]))
        assert ack.start.seq_num == 3
        assert ack.end.seq_num == 4

    async def test_trim_to_future_seq_num_noop(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"record-1")]))
        ack = await stream.append(AppendInput(records=[CommandRecord.trim(999_999)]))
        assert ack.start.seq_num == 1
        assert ack.end.seq_num == 2

    async def test_append_max_batch_size(self, stream: S2Stream):
        records = [Record(body=b"a") for _ in range(1000)]
        ack = await stream.append(AppendInput(records=records))
        assert ack.start.seq_num == 0
        assert ack.end.seq_num == 1000

    async def test_append_invalid_command_header_errors(self, stream: S2Stream):
        record = Record(body=b"lorem", headers=[(b"", b"not-a-command")])
        with pytest.raises(S2ServerError) as exc_info:
            await stream.append(AppendInput(records=[record]))
        assert exc_info.value.code == "invalid"

    async def test_append_invalid_command_header_with_extra_headers_errors(
        self, stream: S2Stream
    ):
        record = Record(body=b"lorem", headers=[(b"", b"fence"), (b"extra", b"value")])
        with pytest.raises(S2ServerError) as exc_info:
            await stream.append(AppendInput(records=[record]))
        assert exc_info.value.code == "invalid"

    async def test_append_nonexistent_stream_errors(self, shared_basin: S2Basin):
        nonexistent = shared_basin.stream("nonexistent-stream-xyz")
        with pytest.raises(S2ServerError) as exc_info:
            await nonexistent.append(AppendInput(records=[Record(body=b"data")]))
        assert exc_info.value.code == "stream_not_found"

    async def test_read_from_seq_num_zero(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"record-{i}".encode()) for i in range(3)])
        )

        batch = await stream.read(start=SeqNum(0))

        assert len(batch.records) == 3

        for i, record in enumerate(batch.records):
            assert record.seq_num == i
            assert record.body == f"record-{i}".encode()

    async def test_read_with_limit(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"record-{i}".encode()) for i in range(5)])
        )

        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(count=2))

        assert len(batch.records) == 2

        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(bytes=20))

        total_bytes = sum(metered_bytes([r]) for r in batch.records)
        assert total_bytes <= 20

    async def test_read_from_timestamp(self, stream: S2Stream):
        ack = await stream.append(AppendInput(records=[Record(body=b"record-0")]))

        batch = await stream.read(start=Timestamp(ack.start.timestamp))

        assert len(batch.records) == 1

    async def test_read_from_tail_offset(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"record-{i}".encode()) for i in range(5)])
        )

        batch = await stream.read(start=TailOffset(2))

        assert len(batch.records) == 2
        assert batch.records[0].body == b"record-3"
        assert batch.records[1].body == b"record-4"

    async def test_read_until_timestamp(self, stream: S2Stream):
        timestamp_0 = now_ms()
        await asyncio.sleep(0.2)
        timestamp_1 = now_ms()
        await asyncio.sleep(0.2)
        timestamp_2 = now_ms()

        await stream.append(
            AppendInput(
                records=[
                    Record(body=b"record-0", timestamp=timestamp_0),
                    Record(body=b"record-1", timestamp=timestamp_1),
                    Record(body=b"record-2", timestamp=timestamp_2),
                ]
            )
        )

        batch = await stream.read(
            start=Timestamp(timestamp_0), until_timestamp=timestamp_2
        )
        assert len(batch.records) == 2
        assert batch.records[0].timestamp == timestamp_0
        assert batch.records[1].timestamp == timestamp_1

    async def test_read_unbounded(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"r{i}".encode()) for i in range(5)])
        )
        batch = await stream.read(start=SeqNum(0))
        assert len(batch.records) == 5

    async def test_read_count_limit_partial(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"r{i}".encode()) for i in range(5)])
        )
        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(count=3))
        assert len(batch.records) == 3

    async def test_read_count_limit_exact(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"r{i}".encode()) for i in range(3)])
        )
        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(count=3))
        assert len(batch.records) == 3

    async def test_read_count_limit_exceeds(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"r{i}".encode()) for i in range(3)])
        )
        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(count=100))
        assert len(batch.records) == 3

    async def test_read_count_zero_returns_empty(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"data")]))
        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(count=0))
        assert batch.records == []

    async def test_read_bytes_limit_partial(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=b"x" * 100) for _ in range(5)])
        )
        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(bytes=200))
        total = sum(metered_bytes([r]) for r in batch.records)
        assert total <= 200

    async def test_read_bytes_limit_exact(self, stream: S2Stream):
        record = Record(body=b"x" * 100)
        await stream.append(AppendInput(records=[record]))
        batch = await stream.read(
            start=SeqNum(0), limit=ReadLimit(bytes=metered_bytes([record]))
        )
        assert len(batch.records) == 1

    async def test_read_bytes_limit_exceeds(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=b"x" * 10) for _ in range(3)])
        )
        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(bytes=100000))
        assert len(batch.records) == 3

    async def test_read_bytes_zero_returns_empty(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"data")]))
        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(bytes=0))
        assert batch.records == []

    async def test_read_count_over_max_clamps(self, stream: S2Stream):
        for batch_start in range(0, 1000, 100):
            records = [
                Record(body=f"r{i}".encode())
                for i in range(batch_start, batch_start + 100)
            ]
            await stream.append(AppendInput(records=records))
        await stream.append(
            AppendInput(records=[Record(body=f"tail-{i}".encode()) for i in range(5)])
        )

        batch = await stream.read(start=SeqNum(0), limit=ReadLimit(count=2000))
        assert len(batch.records) == 1000

    async def test_read_bytes_over_max_clamps(self, stream: S2Stream):
        body = b"a" * 700_000
        await stream.append(AppendInput(records=[Record(body=body)]))
        await stream.append(AppendInput(records=[Record(body=body)]))

        batch = await stream.read(
            start=SeqNum(0), limit=ReadLimit(bytes=2 * 1024 * 1024)
        )
        total = sum(metered_bytes([r]) for r in batch.records)
        assert total <= 1024 * 1024
        assert len(batch.records) == 1

    async def test_read_from_seq_num_with_bytes_limit(self, stream: S2Stream):
        r1 = Record(body=b"ipsum")
        r2 = Record(body=b"dolor")
        ack = await stream.append(
            AppendInput(records=[Record(body=b"lorem"), r1, r2, Record(body=b"sit")])
        )
        assert ack.start.seq_num == 0
        assert ack.end.seq_num == 4

        bytes_limit = metered_bytes([r1]) + metered_bytes([r2])
        batch = await stream.read(start=SeqNum(1), limit=ReadLimit(bytes=bytes_limit))
        assert len(batch.records) == 2
        assert batch.records[0].seq_num == 1
        assert batch.records[0].body == b"ipsum"
        assert batch.records[1].seq_num == 2
        assert batch.records[1].body == b"dolor"

    async def test_read_from_timestamp_with_count_limit(self, stream: S2Stream):
        base = now_ms() - 10_000
        await stream.append(
            AppendInput(
                records=[
                    Record(body=b"lorem", timestamp=base),
                    Record(body=b"ipsum", timestamp=base + 1),
                    Record(body=b"dolor", timestamp=base + 2),
                    Record(body=b"sit", timestamp=base + 3),
                ]
            )
        )

        batch = await stream.read(start=Timestamp(base + 2), limit=ReadLimit(count=1))
        assert len(batch.records) == 1
        assert batch.records[0].seq_num == 2
        assert batch.records[0].timestamp == base + 2

    async def test_read_from_timestamp_with_bytes_limit(self, stream: S2Stream):
        base = now_ms() - 10_000
        r1 = Record(body=b"ipsum", timestamp=base + 1)
        await stream.append(
            AppendInput(
                records=[
                    Record(body=b"lorem", timestamp=base),
                    r1,
                    Record(body=b"dolor", timestamp=base + 2),
                    Record(body=b"sit", timestamp=base + 3),
                ]
            )
        )

        bytes_limit = metered_bytes([r1]) + 5
        batch = await stream.read(
            start=Timestamp(base + 1), limit=ReadLimit(bytes=bytes_limit)
        )
        assert len(batch.records) == 1
        assert batch.records[0].seq_num == 1
        assert batch.records[0].timestamp == base + 1

    async def test_read_from_timestamp_in_future_errors(self, stream: S2Stream):
        base = now_ms() - 10_000
        await stream.append(
            AppendInput(
                records=[
                    Record(body=b"lorem", timestamp=base),
                    Record(body=b"ipsum", timestamp=base + 1),
                ]
            )
        )

        with pytest.raises(ReadUnwrittenError) as exc_info:
            await stream.read(start=Timestamp(base + 100))
        assert exc_info.value.tail.seq_num == 2

    async def test_read_beyond_tail(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"record-{i}".encode()) for i in range(5)])
        )

        with pytest.raises(ReadUnwrittenError) as exc_info:
            await stream.read(start=SeqNum(100))

        assert exc_info.value.tail.seq_num == 5

    async def test_read_empty_stream_errors(self, stream: S2Stream):
        with pytest.raises(ReadUnwrittenError) as exc_info:
            await stream.read(start=SeqNum(0))
        assert exc_info.value.tail.seq_num == 0
        assert exc_info.value.tail.timestamp == 0

    async def test_read_beyond_tail_with_clamp_to_tail_errors(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"lorem")]))

        with pytest.raises(ReadUnwrittenError) as exc_info:
            await stream.read(start=SeqNum(10), clamp_to_tail=True)
        assert exc_info.value.tail.seq_num == 1

    async def test_read_beyond_tail_with_clamp_to_tail_and_wait_returns_empty(
        self, stream: S2Stream
    ):
        await stream.append(AppendInput(records=[Record(body=b"lorem")]))

        batch = await stream.read(start=SeqNum(10), clamp_to_tail=True, wait=1)
        assert batch.records == []

    async def test_read_empty_stream_with_wait_returns_empty(self, stream: S2Stream):
        batch = await stream.read(start=SeqNum(0), wait=1)
        assert batch.records == []

    async def test_read_start_timestamp_ge_until_errors(self, stream: S2Stream):
        base = now_ms() - 10_000
        await stream.append(
            AppendInput(
                records=[
                    Record(body=b"lorem", timestamp=base),
                    Record(body=b"ipsum", timestamp=base + 1),
                    Record(body=b"dolor", timestamp=base + 2),
                ]
            )
        )

        with pytest.raises(S2ServerError):
            await stream.read(start=Timestamp(base + 2), until_timestamp=base + 2)

    async def test_read_nonexistent_stream_errors(self, shared_basin: S2Basin):
        nonexistent = shared_basin.stream("nonexistent-stream-xyz")
        with pytest.raises(S2ServerError) as exc_info:
            await nonexistent.read(start=SeqNum(0))
        assert exc_info.value.code == "stream_not_found"

    async def test_append_session(self, stream: S2Stream):
        acks = []
        async with stream.append_session() as session:
            for i in range(3):
                records = [
                    Record(body=f"batch-{i}-record-{j}".encode()) for j in range(2)
                ]
                ticket = await session.submit(AppendInput(records=records))
                ack = await ticket
                acks.append(ack)

        assert len(acks) == 3

        exp_seq_num = 0
        for ack in acks:
            assert ack.start.seq_num == exp_seq_num
            exp_seq_num = ack.end.seq_num

    async def test_append_session_mismatched_seq_num_errors(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"first")]))

        with pytest.raises(SeqNumMismatchError) as exc_info:
            async with stream.append_session() as session:
                ticket = await session.submit(
                    AppendInput(records=[Record(body=b"wrong")], match_seq_num=999)
                )
                await ticket
        assert exc_info.value.expected_seq_num == 1

    async def test_append_session_mismatched_fencing_token_errors(
        self, stream: S2Stream
    ):
        await stream.append(AppendInput(records=[CommandRecord.fence("correct")]))

        with pytest.raises(FencingTokenMismatchError) as exc_info:
            async with stream.append_session() as session:
                ticket = await session.submit(
                    AppendInput(records=[Record(body=b"data")], fencing_token="wrong")
                )
                await ticket
        assert exc_info.value.expected_fencing_token == "correct"

    async def test_append_session_nonexistent_stream_errors(
        self, shared_basin: S2Basin
    ):
        nonexistent = shared_basin.stream("nonexistent-stream-xyz")

        with pytest.raises(S2ServerError) as exc_info:
            async with nonexistent.append_session() as session:
                ticket = await session.submit(
                    AppendInput(records=[Record(body=b"data")])
                )
                await ticket
        assert exc_info.value.code == "stream_not_found"

    async def test_producer_delivers_all_acks(self, stream: S2Stream):
        async with stream.producer() as p:
            tickets = []
            for i in range(5):
                ticket = await p.submit(Record(body=f"record-{i}".encode()))
                tickets.append(ticket)

            for i, ticket in enumerate(tickets):
                ack = await ticket
                assert ack.seq_num == i

    async def test_producer_close_delivers_all_indexed_acks_from_same_ack(
        self, stream: S2Stream
    ):
        async with stream.producer() as p:
            t0 = await p.submit(Record(body=b"lorem"))
            t1 = await p.submit(Record(body=b"ipsum"))
            t2 = await p.submit(Record(body=b"dolor"))

        ack0 = await t0
        ack1 = await t1
        ack2 = await t2

        assert ack0.seq_num == 0
        assert ack1.seq_num == 1
        assert ack2.seq_num == 2

        assert ack0.batch is ack1.batch
        assert ack1.batch is ack2.batch

    async def test_producer_close_delivers_all_indexed_acks_from_different_acks(
        self, stream: S2Stream
    ):
        async with stream.producer(
            batching=Batching(max_records=1, linger=timedelta(0))
        ) as p:
            t0 = await p.submit(Record(body=b"lorem"))
            t1 = await p.submit(Record(body=b"ipsum"))
            t2 = await p.submit(Record(body=b"dolor"))

        ack0 = await t0
        ack1 = await t1
        ack2 = await t2

        assert ack0.seq_num == 0
        assert ack1.seq_num == 1
        assert ack2.seq_num == 2

        assert ack0.batch is not ack1.batch
        assert ack1.batch is not ack2.batch

    async def test_producer_nonexistent_stream_errors(self, shared_basin: S2Basin):
        nonexistent = shared_basin.stream("nonexistent-stream-xyz")

        with pytest.raises(S2ServerError) as exc_info:
            async with nonexistent.producer() as p:
                ticket = await p.submit(Record(body=b"data"))
                await ticket
        assert exc_info.value.code == "stream_not_found"

    async def test_read_session_termination(self, stream: S2Stream):
        await stream.append(
            AppendInput(records=[Record(body=f"record-{i}".encode()) for i in range(5)])
        )

        batches = []
        async for batch in stream.read_session(
            start=SeqNum(0), limit=ReadLimit(count=2)
        ):
            batches.append(batch)

        assert len(batches) >= 1
        assert len(batches[0].records) == 2

    async def test_read_session_read_existing_then_tails(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"a"), Record(body=b"b")]))

        received = []

        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(2):
                async for batch in stream.read_session(start=SeqNum(0)):
                    received.extend(batch.records)

        assert len(received) == 2
        assert received[0].body == b"a"
        assert received[1].body == b"b"

    async def test_read_session_tail_then_read_new_then_tail(self, stream: S2Stream):
        async def append_later():
            await asyncio.sleep(1)
            await stream.append(AppendInput(records=[Record(body=b"new")]))

        task = asyncio.create_task(append_later())
        received = []
        try:
            with pytest.raises(asyncio.TimeoutError):
                async with asyncio.timeout(5):
                    async for batch in stream.read_session(start=SeqNum(0)):
                        received.extend(batch.records)
        finally:
            task.cancel()
        assert len(received) == 1
        assert received[0].body == b"new"

    async def test_read_session_tails(self, stream: S2Stream):
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(1):
                async for _ in stream.read_session(start=SeqNum(0)):
                    pass

    async def test_read_session_clamp_to_tail_tails(self, stream: S2Stream):
        await stream.append(AppendInput(records=[Record(body=b"data")]))
        received = []
        with pytest.raises(asyncio.TimeoutError):
            async with asyncio.timeout(1):
                async for batch in stream.read_session(
                    start=SeqNum(100), clamp_to_tail=True
                ):
                    received.extend(batch.records)
        assert received == []

    async def test_read_session_beyond_tail_errors(self, stream: S2Stream):
        with pytest.raises(ReadUnwrittenError) as exc_info:
            async for _ in stream.read_session(start=SeqNum(100)):
                pass
        assert exc_info.value.tail.seq_num == 0


@pytest.mark.stream
@pytest.mark.parametrize("compression", [Compression.GZIP])
class TestCompression:
    async def test_compression_roundtrip_unary(
        self, access_token: str, endpoints: Endpoints | None, compression: Compression
    ):
        async with S2(access_token, endpoints=endpoints, compression=compression) as s2:
            basin_name = f"test-py-sdk-{uuid.uuid4().hex[:8]}"
            await s2.create_basin(
                name=basin_name,
                config=BasinConfig(
                    default_stream_config=StreamConfig(
                        storage_class=StorageClass.STANDARD
                    )
                ),
            )
            try:
                basin = s2.basin(basin_name)
                stream_name = f"stream-{uuid.uuid4().hex[:8]}"
                await basin.create_stream(
                    name=stream_name,
                    config=StreamConfig(timestamping=Timestamping(uncapped=True)),
                )
                try:
                    stream = basin.stream(stream_name)
                    ack = await stream.append(
                        AppendInput(
                            records=[
                                Record(body=b"s" * 2048),
                                Record(body=b"2" * 2048),
                            ]
                        )
                    )
                    assert ack.start.seq_num == 0
                    assert ack.end.seq_num == 2

                    batch = await stream.read(start=SeqNum(0))
                    assert len(batch.records) == 2
                finally:
                    await basin.delete_stream(stream_name)
            finally:
                await s2.delete_basin(basin_name)

    async def test_compression_roundtrip_session(
        self, access_token: str, endpoints: Endpoints | None, compression: Compression
    ):
        async with S2(access_token, endpoints=endpoints, compression=compression) as s2:
            basin_name = f"test-py-sdk-{uuid.uuid4().hex[:8]}"
            await s2.create_basin(
                name=basin_name,
                config=BasinConfig(
                    default_stream_config=StreamConfig(
                        timestamping=Timestamping(mode=TimestampingMode.ARRIVAL)
                    )
                ),
            )
            try:
                basin = s2.basin(basin_name)
                stream_name = f"stream-{uuid.uuid4().hex[:8]}"
                await basin.create_stream(
                    name=stream_name,
                    config=StreamConfig(storage_class=StorageClass.STANDARD),
                )
                try:
                    stream = basin.stream(stream_name)
                    # Payload >= 1KiB to trigger compression
                    async with stream.append_session() as session:
                        ticket = await session.submit(
                            AppendInput(records=[Record(body=b"s2" * 10240)])
                        )
                        ack = await ticket

                    assert ack.start.seq_num == 0
                    assert ack.end.seq_num == 1

                    batch = await stream.read(start=SeqNum(0))
                    assert len(batch.records) == 1
                    assert len(batch.records[0].body) == 20480
                finally:
                    await basin.delete_stream(stream_name)
            finally:
                await s2.delete_basin(basin_name)
