import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest

from s2_sdk import (
    AppendAck,
    AppendInput,
    Batching,
    BatchSubmitTicket,
    Compression,
    IndexedAppendAck,
    Producer,
    Record,
    RecordSubmitTicket,
    Retry,
    S2ClientError,
    StreamPosition,
)

_PATCH_TARGET = "s2_sdk._producer.AppendSession"


def _ack(start_seq: int = 0, end_seq: int = 1) -> AppendAck:
    return AppendAck(
        start=StreamPosition(seq_num=start_seq, timestamp=10),
        end=StreamPosition(seq_num=end_seq, timestamp=10),
        tail=StreamPosition(seq_num=end_seq, timestamp=10),
    )


def _mock_session():
    """Create a mock AppendSession that tracks submit calls and resolves tickets."""
    session = AsyncMock()
    session._seq = 0

    async def _submit(input: AppendInput):
        n = len(input.records)
        start = session._seq
        session._seq += n
        ack = _ack(start_seq=start, end_seq=start + n)
        future: asyncio.Future[AppendAck] = asyncio.get_running_loop().create_future()
        future.set_result(ack)
        return BatchSubmitTicket(future)

    session.submit = AsyncMock(side_effect=_submit)
    session.close = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    return session


def _producer(
    batching: Batching | None = None,
    fencing_token: str | None = None,
    match_seq_num: int | None = None,
) -> tuple[Producer, AsyncMock]:
    mock_session = _mock_session()
    with patch(_PATCH_TARGET, return_value=mock_session):
        p = Producer(
            client=AsyncMock(),
            stream_name="test-stream",
            retry=Retry(max_attempts=1),
            compression=Compression.NONE,
            fencing_token=fencing_token,
            match_seq_num=match_seq_num,
            max_unacked_bytes=5 * 1024 * 1024,
            batching=batching or Batching(),
        )
    return p, mock_session


@pytest.mark.asyncio
async def test_submit_and_await_ticket():
    producer, _ = _producer()
    ticket = await producer.submit(Record(body=b"hello"))
    assert isinstance(ticket, RecordSubmitTicket)
    ack = await ticket
    assert isinstance(ack, IndexedAppendAck)
    assert ack.seq_num == 0
    assert ack.batch.start.seq_num == 0
    await producer.close()


@pytest.mark.asyncio
async def test_multiple_records_batched():
    producer, mock_session = _producer(
        batching=Batching(max_records=3),
    )
    tickets = []
    for i in range(3):
        ticket = await producer.submit(Record(body=f"record-{i}".encode()))
        tickets.append(ticket)

    for i, ticket in enumerate(tickets):
        ack = await ticket
        assert ack.seq_num == i

    # All 3 records should be in one batch
    assert mock_session.submit.call_count == 1
    submitted_input = mock_session.submit.call_args[0][0]
    assert len(submitted_input.records) == 3
    await producer.close()


@pytest.mark.asyncio
async def test_fencing_token_and_match_seq_num():
    producer, mock_session = _producer(
        batching=Batching(max_records=2),
        fencing_token="my-fence",
        match_seq_num=10,
    )
    tickets = []
    for _ in range(2):
        tickets.append(await producer.submit(Record(body=b"x")))

    for t in tickets:
        await t

    submitted_input = mock_session.submit.call_args[0][0]
    assert submitted_input.fencing_token == "my-fence"
    assert submitted_input.match_seq_num == 10
    await producer.close()


@pytest.mark.asyncio
async def test_match_seq_num_auto_increments():
    producer, mock_session = _producer(
        batching=Batching(max_records=2),
        match_seq_num=0,
    )
    # First batch of 2
    for _ in range(2):
        await producer.submit(Record(body=b"x"))
    # Second batch of 2
    for _ in range(2):
        await producer.submit(Record(body=b"y"))

    await producer.close()

    calls = mock_session.submit.call_args_list
    assert len(calls) == 2
    assert calls[0][0][0].match_seq_num == 0
    assert calls[1][0][0].match_seq_num == 2


@pytest.mark.asyncio
async def test_linger_timer_flushes_partial_batch():
    producer, mock_session = _producer(
        batching=Batching(max_records=100, linger=timedelta(milliseconds=10)),
    )
    ticket = await producer.submit(Record(body=b"hello"))
    # Wait for linger to fire
    ack = await asyncio.wait_for(ticket, timeout=1.0)
    assert ack.seq_num == 0
    assert mock_session.submit.call_count == 1
    await producer.close()


@pytest.mark.asyncio
async def test_batch_limit_by_record_count():
    producer, mock_session = _producer(
        batching=Batching(max_records=2),
    )
    t1 = await producer.submit(Record(body=b"a"))
    t2 = await producer.submit(Record(body=b"b"))  # triggers flush

    ack1 = await t1
    ack2 = await t2
    assert ack1.seq_num == 0
    assert ack2.seq_num == 1
    assert mock_session.submit.call_count == 1
    await producer.close()


@pytest.mark.asyncio
async def test_batch_limit_by_bytes():
    producer, mock_session = _producer(
        batching=Batching(max_bytes=20),
    )
    # Each record: 8 (overhead) + body. Body of 12 bytes → 20 bytes metered.
    t1 = await producer.submit(Record(body=b"x" * 12))
    # Second record pushes over the limit
    t2 = await producer.submit(Record(body=b"y" * 12))

    ack1 = await t1
    await t2
    assert ack1.seq_num == 0
    # Second record goes in a new batch
    assert mock_session.submit.call_count == 2
    await producer.close()


@pytest.mark.asyncio
async def test_error_propagation():
    producer, mock_session = _producer(
        batching=Batching(max_records=2),
    )
    error = RuntimeError("session failed")

    async def _failing_submit(input):
        raise error

    mock_session.submit = AsyncMock(side_effect=_failing_submit)

    t1 = await producer.submit(Record(body=b"a"))
    with pytest.raises(RuntimeError, match="session failed"):
        await producer.submit(Record(body=b"b"))  # triggers flush which fails

    with pytest.raises(RuntimeError, match="session failed"):
        await t1

    with pytest.raises(RuntimeError, match="session failed"):
        await producer.close()


@pytest.mark.asyncio
async def test_submit_after_close_raises():
    producer, _ = _producer()
    await producer.close()
    with pytest.raises(S2ClientError, match="closed"):
        await producer.submit(Record(body=b"hello"))


@pytest.mark.asyncio
async def test_context_manager():
    producer, _ = _producer()
    async with producer as p:
        ticket = await p.submit(Record(body=b"hello"))
        ack = await ticket
        assert ack.seq_num == 0


@pytest.mark.asyncio
async def test_close_flushes_remaining():
    producer, mock_session = _producer(
        batching=Batching(max_records=100),
    )
    ticket = await producer.submit(Record(body=b"hello"))
    # Record hasn't been flushed yet (batch limit not met, no linger)
    assert mock_session.submit.call_count == 0
    await producer.close()
    # close() should have flushed
    assert mock_session.submit.call_count == 1
    ack = await ticket
    assert ack.seq_num == 0


@pytest.mark.asyncio
async def test_resolve_batch_error_propagates_to_futures():
    """Test that an error during batch resolution propagates to individual tickets."""
    producer, mock_session = _producer(
        batching=Batching(max_records=2),
    )
    error = RuntimeError("ack failed")

    async def _submit_failing_ticket(input):
        future: asyncio.Future[AppendAck] = asyncio.get_running_loop().create_future()
        future.set_exception(error)
        return BatchSubmitTicket(future)

    mock_session.submit = AsyncMock(side_effect=_submit_failing_ticket)

    t1 = await producer.submit(Record(body=b"a"))
    t2 = await producer.submit(Record(body=b"b"))  # triggers flush

    with pytest.raises(RuntimeError, match="ack failed"):
        await t1
    with pytest.raises(RuntimeError, match="ack failed"):
        await t2

    with pytest.raises(RuntimeError, match="ack failed"):
        await producer.close()


@pytest.mark.asyncio
async def test_drain_resolves_multiple_batches_in_order():
    """Drain loop resolves batches sequentially in FIFO order."""
    producer, mock_session = _producer(
        batching=Batching(max_records=2),
    )
    tickets = []
    # Submit 6 records → 3 batches of 2
    for i in range(6):
        tickets.append(await producer.submit(Record(body=f"r{i}".encode())))

    for i, ticket in enumerate(tickets):
        ack = await ticket
        assert ack.seq_num == i

    assert mock_session.submit.call_count == 3
    await producer.close()


@pytest.mark.asyncio
async def test_error_in_drain_fails_remaining_batches():
    """When drain encounters an error, all remaining pending batches are failed."""
    producer, mock_session = _producer(
        batching=Batching(max_records=1),
    )
    call_count = 0
    error = RuntimeError("drain boom")

    async def _submit_nth(input: AppendInput):
        nonlocal call_count
        call_count += 1
        n = len(input.records)
        future: asyncio.Future[AppendAck] = asyncio.get_running_loop().create_future()
        if call_count == 1:
            # First batch succeeds
            ack = _ack(start_seq=0, end_seq=n)
            future.set_result(ack)
        elif call_count == 2:
            # Second batch fails — drain handles this and fails remaining
            future.set_exception(error)
        # else: leave unresolved (orphaned by drain exit, no warning)
        return BatchSubmitTicket(future)

    mock_session.submit = AsyncMock(side_effect=_submit_nth)

    t1 = await producer.submit(Record(body=b"ok"))
    t2 = await producer.submit(Record(body=b"fail"))
    t3 = await producer.submit(Record(body=b"also-fail"))

    ack1 = await t1
    assert ack1.seq_num == 0

    with pytest.raises(RuntimeError, match="drain boom"):
        await t2
    with pytest.raises(RuntimeError, match="drain boom"):
        await t3

    with pytest.raises(RuntimeError, match="drain boom"):
        await producer.close()
