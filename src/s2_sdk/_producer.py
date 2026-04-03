from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Self

from s2_sdk._append_session import AppendSession, BatchSubmitTicket
from s2_sdk._batching import BatchAccumulator
from s2_sdk._client import HttpClient
from s2_sdk._exceptions import S2ClientError
from s2_sdk._types import (
    AppendAck,
    AppendInput,
    Batching,
    Compression,
    IndexedAppendAck,
    Record,
    Retry,
)


@dataclass(slots=True)
class _UnackedBatch:
    ticket: BatchSubmitTicket
    indexed_ack_futs: tuple[asyncio.Future[IndexedAppendAck], ...]


class Producer:
    """High-level interface for submitting individual records.

    Handles batching into :class:`AppendInput` automatically and uses an
    append session internally.

    Caution:
        Returned by :meth:`S2Stream.producer`. Do not instantiate directly.
    """

    __slots__ = (
        "_accumulator",
        "_indexed_ack_futs",
        "_batch_ready",
        "_closed",
        "_drain_task",
        "_error",
        "_fencing_token",
        "_linger_task",
        "_match_seq_num",
        "_unacked",
        "_session",
    )

    def __init__(
        self,
        client: HttpClient,
        stream_name: str,
        retry: Retry,
        compression: Compression,
        fencing_token: str | None,
        match_seq_num: int | None,
        max_unacked_bytes: int,
        batching: Batching,
    ) -> None:
        self._session = AppendSession(
            client=client,
            stream_name=stream_name,
            retry=retry,
            compression=compression,
            max_unacked_bytes=max_unacked_bytes,
            max_unacked_batches=None,
        )
        self._fencing_token = fencing_token
        self._match_seq_num = match_seq_num
        self._accumulator = BatchAccumulator(batching)

        self._indexed_ack_futs: list[asyncio.Future[IndexedAppendAck]] = []
        self._linger_task: asyncio.Task[None] | None = None
        self._unacked: deque[_UnackedBatch] = deque()
        self._batch_ready = asyncio.Event()
        self._drain_task = asyncio.get_running_loop().create_task(self._drain_acks())
        self._closed = False
        self._error: BaseException | None = None

    async def submit(self, record: Record) -> RecordSubmitTicket:
        """Submit a record for appending.

        Waits when backpressure limits are reached.
        """
        if self._closed:
            raise S2ClientError("Producer is closed")
        if self._error is not None:
            raise self._error

        ack_fut: asyncio.Future[IndexedAppendAck] = (
            asyncio.get_running_loop().create_future()
        )
        self._indexed_ack_futs.append(ack_fut)

        first_in_batch = self._accumulator.is_empty()
        self._accumulator.add(record)
        if self._accumulator.is_full():
            await self._flush()
        elif first_in_batch and self._accumulator.linger is not None:
            self._linger_task = asyncio.get_running_loop().create_task(
                self._linger_flush()
            )

        return RecordSubmitTicket(ack_fut)

    async def close(self) -> None:
        """Close the producer and wait for all submitted records to be appended."""
        if self._closed:
            return
        self._closed = True
        await self._flush()
        await self._session.close()
        # Signal drain task to finish and wait for it
        self._batch_ready.set()
        await self._drain_task
        if self._error is not None:
            raise self._error

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.close()
        return False

    async def _flush(self) -> None:
        if self._accumulator.is_empty():
            return

        if self._linger_task is not None:
            self._linger_task.cancel()
            self._linger_task = None

        records = self._accumulator.take()
        indexed_ack_futs = tuple(self._indexed_ack_futs)
        self._indexed_ack_futs.clear()

        batch = AppendInput(
            records=records,
            fencing_token=self._fencing_token,
            match_seq_num=self._match_seq_num,
        )
        if self._match_seq_num is not None:
            self._match_seq_num += len(records)

        try:
            ticket = await self._session.submit(batch)
        except BaseException as e:
            self._error = e
            for ack_fut in indexed_ack_futs:
                if not ack_fut.done():
                    ack_fut.set_exception(e)
                    # Suppress "Future exception was never retrieved" for
                    # futures the caller never got back (submit raised).
                    ack_fut.exception()
            raise

        self._unacked.append(
            _UnackedBatch(ticket=ticket, indexed_ack_futs=indexed_ack_futs)
        )
        self._batch_ready.set()

    async def _drain_acks(self) -> None:
        """Single background task that resolves batches in FIFO order."""
        while True:
            while not self._unacked:
                if self._closed:
                    return
                self._batch_ready.clear()
                if self._unacked:
                    break
                await self._batch_ready.wait()

            unacked = self._unacked.popleft()
            try:
                ack: AppendAck = await unacked.ticket  # type: ignore[assignment]
                for i, ack_fut in enumerate(unacked.indexed_ack_futs):
                    if not ack_fut.done():
                        ack_fut.set_result(
                            IndexedAppendAck(
                                seq_num=ack.start.seq_num + i,
                                batch=ack,
                            )
                        )
            except BaseException as e:
                self._error = e
                for ack_fut in unacked.indexed_ack_futs:
                    if not ack_fut.done():
                        ack_fut.set_exception(e)
                # Fail all remaining unacked batches too
                for remaining in self._unacked:
                    for ack_fut in remaining.indexed_ack_futs:
                        if not ack_fut.done():
                            ack_fut.set_exception(e)
                self._unacked.clear()
                return

    async def _linger_flush(self) -> None:
        assert self._accumulator.linger is not None
        await asyncio.sleep(self._accumulator.linger)
        # Clear before calling _flush() so it doesn't cancel this task.
        self._linger_task = None
        await self._flush()


class RecordSubmitTicket:
    """Awaitable that resolves to an :class:`IndexedAppendAck` once the record is appended."""

    __slots__ = ("_ack_fut",)

    def __init__(self, ack_fut: asyncio.Future[IndexedAppendAck]) -> None:
        self._ack_fut = ack_fut

    def __await__(self):
        return self._ack_fut.__await__()
