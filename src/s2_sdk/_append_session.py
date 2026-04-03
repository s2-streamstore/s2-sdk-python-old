from __future__ import annotations

import asyncio
from collections import deque
from typing import AsyncIterable, NamedTuple, Self

from s2_sdk._client import HttpClient
from s2_sdk._exceptions import S2ClientError
from s2_sdk._s2s._append_session import run_append_session
from s2_sdk._types import (
    AppendAck,
    AppendInput,
    Compression,
    Retry,
    metered_bytes,
)
from s2_sdk._validators import validate_append_input


class _UnackedBatch(NamedTuple):
    ack_fut: asyncio.Future[AppendAck]
    metered_bytes: int


class AppendSession:
    """Session for high-throughput appending with backpressure control.

    Supports pipelining multiple :class:`AppendInput`\ s while preserving
    submission order.

    Caution:
        Returned by :meth:`S2Stream.append_session`. Do not instantiate directly.
    """

    __slots__ = (
        "_closed",
        "_client",
        "_compression",
        "_error",
        "_permits",
        "_queue",
        "_retry",
        "_stream_name",
        "_task",
        "_unacked",
    )

    def __init__(
        self,
        client: HttpClient,
        stream_name: str,
        retry: Retry,
        compression: Compression,
        max_unacked_bytes: int,
        max_unacked_batches: int | None,
    ) -> None:
        self._client = client
        self._stream_name = stream_name
        self._retry = retry
        self._compression = compression
        self._permits = _AppendPermits(max_unacked_bytes, max_unacked_batches)

        self._queue: asyncio.Queue[AppendInput | None] = asyncio.Queue()
        self._unacked: deque[_UnackedBatch] = deque()
        self._closed = False
        self._error: BaseException | None = None

        self._task = asyncio.get_running_loop().create_task(self._run())

    async def submit(self, inp: AppendInput) -> BatchSubmitTicket:
        """Submit a batch of records for appending.

        Waits when backpressure limits are reached.
        """
        self._check_ready()
        batch_bytes = metered_bytes(inp.records)
        validate_append_input(len(inp.records), batch_bytes)

        await self._permits.acquire(batch_bytes)
        # Re-check after potentially waiting on backpressure.
        try:
            self._check_ready()
        except BaseException:
            self._permits.release(batch_bytes)
            raise

        ack_fut: asyncio.Future[AppendAck] = asyncio.get_running_loop().create_future()
        self._unacked.append(_UnackedBatch(ack_fut, batch_bytes))
        await self._queue.put(inp)
        return BatchSubmitTicket(ack_fut)

    def _check_ready(self) -> None:
        if self._closed:
            raise S2ClientError("AppendSession is closed")
        if self._error is not None:
            raise self._error

    async def close(self) -> None:
        """Close the session and wait for all submitted batches to be appended."""
        if self._closed:
            return
        self._closed = True
        await self._queue.put(None)
        await self._task
        if self._error is not None:
            raise self._error

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.close()
        return False

    async def _run(self) -> None:
        try:
            async for ack in run_append_session(
                self._client,
                self._stream_name,
                self._input_iter(),
                retry=self._retry,
                compression=self._compression,
                ack_timeout=self._client._request_timeout,
            ):
                self._resolve_next(ack)
        except BaseException as e:
            # Unwrap single-exception ExceptionGroups so callers see
            # the original exception type (e.g. S2ServerError, SeqNumMismatchError).
            exc = e
            while isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
                exc = exc.exceptions[0]
            self._fail_all(exc)

    async def _input_iter(self) -> AsyncIterable[AppendInput]:
        while True:
            item = await self._queue.get()
            if item is None:
                return
            yield item

    def _resolve_next(self, ack: AppendAck) -> None:
        unacked = self._unacked.popleft()
        self._permits.release(unacked.metered_bytes)
        unacked.ack_fut.set_result(ack)

    def _fail_all(self, error: BaseException) -> None:
        self._error = error
        for unacked in self._unacked:
            self._permits.release(unacked.metered_bytes)
            if not unacked.ack_fut.done():
                unacked.ack_fut.set_exception(error)
        self._unacked.clear()
        # Drain queue
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break


class BatchSubmitTicket:
    """Awaitable that resolves to an :class:`AppendAck` once the batch is appended."""

    __slots__ = ("_ack_fut",)

    def __init__(self, ack_fut: asyncio.Future[AppendAck]) -> None:
        self._ack_fut = ack_fut

    def __await__(self):
        return self._ack_fut.__await__()


class _Semaphore:
    __slots__ = ("_event", "_lock", "_value")

    def __init__(self, value: int) -> None:
        self._value = value
        self._event = asyncio.Event()
        self._event.set()
        self._lock = asyncio.Lock()

    async def acquire(self, n: int) -> None:
        while True:
            async with self._lock:
                if self._value >= n:
                    self._value -= n
                    return
            self._event.clear()
            if self._value >= n:
                continue
            await self._event.wait()

    def release(self, n: int) -> None:
        self._value += n
        self._event.set()


class _AppendPermits:
    __slots__ = ("_bytes", "_count")

    def __init__(self, max_bytes: int, max_count: int | None = None) -> None:
        self._bytes = _Semaphore(max_bytes)
        self._count = _Semaphore(max_count) if max_count is not None else None

    async def acquire(self, n_bytes: int) -> None:
        if self._count is not None:
            await self._count.acquire(1)
        try:
            await self._bytes.acquire(n_bytes)
        except BaseException:
            if self._count is not None:
                self._count.release(1)
            raise

    def release(self, n_bytes: int) -> None:
        self._bytes.release(n_bytes)
        if self._count is not None:
            self._count.release(1)
