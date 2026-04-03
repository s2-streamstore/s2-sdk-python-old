from __future__ import annotations

import asyncio
import json as json_lib
import ssl
import time
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from importlib.metadata import version
from typing import Any, NamedTuple
from urllib.parse import urlencode, urlsplit

import h2.config
import h2.connection
import h2.events

from s2_sdk._compression import compress, decompress
from s2_sdk._exceptions import (
    UNKNOWN_CODE,
    ConnectError,
    ConnectionClosedError,
    ProtocolError,
    ReadTimeoutError,
    S2ClientError,
    S2ServerError,
    TransportError,
    raise_for_412,
    raise_for_416,
)
from s2_sdk._types import Compression

_VERSION = version("s2-sdk")
_USER_AGENT = f"s2-sdk-python/{_VERSION}"

DEFAULT_MAX_STREAMS_PER_CONN = 100
IDLE_TIMEOUT = 90.0
REAPER_INTERVAL = 30.0
_DRAIN_BUFFER_THRESHOLD = 65536  # 64 KiB — drain when write buffer exceeds this


_COMPRESSION_ENCODING = {
    Compression.ZSTD: "zstd",
    Compression.GZIP: "gzip",
}

_ENCODING_COMPRESSION = {v: k for k, v in _COMPRESSION_ENCODING.items()}


class HttpClient:
    __slots__ = (
        "_base_url",
        "_pool",
        "_request_timeout",
        "_scheme",
        "_authority",
        "_headers",
        "_compression",
    )

    def __init__(
        self,
        pool: ConnectionPool,
        base_url: str,
        request_timeout: float,
        headers: dict[str, str] | None = None,
        compression: Compression = Compression.NONE,
    ) -> None:
        self._pool = pool
        self._base_url = base_url
        self._request_timeout = request_timeout
        origin = _origin(base_url)
        self._scheme = origin.scheme
        default_port = 443 if origin.scheme == "https" else 80
        self._authority = (
            origin.host
            if origin.port == default_port
            else f"{origin.host}:{origin.port}"
        )
        self._headers = headers
        self._compression = compression

    async def unary_request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
    ) -> Response:
        pc, state = await self._pool.checkout(self._base_url)
        conn = pc._conn
        stream_id: int | None = None
        try:
            url_path = _build_path(path, params)
            h2_headers = self._build_headers(method, url_path, headers)

            # Build body
            body: bytes | None = None
            if json is not None:
                body = json_lib.dumps(json).encode("utf-8")
                h2_headers.append(("content-type", "application/json"))
            elif content is not None:
                body = content

            # Compress request body
            if body is not None and self._compression != Compression.NONE:
                body = compress(body, self._compression)
                h2_headers.append(
                    ("content-encoding", _COMPRESSION_ENCODING[self._compression])
                )

            if body is not None:
                h2_headers.append(("content-length", str(len(body))))

            end_stream = body is None
            stream_id = await conn.send_headers(
                state, h2_headers, end_stream=end_stream
            )

            if body is not None:
                assert stream_id is not None
                await conn.send_data(stream_id, body, end_stream=True)

            # Wait for response headers
            resp_headers = await asyncio.wait_for(
                state.response_headers,
                timeout=self._request_timeout,
            )
            status_code = _status_from_headers(resp_headers)

            # Read full body
            chunks: list[bytes] = []
            while True:
                item = await asyncio.wait_for(
                    state.data_queue.get(),
                    timeout=self._request_timeout,
                )
                if item is None:
                    break
                chunk, flow_bytes = _queue_item_parts(item)
                chunks.append(chunk)
                if flow_bytes > 0:
                    state.unacked_flow_bytes -= flow_bytes
                    await conn.ack_data(stream_id, flow_bytes)

            if state.error is not None and not state.end_stream_received:
                raise state.error

            resp_body = b"".join(chunks)

            # Decompress response body
            content_encoding = _header_value(resp_headers, "content-encoding")
            if (
                content_encoding is not None
                and content_encoding in _ENCODING_COMPRESSION
            ):
                resp_body = decompress(
                    resp_body, _ENCODING_COMPRESSION[content_encoding]
                )

            response = Response(status_code, resp_body)
        except TransportError:
            raise
        except asyncio.TimeoutError:
            raise ReadTimeoutError("Request timed out")
        finally:
            if stream_id is not None:
                nbytes = _take_all_unacked_flow_bytes(state)
                if nbytes > 0:
                    try:
                        await conn.ack_data(stream_id, nbytes)
                    except Exception:
                        pass
                if not state.ended.is_set():
                    await conn.reset_stream(stream_id)
            conn.release_stream(stream_id, state)
            pc.touch_idle()

        retry_after_ms = _header_value(resp_headers, "retry-after-ms")
        _raise_for_status(response, retry_after_ms=retry_after_ms)
        return response

    @asynccontextmanager
    async def streaming_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        content: Any = None,
        frame_signal: Any = None,
    ) -> AsyncIterator[StreamingResponse]:
        pc, state = await self._pool.checkout(self._base_url)
        conn = pc._conn
        stream_id: int | None = None
        send_task: asyncio.Task[None] | None = None
        try:
            url_path = _build_path(path, params)
            h2_headers = self._build_headers(method, url_path, headers)

            has_body = content is not None
            stream_id = await conn.send_headers(
                state, h2_headers, end_stream=not has_body
            )

            if has_body:
                on_write = frame_signal.signal if frame_signal is not None else None
                send_task = asyncio.get_running_loop().create_task(
                    # stream_id is assigned by send_headers above.
                    _drain_body(conn, stream_id, content, on_write)
                )

                # Propagate sender errors to stream state so the response
                # reader doesn't hang forever on data_queue.get().
                def _on_send_done(
                    task: asyncio.Task[None], _state: Any = state
                ) -> None:
                    if task.cancelled():
                        return
                    exc = task.exception()
                    if exc is not None:
                        conn._fail_stream(_state, exc)

                assert send_task is not None
                send_task.add_done_callback(_on_send_done)

            # Wait for response headers
            resp_headers = await asyncio.wait_for(
                state.response_headers,
                timeout=self._request_timeout,
            )
            status_code = _status_from_headers(resp_headers)

            async def _ack_stream_data(nbytes: int) -> None:
                assert stream_id is not None
                await conn.ack_data(stream_id, nbytes)

            response = StreamingResponse(
                status_code=status_code,
                data_queue=state.data_queue,
                ended=state.ended,
                stream_state=state,
                ack=_ack_stream_data,
            )
            yield response
        except TransportError:
            raise
        except asyncio.TimeoutError:
            raise ReadTimeoutError("Streaming request timed out")
        finally:
            if send_task is not None and not send_task.done():
                send_task.cancel()
                try:
                    await send_task
                except (asyncio.CancelledError, Exception):
                    pass
            # Ack remaining flow bytes to keep connection window healthy
            if stream_id is not None:
                nbytes = _take_all_unacked_flow_bytes(state)
                if nbytes > 0:
                    try:
                        await conn.ack_data(stream_id, nbytes)
                    except Exception:
                        pass
                if not state.ended.is_set():
                    await conn.reset_stream(stream_id)
            conn.release_stream(stream_id, state)
            pc.touch_idle()

    def _build_headers(
        self,
        method: str,
        url_path: str,
        extra_headers: dict[str, str] | None = None,
    ) -> list[tuple[str, str]]:
        h = [
            (":method", method),
            (":path", url_path),
            (":scheme", self._scheme),
            (":authority", self._authority),
            ("user-agent", _USER_AGENT),
        ]
        if self._compression != Compression.NONE:
            h.append(("accept-encoding", _COMPRESSION_ENCODING[self._compression]))
        if self._headers:
            for k, v in self._headers.items():
                h.append((k.lower(), v))
        if extra_headers:
            for k, v in extra_headers.items():
                h.append((k.lower(), v))
        return h


class ConnectionPool:
    __slots__ = (
        "_closed",
        "_connect_timeout",
        "_hosts",
        "_host_locks",
        "_reaper_task",
        "_ssl_context",
    )

    def __init__(self, connect_timeout: float) -> None:
        self._connect_timeout = connect_timeout
        self._hosts: dict[str, list[_PooledConnection]] = {}
        self._host_locks: dict[str, asyncio.Lock] = {}
        self._reaper_task: asyncio.Task[None] | None = None
        self._closed = False
        self._ssl_context = ssl.create_default_context()
        self._ssl_context.set_alpn_protocols(["h2"])

    async def checkout(self, base_url: str) -> _Checkout:
        if self._closed:
            raise S2ClientError("Pool is closed")

        self._ensure_reaper()

        result = self._try_checkout(base_url)
        if result is not None:
            return result

        lock = self._host_locks.get(base_url)
        if lock is None:
            lock = asyncio.Lock()
            self._host_locks[base_url] = lock

        async with lock:
            # Re-check after acquiring lock — another caller may have
            # created a connection while we waited.
            result = self._try_checkout(base_url)
            if result is not None:
                return result

            scheme, host, port = _origin(base_url)
            use_ssl = self._ssl_context if scheme == "https" else None

            conn = Connection(
                host=host,
                port=port,
                ssl_context=use_ssl,
                connect_timeout=self._connect_timeout,
            )
            await conn.connect()

            # Wait briefly for the server's initial SETTINGS frame so
            # max_concurrent_streams reflects the real limit.
            try:
                await asyncio.wait_for(
                    conn._settings_received.wait(),
                    timeout=self._connect_timeout,
                )
            except asyncio.TimeoutError:
                pass  # Proceed with h2 defaults
            if conn._recv_dead:
                await conn.close()
                raise ConnectError(
                    f"Connection to {host}:{port} closed before HTTP/2 SETTINGS"
                )

            pc = _PooledConnection(conn)
            conns = self._hosts.get(base_url)
            if conns is None:
                conns = [pc]
                self._hosts[base_url] = conns
            else:
                conns.append(pc)
            if conn._settings_received.is_set() and conn.max_concurrent_streams <= 0:
                await pc.close()
                conns.remove(pc)
                raise ProtocolError("Connection has no available stream capacity")
            state = pc._conn.reserve_stream()
            return _Checkout(pc, state)

    def _try_checkout(self, base_url: str) -> _Checkout | None:
        conns = self._hosts.get(base_url)
        if conns is not None:
            for pc in conns:
                if pc.has_capacity:
                    state = pc._conn.reserve_stream()
                    return _Checkout(pc, state)
        return None

    def _ensure_reaper(self) -> None:
        if self._reaper_task is None or self._reaper_task.done():
            self._reaper_task = asyncio.get_running_loop().create_task(
                self._reap_idle()
            )

    async def _reap_idle(self) -> None:
        while not self._closed:
            await asyncio.sleep(REAPER_INTERVAL)
            empty_hosts: list[str] = []
            for base_url, conns in tuple(self._hosts.items()):
                to_close: list[_PooledConnection] = []
                for pc in conns:
                    if not pc._conn.is_available:
                        to_close.append(pc)
                    elif (
                        pc.is_idle
                        and pc.idle_for() > IDLE_TIMEOUT
                        and len(conns) - len(to_close) > 1
                    ):
                        to_close.append(pc)
                for pc in to_close:
                    conns.remove(pc)
                for pc in to_close:
                    await pc.close()
                if not conns:
                    empty_hosts.append(base_url)
            for base_url in empty_hosts:
                self._hosts.pop(base_url, None)
                lock = self._host_locks.get(base_url)
                if lock is not None and not lock.locked():
                    self._host_locks.pop(base_url, None)

    async def close(self) -> None:
        self._closed = True
        if self._reaper_task is not None:
            self._reaper_task.cancel()
            try:
                await self._reaper_task
            except asyncio.CancelledError:
                pass
        for conns in self._hosts.values():
            for pc in conns:
                await pc.close()
        self._hosts.clear()
        self._host_locks.clear()


class Response:
    __slots__ = ("status_code", "_content")

    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self._content = content

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def text(self) -> str:
        return self._content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json_lib.loads(self._content)


class StreamingResponse:
    __slots__ = (
        "status_code",
        "_data_queue",
        "_ended",
        "_stream_state",
        "_buf",
        "_ack",
    )

    def __init__(
        self,
        status_code: int,
        data_queue: asyncio.Queue[tuple[bytes, int] | bytes | None],
        ended: asyncio.Event,
        stream_state: Any,
        ack: Callable[[int], Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self._data_queue = data_queue
        self._ended = ended
        self._stream_state = stream_state
        self._buf = bytearray()
        self._ack = ack

    async def aread(self) -> bytes:
        chunks: list[bytes] = []
        if self._buf:
            chunks.append(bytes(self._buf))
            self._buf.clear()
        while True:
            item = await self._data_queue.get()
            if item is None:
                break
            chunk, flow_bytes = _queue_item_parts(item)
            chunks.append(chunk)
            if self._ack is not None and flow_bytes > 0:
                self._stream_state.unacked_flow_bytes -= flow_bytes
                await self._ack(flow_bytes)
        if (
            self._stream_state.error is not None
            and not self._stream_state.end_stream_received
        ):
            raise self._stream_state.error
        return b"".join(chunks)

    async def aiter_bytes(self) -> AsyncGenerator[bytes, None]:
        if self._buf:
            yield bytes(self._buf)
            self._buf.clear()
        while True:
            item = await self._data_queue.get()
            if item is None:
                if (
                    self._stream_state.error is not None
                    and not self._stream_state.end_stream_received
                ):
                    raise self._stream_state.error
                return
            chunk, flow_bytes = _queue_item_parts(item)
            try:
                yield chunk
            finally:
                if self._ack is not None and flow_bytes > 0:
                    self._stream_state.unacked_flow_bytes -= flow_bytes
                    await self._ack(flow_bytes)


class _PooledConnection:
    __slots__ = ("_conn", "_idle_since")

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self._idle_since: float | None = None

    @property
    def has_capacity(self) -> bool:
        return (
            self._conn.is_available
            and self._conn._settings_received.is_set()
            and self._conn.open_stream_count < self._conn.max_concurrent_streams
        )

    @property
    def is_idle(self) -> bool:
        return self._conn.open_stream_count == 0

    def touch_idle(self) -> None:
        if self._conn.open_stream_count == 0:
            self._idle_since = time.monotonic()

    def idle_for(self) -> float:
        if self._idle_since is None:
            return 0.0
        return time.monotonic() - self._idle_since

    async def close(self) -> None:
        await self._conn.close()


@dataclass
class _StreamState:
    response_headers: asyncio.Future[list[tuple[str, str]]] = field(
        default_factory=lambda: asyncio.get_running_loop().create_future()
    )
    data_queue: asyncio.Queue[tuple[bytes, int] | bytes | None] = field(
        default_factory=asyncio.Queue
    )
    ended: asyncio.Event = field(default_factory=asyncio.Event)
    window_updated: asyncio.Event = field(default_factory=asyncio.Event)
    error: BaseException | None = None
    end_stream_received: bool = False
    unacked_flow_bytes: int = 0


class Connection:
    __slots__ = (
        "_host",
        "_port",
        "_ssl_context",
        "_connect_timeout",
        "_reader",
        "_writer",
        "_h2",
        "_write_lock",
        "_streams",
        "_pending_streams",
        "_recv_task",
        "_closed",
        "_goaway_received",
        "_recv_dead",
        "_settings_received",
    )

    def __init__(
        self,
        host: str,
        port: int,
        ssl_context: ssl.SSLContext | None,
        connect_timeout: float,
    ) -> None:
        self._host = host
        self._port = port
        self._ssl_context = ssl_context
        self._connect_timeout = connect_timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._h2: h2.connection.H2Connection | None = None
        self._write_lock = asyncio.Lock()
        self._streams: dict[int, _StreamState] = {}
        self._pending_streams: dict[int, _StreamState] = {}
        self._recv_task: asyncio.Task[None] | None = None
        self._closed = False
        self._goaway_received = False
        self._recv_dead = False
        self._settings_received = asyncio.Event()

    async def connect(self) -> None:
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self._host,
                    self._port,
                    ssl=self._ssl_context,
                ),
                timeout=self._connect_timeout,
            )
        except asyncio.TimeoutError:
            raise ConnectError(f"Connection to {self._host}:{self._port} timed out")
        except OSError as e:
            raise ConnectError(str(e)) from e

        if self._ssl_context is not None:
            assert self._writer is not None
            ssl_object = self._writer.get_extra_info("ssl_object")
            if ssl_object is not None:
                alpn = ssl_object.selected_alpn_protocol()
                if alpn != "h2":
                    self._writer.close()
                    raise ConnectError(
                        f"ALPN negotiation failed: expected 'h2', got {alpn!r}"
                    )

        config = h2.config.H2Configuration(client_side=True, header_encoding="utf-8")
        self._h2 = h2.connection.H2Connection(config=config)
        self._h2.initiate_connection()
        self._flush_h2_data_sync()
        assert self._writer is not None
        await self._writer.drain()
        self._recv_task = asyncio.get_running_loop().create_task(self._recv_loop())

    def reserve_stream(self) -> _StreamState:
        """Reserve per-connection capacity for a future outbound stream."""
        state = _StreamState()
        self._pending_streams[id(state)] = state
        return state

    async def send_headers(
        self,
        state: _StreamState,
        headers: list[tuple[str, str]],
        end_stream: bool = False,
    ) -> int:
        """Atomically allocate a stream ID and send request headers."""
        assert self._h2 is not None
        async with self._write_lock:
            if state.error is not None:
                raise state.error
            pending_state = self._pending_streams.pop(id(state), None)
            if pending_state is None:
                raise ProtocolError("Stream reservation missing")
            stream_id = self._h2.get_next_available_stream_id()
            self._streams[stream_id] = state
            try:
                self._h2.send_headers(stream_id, headers, end_stream=end_stream)
                await self._flush_h2_data_and_drain()
            except Exception:
                self._streams.pop(stream_id, None)
                raise
            return stream_id

    async def send_data(
        self,
        stream_id: int,
        data: bytes,
        end_stream: bool = False,
        on_write: Callable[[], None] | None = None,
    ) -> None:
        """Send data on a stream, respecting flow control windows.

        Chunks by flow control window + max frame size. If ``on_write`` is
        provided, it is called after each chunk is flushed to the socket.
        """
        assert self._h2 is not None
        offset = 0
        while offset < len(data):
            state = self._streams.get(stream_id)
            if state and state.error:
                raise state.error

            # Window check + send in a single lock acquisition to avoid TOCTOU.
            sent = False
            async with self._write_lock:
                window = self._h2.local_flow_control_window(stream_id)
                if window > 0:
                    max_frame = self._h2.max_outbound_frame_size
                    chunk_size = min(len(data) - offset, window, max_frame)
                    chunk = data[offset : offset + chunk_size]
                    is_last_chunk = offset + chunk_size >= len(data)

                    self._h2.send_data(
                        stream_id,
                        chunk,
                        end_stream=end_stream and is_last_chunk,
                    )
                    await self._flush_h2_data()
                    offset += chunk_size
                    sent = True

            if sent:
                if on_write:
                    on_write()
                continue

            # Window exhausted — wait for update (lock released).
            # Caller-level timeouts (unary or streaming) will cancel this
            # if it takes too long.
            state = self._streams.get(stream_id)
            if state:
                state.window_updated.clear()
                # Re-check under lock to avoid missing an update.
                async with self._write_lock:
                    window = self._h2.local_flow_control_window(stream_id)
                if window <= 0:
                    await state.window_updated.wait()

        # Handle empty data with end_stream
        if not data and end_stream:
            async with self._write_lock:
                self._h2.send_data(stream_id, b"", end_stream=True)
                await self._flush_h2_data_and_drain()
            if on_write:
                on_write()

    async def end_stream(self, stream_id: int) -> None:
        """Send END_STREAM on a stream."""
        assert self._h2 is not None
        async with self._write_lock:
            self._h2.send_data(stream_id, b"", end_stream=True)
            await self._flush_h2_data_and_drain()

    async def ack_data(self, stream_id: int, nbytes: int) -> None:
        """Acknowledge received data to update the flow control window."""
        assert self._h2 is not None
        async with self._write_lock:
            self._h2.acknowledge_received_data(nbytes, stream_id)
            await self._flush_h2_data()

    async def reset_stream(self, stream_id: int) -> None:
        """Send RST_STREAM to tell the peer to stop sending."""
        assert self._h2 is not None
        try:
            async with self._write_lock:
                self._h2.reset_stream(stream_id)
                await self._flush_h2_data_and_drain()
        except Exception:
            pass  # Best effort

    def release_stream(
        self, stream_id: int | None, state: _StreamState | None = None
    ) -> None:
        """Clean up active or reserved stream state."""
        if stream_id is not None:
            self._streams.pop(stream_id, None)
        if state is not None:
            self._pending_streams.pop(id(state), None)

    @property
    def is_available(self) -> bool:
        """Connection is usable for new streams."""
        return not self._closed and not self._goaway_received and not self._recv_dead

    @property
    def max_concurrent_streams(self) -> int:
        assert self._h2 is not None
        advertised = self._h2.remote_settings.max_concurrent_streams
        if advertised is None:
            return DEFAULT_MAX_STREAMS_PER_CONN
        return int(advertised)

    @property
    def open_stream_count(self) -> int:
        return len(self._streams) + len(self._pending_streams)

    async def close(self) -> None:
        """Send GOAWAY, cancel recv_loop, close socket."""
        if self._closed:
            return
        self._closed = True
        self._fail_all_streams(ConnectionClosedError("Connection closed"))

        if self._h2 is not None and self._writer is not None:
            try:
                async with self._write_lock:
                    self._h2.close_connection()
                    await self._flush_h2_data_and_drain()
            except Exception:
                pass

        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):
                pass

        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass

    def _flush_h2_data_sync(self) -> None:
        """Write pending h2 bytes to socket. Must be called under _write_lock or during init."""
        assert self._h2 is not None
        assert self._writer is not None
        data = self._h2.data_to_send()
        if data:
            self._writer.write(data)

    async def _flush_h2_data(self) -> None:
        """Write pending h2 bytes, draining only when the buffer is large.

        This allows small writes to coalesce in the kernel buffer, reducing
        the number of syscalls and await-points on high-rate paths.
        """
        assert self._writer is not None
        self._flush_h2_data_sync()
        transport = self._writer.transport
        if (
            transport is not None
            and transport.get_write_buffer_size() >= _DRAIN_BUFFER_THRESHOLD
        ):
            await self._writer.drain()

    async def _flush_h2_data_and_drain(self) -> None:
        """Write pending h2 bytes and unconditionally drain the socket."""
        assert self._writer is not None
        self._flush_h2_data_sync()
        await self._writer.drain()

    async def _recv_loop(self) -> None:
        """Background task: read from socket, feed to h2, dispatch events."""
        assert self._reader is not None
        assert self._h2 is not None
        try:
            while not self._closed:
                data = await self._reader.read(65535)
                if not data:
                    self._fail_all_streams(
                        ConnectionClosedError("Connection closed by remote")
                    )
                    return

                # All h2 state mutations must be under _write_lock to avoid
                # concurrent access with send_data/send_headers.
                async with self._write_lock:
                    events = self._h2.receive_data(data)
                    for event in events:
                        self._handle_event(event)
                    # Flush h2 data generated by event handling (e.g. window update ACKs)
                    await self._flush_h2_data()
        except Exception as e:
            if not self._closed:
                self._fail_all_streams(ConnectionClosedError(f"recv_loop error: {e}"))
        finally:
            self._recv_dead = True

    def _handle_event(self, event: h2.events.Event) -> None:
        if isinstance(event, h2.events.ResponseReceived):
            state = self._streams.get(event.stream_id)
            if state and not state.response_headers.done():
                # h2 returns str tuples when header_encoding is set,
                # but type stubs declare bytes.
                headers = [(str(n), str(v)) for n, v in event.headers]
                state.response_headers.set_result(headers)

        elif isinstance(event, h2.events.DataReceived):
            state = self._streams.get(event.stream_id)
            if state:
                state.data_queue.put_nowait((event.data, event.flow_controlled_length))
                state.unacked_flow_bytes += event.flow_controlled_length

        elif isinstance(event, h2.events.StreamEnded):
            state = self._streams.get(event.stream_id)
            if state:
                state.end_stream_received = True
                state.data_queue.put_nowait(None)
                state.ended.set()

        elif isinstance(event, h2.events.WindowUpdated):
            if event.stream_id == 0:
                # Connection-level window update — wake all streams.
                for state in self._streams.values():
                    state.window_updated.set()
            else:
                state = self._streams.get(event.stream_id)
                if state:
                    state.window_updated.set()

        elif isinstance(event, h2.events.StreamReset):
            state = self._streams.get(event.stream_id)
            if state:
                err = ProtocolError(
                    f"Stream reset with error code {event.error_code}",
                    error_code=event.error_code,
                )
                self._fail_stream(state, err)

        elif isinstance(event, h2.events.ConnectionTerminated):
            self._goaway_received = True
            err = ProtocolError(
                f"GOAWAY received: error_code={event.error_code}, "
                f"last_stream_id={event.last_stream_id}",
                error_code=event.error_code,
            )
            # Only fail streams the server never processed.
            if event.last_stream_id is not None:
                for stream_id, state in self._streams.items():
                    if stream_id > event.last_stream_id:
                        self._fail_stream(state, err)
            else:
                self._fail_all_streams(err)

        elif isinstance(event, h2.events.RemoteSettingsChanged):
            self._settings_received.set()
            for state in self._streams.values():
                state.window_updated.set()

    def _fail_stream(self, state: _StreamState, error: BaseException) -> None:
        if state.error is None:
            state.error = error
        if not state.response_headers.done():
            state.response_headers.set_exception(state.error)
        if not state.ended.is_set():
            state.data_queue.put_nowait(None)
            state.ended.set()
        state.window_updated.set()

    def _fail_all_streams(self, error: BaseException) -> None:
        for state in self._streams.values():
            self._fail_stream(state, error)
        for state in self._pending_streams.values():
            self._fail_stream(state, error)


async def _drain_body(
    conn: Connection,
    stream_id: int,
    content: Any,
    on_write: Any | None,
) -> None:
    """Drain an async generator body into h2 send_data calls."""
    try:
        async for chunk in content:
            await conn.send_data(stream_id, chunk, on_write=on_write)
        await conn.end_stream(stream_id)
    except (asyncio.CancelledError, Exception):
        await conn.reset_stream(stream_id)
        raise


def _queue_item_parts(item: tuple[bytes, int] | bytes) -> tuple[bytes, int]:
    if isinstance(item, tuple):
        return item
    return item, len(item)


def _take_all_unacked_flow_bytes(state: _StreamState) -> int:
    nbytes = state.unacked_flow_bytes
    state.unacked_flow_bytes = 0
    return nbytes


def _parse_retry_after_ms(raw: str | None) -> float | None:
    if raw is None:
        return None
    try:
        return int(raw) / 1000.0
    except (ValueError, TypeError):
        return None


def _raise_for_status(response: Response, *, retry_after_ms: str | None = None) -> None:
    status = response.status_code
    if 200 <= status < 300:
        return

    retry_after = _parse_retry_after_ms(retry_after_ms)
    body: Any | None
    try:
        body = response.json()
    except Exception:
        body = None

    if status == 412 and isinstance(body, dict):
        code = body.get("code", UNKNOWN_CODE)
        raise_for_412(body, code)

    if status == 416 and isinstance(body, dict):
        code = body.get("code", UNKNOWN_CODE)
        raise_for_416(body, code)

    if isinstance(body, dict):
        message = body.get("message", response.text)
        code = body.get("code", UNKNOWN_CODE)
    else:
        message = response.text
        code = UNKNOWN_CODE

    err = S2ServerError(code, message, status)
    err._retry_after = retry_after
    raise err


class _Checkout(NamedTuple):
    connection: _PooledConnection
    state: _StreamState


class _Origin(NamedTuple):
    scheme: str
    host: str
    port: int


def _origin(base_url: str) -> _Origin:
    url = base_url if "://" in base_url else f"https://{base_url}"
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise S2ClientError(f"Unsupported URL scheme: {parsed.scheme!r}")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise S2ClientError("Endpoint URL must be an origin without path or query")
    if parsed.username is not None or parsed.password is not None:
        raise S2ClientError("Endpoint URL must not include userinfo")
    if parsed.hostname is None:
        raise S2ClientError("Endpoint URL must include a host")
    default_port = 443 if scheme == "https" else 80
    try:
        port = parsed.port if parsed.port is not None else default_port
    except ValueError as e:
        raise S2ClientError(f"Invalid endpoint URL: {e}") from e
    return _Origin(scheme, parsed.hostname, port)


def _build_path(path: str, params: dict[str, Any] | None) -> str:
    if not params:
        return path
    qs = urlencode({k: v for k, v in params.items() if v is not None})
    return f"{path}?{qs}" if qs else path


def _status_from_headers(headers: list[tuple[str, str]]) -> int:
    for name, value in headers:
        if name == ":status":
            return int(value)
    raise TransportError("No :status header in response")


def _header_value(headers: list[tuple[str, str]], name: str) -> str | None:
    for k, v in headers:
        if k == name:
            return v
    return None
