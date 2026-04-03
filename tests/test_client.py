import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from s2_sdk._client import (
    IDLE_TIMEOUT,
    ConnectionPool,
    HttpClient,
    Response,
    _PooledConnection,
    _raise_for_status,
    _StreamState,
)
from s2_sdk._exceptions import (
    ConnectionClosedError,
    ProtocolError,
    ReadTimeoutError,
    S2ClientError,
    S2ServerError,
)
from s2_sdk._types import Compression

_DEFAULT_MAX_STREAMS = 100


def _mock_connection(
    open_streams: int = 0,
    available: bool = True,
    max_concurrent_streams: int = _DEFAULT_MAX_STREAMS,
) -> AsyncMock:
    conn = AsyncMock()
    conn.is_available = available
    conn.open_stream_count = open_streams
    conn.max_concurrent_streams = max_concurrent_streams
    conn.connect = AsyncMock()
    conn.close = AsyncMock()
    conn._streams = {}
    conn._pending_streams = {}
    conn._recv_dead = False
    conn._settings_received = asyncio.Event()
    conn._settings_received.set()

    def _reserve_stream():
        state = MagicMock()
        conn._pending_streams[id(state)] = state
        conn.open_stream_count = len(conn._streams) + len(conn._pending_streams)
        return state

    conn.reserve_stream = _reserve_stream
    return conn


@pytest.fixture
def pool():
    return ConnectionPool(connect_timeout=5.0)


@pytest.mark.asyncio
async def test_checkout_creates_connection(pool: ConnectionPool):
    with patch("s2_sdk._client.Connection") as MockConn:
        mock_conn = _mock_connection()
        MockConn.return_value = mock_conn
        pc, state = await pool.checkout("https://example.com")
        assert isinstance(pc, _PooledConnection)
        assert state is not None
        mock_conn.connect.assert_awaited_once()
    await pool.close()


@pytest.mark.asyncio
async def test_checkout_reuses_connection_with_capacity(pool: ConnectionPool):
    with patch("s2_sdk._client.Connection") as MockConn:
        mock_conn = _mock_connection()
        MockConn.return_value = mock_conn
        pc1, _ = await pool.checkout("https://example.com")
        pc2, _ = await pool.checkout("https://example.com")
        assert pc1 is pc2
        # Only one connection created
        assert MockConn.call_count == 1
    await pool.close()


@pytest.mark.asyncio
async def test_checkout_scales_when_saturated(pool: ConnectionPool):
    with patch("s2_sdk._client.Connection") as MockConn:
        conn1 = _mock_connection()
        conn2 = _mock_connection()
        MockConn.side_effect = [conn1, conn2]

        # Fill first connection to capacity
        for _ in range(_DEFAULT_MAX_STREAMS):
            await pool.checkout("https://example.com")

        # Next checkout should create a new connection
        pc, _ = await pool.checkout("https://example.com")
        assert pc._conn is conn2
    await pool.close()


@pytest.mark.asyncio
async def test_checkout_different_hosts(pool: ConnectionPool):
    with patch("s2_sdk._client.Connection") as MockConn:
        MockConn.return_value = _mock_connection()
        pc1, _ = await pool.checkout("https://a.example.com")
        MockConn.return_value = _mock_connection()
        pc2, _ = await pool.checkout("https://b.example.com")
        assert pc1 is not pc2
    await pool.close()


@pytest.mark.asyncio
async def test_close_pool(pool: ConnectionPool):
    with patch("s2_sdk._client.Connection") as MockConn:
        MockConn.return_value = _mock_connection()
        await pool.checkout("https://example.com")
    await pool.close()
    assert pool._closed


@pytest.mark.asyncio
async def test_closed_pool_raises(pool: ConnectionPool):
    await pool.close()
    with pytest.raises(S2ClientError, match="Pool is closed"):
        await pool.checkout("https://example.com")


@pytest.mark.asyncio
async def test_dead_connection_not_reused(pool: ConnectionPool):
    """A connection whose recv_loop has died should not be reused."""
    with patch("s2_sdk._client.Connection") as MockConn:
        conn1 = _mock_connection()
        conn2 = _mock_connection()
        MockConn.side_effect = [conn1, conn2]

        pc1, state1 = await pool.checkout("https://example.com")
        # Simulate release so open_stream_count goes back down
        conn1._pending_streams.pop(id(state1), None)
        conn1.open_stream_count = 0

        # Mark connection as dead (real is_available checks _recv_dead)
        conn1._recv_dead = True
        conn1.is_available = False

        # Next checkout should create a new connection
        pc2, _ = await pool.checkout("https://example.com")
        assert pc2._conn is conn2
    await pool.close()


@pytest.mark.asyncio
async def test_settings_not_received_skips_reuse(pool: ConnectionPool):
    """Connection without settings should not be reused via _try_checkout."""
    with patch("s2_sdk._client.Connection") as MockConn:
        conn1 = _mock_connection()
        conn2 = _mock_connection()
        MockConn.side_effect = [conn1, conn2]

        # First checkout creates conn1 and reserves a stream on it.
        pc1, state1 = await pool.checkout("https://example.com")
        conn1._pending_streams.pop(id(state1), None)
        conn1.open_stream_count = 0

        # Clear settings after creation so _try_checkout skips conn1 reuse.
        conn1._settings_received.clear()

        # Next checkout should skip conn1 and create conn2
        pc2, _ = await pool.checkout("https://example.com")
        assert pc2._conn is conn2
    await pool.close()


@pytest.mark.asyncio
async def test_new_connection_with_zero_stream_capacity_raises(pool: ConnectionPool):
    with patch("s2_sdk._client.Connection") as MockConn:
        conn = _mock_connection(max_concurrent_streams=0)
        MockConn.return_value = conn

        with pytest.raises(ProtocolError, match="no available stream capacity"):
            await pool.checkout("https://example.com")

        conn.close.assert_awaited_once()
        assert pool._hosts["https://example.com"] == []
    await pool.close()


@pytest.mark.asyncio
async def test_new_connection_without_settings_proceeds_with_defaults():
    pool = ConnectionPool(connect_timeout=0.001)
    try:
        with patch("s2_sdk._client.Connection") as MockConn:
            conn = _mock_connection()
            conn._settings_received.clear()
            MockConn.return_value = conn

            pc, state = await pool.checkout("https://example.com")

            assert pc._conn is conn
            assert state is not None
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_unary_request_timeout_acks_and_resets_stream():
    state = _StreamState()
    state.unacked_flow_bytes = 11

    conn = AsyncMock()
    conn.send_headers = AsyncMock(return_value=1)
    conn.release_stream = MagicMock()
    conn.ack_data = AsyncMock()
    conn.reset_stream = AsyncMock()

    pc = MagicMock()
    pc._conn = conn
    pc.touch_idle = MagicMock()

    pool = MagicMock()
    pool.checkout = AsyncMock(return_value=(pc, state))

    client = HttpClient(
        pool=pool, base_url="https://example.com", request_timeout=0.001
    )

    with pytest.raises(ReadTimeoutError, match="Request timed out"):
        await client.unary_request("GET", "/v1/test")

    conn.ack_data.assert_awaited_once_with(1, 11)
    conn.reset_stream.assert_awaited_once_with(1)
    conn.release_stream.assert_called_once_with(1, state)
    pc.touch_idle.assert_called_once()


@pytest.mark.asyncio
async def test_send_headers_allocates_ids_from_h2_under_lock():
    from s2_sdk._client import Connection

    conn = Connection(
        host="localhost",
        port=443,
        ssl_context=None,
        connect_timeout=5.0,
    )
    conn._h2 = MagicMock()
    conn._h2.get_next_available_stream_id.side_effect = [1, 3]

    state1 = conn.reserve_stream()
    state2 = conn.reserve_stream()

    with patch.object(Connection, "_flush_h2_data_and_drain", new=AsyncMock()):
        stream1 = await conn.send_headers(state1, [(":method", "GET")], end_stream=True)
        stream2 = await conn.send_headers(state2, [(":method", "GET")], end_stream=True)

    assert stream1 == 1
    assert stream2 == 3
    assert conn.open_stream_count == 2


@pytest.mark.asyncio
async def test_reserve_stream_counts_pending_capacity():
    from s2_sdk._client import Connection

    conn = Connection(
        host="localhost",
        port=443,
        ssl_context=None,
        connect_timeout=5.0,
    )
    conn._h2 = MagicMock()
    conn._h2.remote_settings.max_concurrent_streams = 1
    conn._settings_received.set()

    pc = _PooledConnection(conn)
    assert pc.has_capacity is True

    state = conn.reserve_stream()

    assert conn.open_stream_count == 1
    assert pc.has_capacity is False

    conn.release_stream(None, state)
    assert conn.open_stream_count == 0


@pytest.mark.asyncio
async def test_reaper_keeps_one_idle_connection_per_host():
    pool = ConnectionPool(connect_timeout=5.0)
    try:
        conns = [_PooledConnection(_mock_connection()) for _ in range(3)]
        past = asyncio.get_running_loop().time() - (IDLE_TIMEOUT + 1)
        for pc in conns:
            pc._idle_since = past
        pool._hosts["https://example.com"] = conns

        async def _sleep(_: float) -> None:
            pool._closed = True

        with patch("s2_sdk._client.asyncio.sleep", new=_sleep):
            await pool._reap_idle()

        assert len(pool._hosts["https://example.com"]) == 1
    finally:
        pool._closed = True
        await pool.close()


@pytest.mark.asyncio
async def test_reaper_prunes_unavailable_connections():
    pool = ConnectionPool(connect_timeout=5.0)
    try:
        dead = _PooledConnection(_mock_connection(available=False))
        live = _PooledConnection(_mock_connection())
        live._idle_since = asyncio.get_running_loop().time()
        pool._hosts["https://example.com"] = [dead, live]

        async def _sleep(_: float) -> None:
            pool._closed = True

        with patch("s2_sdk._client.asyncio.sleep", new=_sleep):
            await pool._reap_idle()

        assert pool._hosts["https://example.com"] == [live]
        dead._conn.close.assert_awaited_once()  # type: ignore[attr-defined]
    finally:
        pool._closed = True
        await pool.close()


@pytest.mark.asyncio
async def test_reaper_does_not_close_connection_reacquired_mid_reap():
    pool = ConnectionPool(connect_timeout=5.0)
    try:
        conn1 = _mock_connection()
        conn2 = _mock_connection()
        pc1 = _PooledConnection(conn1)
        pc2 = _PooledConnection(conn2)
        past = asyncio.get_running_loop().time() - (IDLE_TIMEOUT + 1)
        pc1._idle_since = past
        pc2._idle_since = past
        pool._hosts["https://example.com"] = [pc1, pc2]

        async def _close_first() -> None:
            conn2.open_stream_count = 1

        conn1.close.side_effect = _close_first

        async def _sleep(_: float) -> None:
            pool._closed = True

        with patch("s2_sdk._client.asyncio.sleep", new=_sleep):
            await pool._reap_idle()

        assert pc2 in pool._hosts["https://example.com"]
        conn2.close.assert_not_awaited()
    finally:
        pool._closed = True
        await pool.close()


@pytest.mark.asyncio
async def test_reaper_handles_hosts_added_during_close():
    pool = ConnectionPool(connect_timeout=5.0)
    try:
        dead = _PooledConnection(_mock_connection(available=False))
        pool._hosts["https://example.com"] = [dead]

        async def _close_and_add_host() -> None:
            pool._hosts["https://other.example.com"] = []

        dead._conn.close.side_effect = _close_and_add_host  # type: ignore[attr-defined]

        async def _sleep(_: float) -> None:
            pool._closed = True

        with patch("s2_sdk._client.asyncio.sleep", new=_sleep):
            await pool._reap_idle()

        assert "https://example.com" not in pool._hosts
        assert "https://other.example.com" in pool._hosts
    finally:
        pool._closed = True
        await pool.close()


@pytest.mark.asyncio
async def test_reaper_prunes_empty_host_lists():
    pool = ConnectionPool(connect_timeout=5.0)
    try:
        dead = _PooledConnection(_mock_connection(available=False))
        pool._hosts["https://example.com"] = [dead]
        pool._host_locks["https://example.com"] = asyncio.Lock()

        async def _sleep(_: float) -> None:
            pool._closed = True

        with patch("s2_sdk._client.asyncio.sleep", new=_sleep):
            await pool._reap_idle()

        assert "https://example.com" not in pool._hosts
        assert "https://example.com" not in pool._host_locks
    finally:
        pool._closed = True
        await pool.close()


def test_build_headers_omits_accept_encoding_when_compression_disabled():
    pool = MagicMock()

    client = HttpClient(pool=pool, base_url="https://example.com", request_timeout=30.0)

    headers = client._build_headers("GET", "/v1/test")

    assert "accept-encoding" not in {key for key, _ in headers}


@pytest.mark.parametrize(
    ("compression", "encoding"),
    [
        (Compression.GZIP, "gzip"),
        (Compression.ZSTD, "zstd"),
    ],
)
def test_build_headers_sets_accept_encoding_from_compression(
    compression: Compression, encoding: str
):
    pool = MagicMock()

    client = HttpClient(
        pool=pool,
        base_url="https://example.com",
        request_timeout=30.0,
        compression=compression,
    )

    headers = client._build_headers("GET", "/v1/test")
    accept_encoding = [value for key, value in headers if key == "accept-encoding"]

    assert accept_encoding == [encoding]


@pytest.mark.asyncio
async def test_truncated_body_raises_error():
    """Receiving None sentinel with state.error set should raise."""
    from s2_sdk._client import StreamingResponse

    state = _StreamState()
    q = state.data_queue

    # Simulate partial data then error
    q.put_nowait(b"partial")
    error = ConnectionError("stream reset")
    state.error = error
    q.put_nowait(None)

    resp = StreamingResponse(
        status_code=200,
        data_queue=q,
        ended=state.ended,
        stream_state=state,
    )

    # aread should raise the error
    with pytest.raises(ConnectionError, match="stream reset"):
        await resp.aread()


@pytest.mark.asyncio
async def test_truncated_body_raises_in_aiter():
    """aiter_bytes should raise error on None sentinel with state.error."""
    from s2_sdk._client import StreamingResponse

    state = _StreamState()
    q = state.data_queue

    q.put_nowait(b"chunk1")
    error = ConnectionError("reset")
    state.error = error
    q.put_nowait(None)

    resp = StreamingResponse(
        status_code=200,
        data_queue=q,
        ended=state.ended,
        stream_state=state,
    )

    chunks = []
    with pytest.raises(ConnectionError, match="reset"):
        async for chunk in resp.aiter_bytes():
            chunks.append(chunk)
    assert chunks == [b"chunk1"]


@pytest.mark.asyncio
async def test_clean_eof_no_error():
    """Normal EOF (no error) should return data without raising."""
    from s2_sdk._client import StreamingResponse

    state = _StreamState()
    q = state.data_queue

    q.put_nowait(b"hello")
    q.put_nowait(None)
    state.ended.set()

    resp = StreamingResponse(
        status_code=200,
        data_queue=q,
        ended=state.ended,
        stream_state=state,
    )

    data = await resp.aread()
    assert data == b"hello"


@pytest.mark.asyncio
async def test_aread_acks_each_consumed_chunk():
    from s2_sdk._client import StreamingResponse

    state = _StreamState()
    q = state.data_queue
    q.put_nowait((b"a" * 20000, 20000))
    q.put_nowait((b"b" * 20000, 20000))
    q.put_nowait(None)
    state.unacked_flow_bytes = 40000

    ack = AsyncMock()
    resp = StreamingResponse(
        status_code=200,
        data_queue=q,
        ended=state.ended,
        stream_state=state,
        ack=ack,
    )

    data = await resp.aread()

    assert data == (b"a" * 20000) + (b"b" * 20000)
    assert state.unacked_flow_bytes == 0
    assert [call.args for call in ack.await_args_list] == [(20000,), (20000,)]


@pytest.mark.asyncio
async def test_aiter_acks_only_consumed_chunk_on_early_exit():
    from s2_sdk._client import StreamingResponse

    state = _StreamState()
    q = state.data_queue
    q.put_nowait((b"first", 40000))
    q.put_nowait((b"second", 7))
    state.unacked_flow_bytes = 40007

    ack = AsyncMock()
    resp = StreamingResponse(
        status_code=200,
        data_queue=q,
        ended=state.ended,
        stream_state=state,
        ack=ack,
    )

    iterator = resp.aiter_bytes()
    chunk = await anext(iterator)
    assert chunk == b"first"
    await iterator.aclose()

    assert state.unacked_flow_bytes == 7
    assert [call.args for call in ack.await_args_list] == [(40000,)]


@pytest.mark.asyncio
async def test_drain_body_resets_stream_on_generator_error():
    from s2_sdk._client import _drain_body

    async def _broken_body():
        yield b"first"
        raise RuntimeError("boom")

    conn = AsyncMock()

    with pytest.raises(RuntimeError, match="boom"):
        await _drain_body(conn, 1, _broken_body(), None)

    conn.send_data.assert_awaited_once_with(1, b"first", on_write=None)
    conn.reset_stream.assert_awaited_once_with(1)
    conn.end_stream.assert_not_called()


@pytest.mark.asyncio
async def test_close_fails_inflight_streams():
    from s2_sdk._client import Connection

    conn = Connection(
        host="localhost",
        port=443,
        ssl_context=None,
        connect_timeout=5.0,
    )
    state = _StreamState()
    conn._streams[1] = state

    await conn.close()

    assert isinstance(state.error, ConnectionClosedError)
    assert state.response_headers.done()
    assert state.window_updated.is_set()
    assert state.data_queue.get_nowait() is None


@pytest.mark.asyncio
async def test_fail_stream_is_idempotent_for_queue_sentinel():
    from s2_sdk._client import Connection

    conn = Connection(
        host="localhost",
        port=443,
        ssl_context=None,
        connect_timeout=5.0,
    )
    state = _StreamState()

    conn._fail_stream(state, ConnectionClosedError("first"))
    conn._fail_stream(state, ConnectionClosedError("second"))

    assert state.data_queue.qsize() == 1
    assert state.data_queue.get_nowait() is None


@pytest.mark.parametrize("status_code", [412, 416])
def test_raise_for_status_falls_back_to_text_for_non_json_special_status(
    status_code: int,
):
    response = Response(status_code, b"not-json")

    with pytest.raises(S2ServerError) as exc_info:
        _raise_for_status(response)

    assert exc_info.value.status_code == status_code
    assert exc_info.value.code == "unknown"
    assert str(exc_info.value) == "not-json"


@pytest.mark.asyncio
async def test_remote_settings_changed_wakes_waiters():
    import h2.events

    from s2_sdk._client import Connection

    conn = Connection(
        host="localhost",
        port=443,
        ssl_context=None,
        connect_timeout=5.0,
    )
    state = _StreamState()
    conn._streams[1] = state

    conn._handle_event(h2.events.RemoteSettingsChanged())

    assert conn._settings_received.is_set()
    assert state.window_updated.is_set()


def test_origin_rejects_path():
    from s2_sdk._client import _origin

    with pytest.raises(S2ClientError, match="origin without path or query"):
        _origin("https://example.com/api")


def test_origin_invalid_port_is_client_error():
    from s2_sdk._client import _origin

    with pytest.raises(S2ClientError, match="Invalid endpoint URL"):
        _origin("https://example.com:99999")
