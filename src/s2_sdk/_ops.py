import uuid
from collections.abc import AsyncIterator
from typing import Any, AsyncIterable, Self
from urllib.parse import quote

import s2_sdk._generated.s2.v1.s2_pb2 as pb
from s2_sdk import _types as types
from s2_sdk._append_session import AppendSession
from s2_sdk._client import ConnectionPool, HttpClient
from s2_sdk._exceptions import S2ServerError, fallible
from s2_sdk._mappers import (
    access_token_info_from_json,
    access_token_info_to_json,
    append_ack_from_proto,
    append_input_to_proto,
    basin_config_from_json,
    basin_config_to_json,
    basin_info_from_json,
    basin_reconfiguration_to_json,
    metric_set_from_json,
    read_batch_from_proto,
    read_limit_params,
    read_start_params,
    stream_config_from_json,
    stream_config_to_json,
    stream_info_from_json,
    stream_reconfiguration_to_json,
    tail_from_json,
)
from s2_sdk._producer import Producer
from s2_sdk._retrier import Retrier, http_retry_on, is_safe_to_retry_unary
from s2_sdk._s2s._read_session import run_read_session
from s2_sdk._types import ONE_MIB, Compression, Endpoints, Retry, Timeout, metered_bytes
from s2_sdk._validators import (
    validate_append_input,
    validate_basin,
    validate_batching,
    validate_max_unacked,
    validate_retry,
)


class S2:
    """Client for S2, an API for unlimited, durable, real-time streams.

    Works with both the `cloud <https://s2.dev/docs/intro>`_ and
    `open source, self-hosted <https://s2.dev/docs/s2-lite>`_ versions.

    Args:
        access_token: Access token for authenticating with S2.
        endpoints: S2 endpoints. If ``None``, defaults to public cloud
            endpoints. See :class:`Endpoints`.
        timeout: Timeout configuration. If ``None``, default values are
            used. See :class:`Timeout`.
        retry: Retry configuration. If ``None``, default values are
            used. See :class:`Retry`.
        compression: Compression algorithm for requests and responses.
            Defaults to ``NONE``. See :class:`Compression`.

    Tip:
        Use as an async context manager to ensure connections are closed::

            async with S2(token) as s2:
                ...

    Warning:
        If not using a context manager, call :meth:`close` when done.
    """

    __slots__ = (
        "_account_client",
        "_auth_header",
        "_basin_clients",
        "_compression",
        "_endpoints",
        "_pool",
        "_request_timeout",
        "_retry",
        "_retrier",
    )

    @fallible
    def __init__(
        self,
        access_token: str,
        *,
        endpoints: Endpoints | None = None,
        timeout: Timeout | None = None,
        retry: Retry | None = None,
        compression: Compression = Compression.NONE,
    ) -> None:
        if endpoints is None:
            endpoints = Endpoints.default()
        if timeout is None:
            timeout = Timeout()
        if retry is None:
            retry = Retry()
        validate_retry(retry.max_attempts)
        self._endpoints = endpoints
        self._retry = retry
        self._compression = compression
        self._auth_header = ("authorization", f"Bearer {access_token}")
        self._pool = ConnectionPool(
            connect_timeout=timeout.connection.total_seconds(),
        )
        self._request_timeout = timeout.request.total_seconds()
        self._account_client = HttpClient(
            pool=self._pool,
            base_url=endpoints._account_url(),
            request_timeout=self._request_timeout,
            headers={self._auth_header[0]: self._auth_header[1]},
            compression=compression,
        )
        self._basin_clients: dict[str, HttpClient] = {}
        self._retrier = Retrier(
            should_retry_on=http_retry_on,
            max_attempts=retry.max_attempts,
            min_base_delay=retry.min_base_delay.total_seconds(),
            max_base_delay=retry.max_base_delay.total_seconds(),
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool:
        await self.close()
        return False

    def __getitem__(self, name: str) -> "S2Basin":
        return self.basin(name)

    async def close(self) -> None:
        """Close all open connections to S2 service endpoints."""
        await self._pool.close()

    def _get_basin_client(self, name: str) -> HttpClient:
        if name not in self._basin_clients:
            headers = {self._auth_header[0]: self._auth_header[1]}
            if self._endpoints._is_direct_basin():
                headers["s2-basin"] = name
            self._basin_clients[name] = HttpClient(
                pool=self._pool,
                base_url=self._endpoints._basin_url(name),
                request_timeout=self._request_timeout,
                headers=headers,
                compression=self._compression,
            )
        return self._basin_clients[name]

    @fallible
    async def create_basin(
        self,
        name: str,
        *,
        config: types.BasinConfig | None = None,
    ) -> types.BasinInfo:
        """Create a basin.

        Args:
            name: Name of the basin.
            config: Configuration for the basin.

        Returns:
            Information about the created basin.

        Note:
            ``name`` must be globally unique, 8--48 characters, comprising lowercase
            letters, numbers, and hyphens. It cannot begin or end with a hyphen.
        """
        validate_basin(name)
        json: dict[str, Any] = {"basin": name}
        if config is not None:
            json["config"] = basin_config_to_json(config)

        response = await self._retrier(
            self._account_client.unary_request,
            "POST",
            "/v1/basins",
            json=json,
            headers={"s2-request-token": _s2_request_token()},
        )
        return basin_info_from_json(response.json())

    def basin(self, name: str) -> "S2Basin":
        """Get an :class:`S2Basin` for performing basin-level operations.

        Args:
            name: Name of the basin.

        Returns:
            An :class:`S2Basin` bound to the given basin name.

        Tip:
            Also available via subscript: ``s2["my-basin"]``.
        """
        validate_basin(name)
        return S2Basin(
            name,
            self._get_basin_client(name),
            retry=self._retry,
            compression=self._compression,
        )

    @fallible
    async def list_basins(
        self,
        *,
        prefix: str = "",
        start_after: str = "",
        limit: int = 1000,
    ) -> types.Page[types.BasinInfo]:
        """List a page of basins.

        Args:
            prefix: Filter to basins whose name starts with this prefix.
            start_after: List basins whose name is lexicographically after this value.
            limit: Maximum number of basins to return per page.

        Returns:
            A page of :class:`BasinInfo`.

        Tip:
            See :meth:`list_all_basins` for automatic pagination.
        """
        params: dict[str, Any] = {}
        if prefix:
            params["prefix"] = prefix
        if start_after:
            params["start_after"] = start_after
        if limit != 1000:
            params["limit"] = limit

        response = await self._retrier(
            self._account_client.unary_request, "GET", "/v1/basins", params=params
        )
        data = response.json()
        return types.Page(
            items=[basin_info_from_json(b) for b in data["basins"]],
            has_more=data["has_more"],
        )

    @fallible
    async def list_all_basins(
        self,
        *,
        prefix: str = "",
        start_after: str = "",
        include_deleted: bool = False,
    ) -> AsyncIterator[types.BasinInfo]:
        """List all basins, paginating automatically.

        Args:
            prefix: Filter to basins whose name starts with this prefix.
            start_after: List basins whose name is lexicographically after this value.
            include_deleted: Include basins that are being deleted.

        Yields:
            :class:`BasinInfo` for each basin.
        """
        while True:
            page = await self.list_basins(prefix=prefix, start_after=start_after)
            for info in page.items:
                if not include_deleted and info.deleted_at is not None:
                    continue
                yield info
            if not page.has_more or not page.items:
                break
            start_after = page.items[-1].name

    @fallible
    async def delete_basin(self, name: str, *, ignore_not_found: bool = False) -> None:
        """Delete a basin.

        Args:
            name: Name of the basin to delete.
            ignore_not_found: If ``True``, do not raise on 404.

        Note:
            Basin deletion is asynchronous and may take several minutes to complete.
        """
        await _maybe_not_found(
            self._retrier(
                self._account_client.unary_request, "DELETE", f"/v1/basins/{name}"
            ),
            ignore=ignore_not_found,
        )

    @fallible
    async def get_basin_config(self, name: str) -> types.BasinConfig:
        """Get basin configuration.

        Args:
            name: Name of the basin.

        Returns:
            Current configuration of the basin.
        """
        response = await self._retrier(
            self._account_client.unary_request, "GET", f"/v1/basins/{name}"
        )
        return basin_config_from_json(response.json())

    @fallible
    async def reconfigure_basin(
        self,
        name: str,
        *,
        config: types.BasinConfig,
    ) -> types.BasinConfig:
        """Reconfigure a basin.

        Args:
            name: Name of the basin.
            config: New configuration. Only provided fields are updated.

        Returns:
            Updated basin configuration.

        Note:
            Modifying ``default_stream_config`` only affects newly created streams.
        """
        json = basin_reconfiguration_to_json(config)
        response = await self._retrier(
            self._account_client.unary_request,
            "PATCH",
            f"/v1/basins/{name}",
            json=json,
        )
        return basin_config_from_json(response.json())

    @fallible
    async def issue_access_token(
        self,
        id: str,
        *,
        scope: types.AccessTokenScope,
        expires_at: str | None = None,
        auto_prefix_streams: bool = False,
    ) -> str:
        """Issue an access token.

        Args:
            id: Unique identifier for the token (1--96 bytes).
            scope: Permissions scope for the token.
            expires_at: Optional expiration time (RFC 3339).
            auto_prefix_streams: Automatically prefix stream names during
                creation and strip the prefix during listing.

        Returns:
            The access token string.
        """
        json = access_token_info_to_json(id, scope, auto_prefix_streams, expires_at)
        response = await self._retrier(
            self._account_client.unary_request,
            "POST",
            "/v1/access-tokens",
            json=json,
        )
        return response.json()["access_token"]

    @fallible
    async def list_access_tokens(
        self,
        *,
        prefix: str = "",
        start_after: str = "",
        limit: int = 1000,
    ) -> types.Page[types.AccessTokenInfo]:
        """List a page of access tokens.

        Args:
            prefix: Filter to tokens whose ID starts with this prefix.
            start_after: List tokens whose ID is lexicographically after this value.
            limit: Maximum number of tokens to return per page.

        Returns:
            A page of :class:`AccessTokenInfo`.

        Tip:
            See :meth:`list_all_access_tokens` for automatic pagination.
        """
        params: dict[str, Any] = {}
        if prefix:
            params["prefix"] = prefix
        if start_after:
            params["start_after"] = start_after
        if limit != 1000:
            params["limit"] = limit

        response = await self._retrier(
            self._account_client.unary_request,
            "GET",
            "/v1/access-tokens",
            params=params,
        )
        data = response.json()
        return types.Page(
            items=[access_token_info_from_json(info) for info in data["access_tokens"]],
            has_more=data["has_more"],
        )

    @fallible
    async def list_all_access_tokens(
        self,
        *,
        prefix: str = "",
        start_after: str = "",
    ) -> AsyncIterator[types.AccessTokenInfo]:
        """List all access tokens, paginating automatically.

        Args:
            prefix: Filter to tokens whose ID starts with this prefix.
            start_after: List tokens whose ID is lexicographically after this value.

        Yields:
            :class:`AccessTokenInfo` for each token.
        """
        while True:
            page = await self.list_access_tokens(prefix=prefix, start_after=start_after)
            for info in page.items:
                yield info
            if not page.has_more or not page.items:
                break
            start_after = page.items[-1].id

    @fallible
    async def revoke_access_token(self, id: str) -> None:
        """Revoke an access token.

        Args:
            id: Identifier of the token to revoke.
        """
        await self._retrier(
            self._account_client.unary_request, "DELETE", _access_token_path(id)
        )

    @fallible
    async def account_metrics(
        self,
        *,
        set: types.AccountMetricSet,
        start: int | None = None,
        end: int | None = None,
        interval: types.TimeseriesInterval | None = None,
    ) -> list[types.Scalar | types.Accumulation | types.Gauge | types.Label]:
        """Get account metrics.

        Args:
            set: Metric set to query.
            start: Start of the time range (epoch seconds).
            end: End of the time range (epoch seconds).
            interval: Accumulation interval for timeseries metrics.

        Returns:
            List of metric values.
        """
        response = await self._retrier(
            self._account_client.unary_request,
            "GET",
            "/v1/metrics",
            params=_metrics_params(set.value, start, end, interval),
        )
        return metric_set_from_json(response.json())

    @fallible
    async def basin_metrics(
        self,
        basin: str,
        *,
        set: types.BasinMetricSet,
        start: int | None = None,
        end: int | None = None,
        interval: types.TimeseriesInterval | None = None,
    ) -> list[types.Scalar | types.Accumulation | types.Gauge | types.Label]:
        """Get basin metrics.

        Args:
            basin: Name of the basin.
            set: Metric set to query.
            start: Start of the time range (epoch seconds).
            end: End of the time range (epoch seconds).
            interval: Accumulation interval for timeseries metrics.

        Returns:
            List of metric values.
        """
        response = await self._retrier(
            self._account_client.unary_request,
            "GET",
            f"/v1/metrics/{_encode_path_segment(basin)}",
            params=_metrics_params(set.value, start, end, interval),
        )
        return metric_set_from_json(response.json())

    @fallible
    async def stream_metrics(
        self,
        basin: str,
        stream: str,
        *,
        set: types.StreamMetricSet,
        start: int | None = None,
        end: int | None = None,
        interval: types.TimeseriesInterval | None = None,
    ) -> list[types.Scalar | types.Accumulation | types.Gauge | types.Label]:
        """Get stream metrics.

        Args:
            basin: Name of the basin.
            stream: Name of the stream.
            set: Metric set to query.
            start: Start of the time range (epoch seconds).
            end: End of the time range (epoch seconds).
            interval: Accumulation interval for timeseries metrics.

        Returns:
            List of metric values.
        """
        response = await self._retrier(
            self._account_client.unary_request,
            "GET",
            (
                f"/v1/metrics/{_encode_path_segment(basin)}"
                f"/{_encode_path_segment(stream)}"
            ),
            params=_metrics_params(set.value, start, end, interval),
        )
        return metric_set_from_json(response.json())


class S2Basin:
    """
    Caution:
        Returned by :meth:`S2.basin`. Do not instantiate directly.
    """

    __slots__ = (
        "_name",
        "_client",
        "_compression",
        "_retry",
        "_retrier",
    )

    @fallible
    def __init__(
        self,
        name: str,
        client: HttpClient,
        *,
        retry: Retry,
        compression: Compression,
    ) -> None:
        self._name = name
        self._client = client
        self._retry = retry
        self._compression = compression
        self._retrier = Retrier(
            should_retry_on=http_retry_on,
            max_attempts=retry.max_attempts,
            min_base_delay=retry.min_base_delay.total_seconds(),
            max_base_delay=retry.max_base_delay.total_seconds(),
        )

    def __repr__(self) -> str:
        return f"S2Basin(name={self.name})"

    def __getitem__(self, name: str) -> "S2Stream":
        return self.stream(name)

    @property
    def name(self) -> str:
        """Basin name."""
        return self._name

    @fallible
    async def create_stream(
        self,
        name: str,
        *,
        config: types.StreamConfig | None = None,
    ) -> types.StreamInfo:
        """Create a stream.

        Args:
            name: Name of the stream.
            config: Configuration for the stream.

        Returns:
            Information about the created stream.

        Note:
            ``name`` must be unique within the basin. It can be an arbitrary string
            up to 512 characters. ``/`` is recommended as a delimiter for
            hierarchical naming.
        """
        json: dict[str, Any] = {"stream": name}
        if config is not None:
            json["config"] = stream_config_to_json(config)

        response = await self._retrier(
            self._client.unary_request,
            "POST",
            "/v1/streams",
            json=json,
            headers={"s2-request-token": _s2_request_token()},
        )
        return stream_info_from_json(response.json())

    def stream(self, name: str) -> "S2Stream":
        """Get an :class:`S2Stream` for performing stream-level operations.

        Args:
            name: Name of the stream.

        Returns:
            An :class:`S2Stream` bound to the given stream name.

        Tip:
            Also available via subscript: ``s2["my-basin"]["my-stream"]``.
        """
        return S2Stream(
            name,
            self._client,
            retry=self._retry,
            compression=self._compression,
        )

    @fallible
    async def list_streams(
        self,
        *,
        prefix: str = "",
        start_after: str = "",
        limit: int = 1000,
    ) -> types.Page[types.StreamInfo]:
        """List a page of streams.

        Args:
            prefix: Filter to streams whose name starts with this prefix.
            start_after: List streams whose name is lexicographically after this value.
            limit: Maximum number of streams to return per page.

        Returns:
            A page of :class:`StreamInfo`.

        Tip:
            See :meth:`list_all_streams` for automatic pagination.
        """
        params: dict[str, Any] = {}
        if prefix:
            params["prefix"] = prefix
        if start_after:
            params["start_after"] = start_after
        if limit != 1000:
            params["limit"] = limit

        response = await self._retrier(
            self._client.unary_request, "GET", "/v1/streams", params=params
        )
        data = response.json()
        return types.Page(
            items=[stream_info_from_json(s) for s in data["streams"]],
            has_more=data["has_more"],
        )

    @fallible
    async def list_all_streams(
        self,
        *,
        prefix: str = "",
        start_after: str = "",
        include_deleted: bool = False,
    ) -> AsyncIterator[types.StreamInfo]:
        """List all streams, paginating automatically.

        Args:
            prefix: Filter to streams whose name starts with this prefix.
            start_after: List streams whose name is lexicographically after this value.
            include_deleted: Include streams that are being deleted.

        Yields:
            :class:`StreamInfo` for each stream.
        """
        while True:
            page = await self.list_streams(prefix=prefix, start_after=start_after)
            for info in page.items:
                if not include_deleted and info.deleted_at is not None:
                    continue
                yield info
            if not page.has_more or not page.items:
                break
            start_after = page.items[-1].name

    @fallible
    async def delete_stream(self, name: str, *, ignore_not_found: bool = False) -> None:
        """Delete a stream.

        Args:
            name: Name of the stream to delete.
            ignore_not_found: If ``True``, do not raise on 404.

        Note:
            Stream deletion is asynchronous and may take several minutes to complete.
        """
        await _maybe_not_found(
            self._retrier(self._client.unary_request, "DELETE", _stream_path(name)),
            ignore=ignore_not_found,
        )

    @fallible
    async def get_stream_config(self, name: str) -> types.StreamConfig:
        """Get stream configuration.

        Args:
            name: Name of the stream.

        Returns:
            Current configuration of the stream.
        """
        response = await self._retrier(
            self._client.unary_request, "GET", _stream_path(name)
        )
        return stream_config_from_json(response.json())

    @fallible
    async def reconfigure_stream(
        self,
        name: str,
        *,
        config: types.StreamConfig,
    ) -> types.StreamConfig:
        """Reconfigure a stream.

        Args:
            name: Name of the stream.
            config: New configuration. Only provided fields are updated.

        Returns:
            Updated stream configuration.
        """
        json = stream_reconfiguration_to_json(config)
        response = await self._retrier(
            self._client.unary_request, "PATCH", _stream_path(name), json=json
        )
        return stream_config_from_json(response.json())


class S2Stream:
    """
    Caution:
        Returned by :meth:`S2Basin.stream`. Do not instantiate directly.
    """

    __slots__ = (
        "_name",
        "_client",
        "_compression",
        "_retry",
        "_retrier",
        "_append_retrier",
    )

    def __init__(
        self,
        name: str,
        client: HttpClient,
        *,
        retry: Retry,
        compression: Compression,
    ) -> None:
        self._name = name
        self._client = client
        self._retry = retry
        self._compression = compression
        self._retrier = Retrier(
            should_retry_on=http_retry_on,
            max_attempts=retry.max_attempts,
            min_base_delay=retry.min_base_delay.total_seconds(),
            max_base_delay=retry.max_base_delay.total_seconds(),
        )
        self._append_retrier = Retrier(
            should_retry_on=lambda e: is_safe_to_retry_unary(
                e, retry.append_retry_policy
            ),
            max_attempts=retry.max_attempts,
            min_base_delay=retry.min_base_delay.total_seconds(),
            max_base_delay=retry.max_base_delay.total_seconds(),
        )

    def __repr__(self) -> str:
        return f"S2Stream(name={self.name})"

    @property
    def name(self) -> str:
        """Stream name."""
        return self._name

    @fallible
    async def check_tail(self) -> types.StreamPosition:
        """Check the tail of a stream.

        Returns:
            The tail position — the next sequence number to be assigned and the
            timestamp of the last record on the stream.
        """
        response = await self._retrier(
            self._client.unary_request,
            "GET",
            _stream_path(self.name, "/records/tail"),
        )
        return tail_from_json(response.json())

    @fallible
    async def append(self, inp: types.AppendInput) -> types.AppendAck:
        """Append a batch of records to a stream.

        Args:
            inp: Batch of records and optional conditions.

        Returns:
            Acknowledgement with assigned sequence numbers and tail position.
        """
        validate_append_input(len(inp.records), metered_bytes(inp.records))
        proto = append_input_to_proto(inp)
        body = proto.SerializeToString()

        response = await self._append_retrier(
            self._client.unary_request,
            "POST",
            _stream_path(self.name, "/records"),
            content=body,
            headers={
                "content-type": "application/x-protobuf",
                "accept": "application/x-protobuf",
            },
        )
        ack = pb.AppendAck()
        ack.ParseFromString(response.content)
        return append_ack_from_proto(ack)

    def append_session(
        self,
        *,
        max_unacked_bytes: int = 5 * ONE_MIB,
        max_unacked_batches: int | None = None,
    ) -> AppendSession:
        """Open a session for appending batches of records continuously.

        Pipelined inputs are guaranteed to be processed in order.

        Args:
            max_unacked_bytes: Maximum total metered bytes of unacknowledged
                batches before backpressure is applied. Default is 5 MiB.
            max_unacked_batches: Maximum number of unacknowledged batches
                before backpressure is applied. If ``None``, no limit is applied.

        Returns:
            An :class:`AppendSession` to use as an async context manager.

        Tip:
            Use as an async context manager::

                async with stream.append_session() as session:
                    ticket = await session.submit(AppendInput(records=[...]))
                    ack = await ticket

        Warning:
            If not using a context manager, call :meth:`AppendSession.close` to
            ensure all submitted batches are appended.
        """
        validate_max_unacked(max_unacked_bytes, max_unacked_batches)
        return AppendSession(
            client=self._client,
            stream_name=self.name,
            retry=self._retry,
            compression=self._compression,
            max_unacked_bytes=max_unacked_bytes,
            max_unacked_batches=max_unacked_batches,
        )

    def producer(
        self,
        *,
        fencing_token: str | None = None,
        match_seq_num: int | None = None,
        batching: types.Batching | None = None,
        max_unacked_bytes: int = 5 * ONE_MIB,
    ) -> Producer:
        """Open a producer with per-record submit and auto-batching.

        Args:
            fencing_token: Fencing token applied to every batch.
            match_seq_num: Expected sequence number for the first record.
                Automatically advanced after each acknowledgement.
            batching: Auto-batching configuration. If ``None``, default
                values are used. See :class:`Batching`.
            max_unacked_bytes: Maximum total metered bytes of unacknowledged
                batches before backpressure is applied. Default is 5 MiB.

        Returns:
            A :class:`Producer` to use as an async context manager.

        Tip:
            Use as an async context manager::

                async with stream.producer() as p:
                    ticket = await p.submit(Record(body=b"hello"))
                    ack = await ticket

        Warning:
            If not using a context manager, call :meth:`Producer.close` to
            ensure all submitted records are appended.
        """
        if batching is None:
            batching = types.Batching()
        validate_max_unacked(max_unacked_bytes)
        validate_batching(batching.max_records, batching.max_bytes)
        return Producer(
            client=self._client,
            stream_name=self.name,
            retry=self._retry,
            compression=self._compression,
            fencing_token=fencing_token,
            match_seq_num=match_seq_num,
            max_unacked_bytes=max_unacked_bytes,
            batching=batching,
        )

    @fallible
    async def read(
        self,
        *,
        start: types.SeqNum | types.Timestamp | types.TailOffset,
        limit: types.ReadLimit | None = None,
        until_timestamp: int | None = None,
        clamp_to_tail: bool = False,
        wait: int | None = None,
        ignore_command_records: bool = False,
    ) -> types.ReadBatch:
        """Read a batch of records from a stream.

        Args:
            start: Inclusive start position.
            limit: Maximum number of records or metered bytes to return.
            until_timestamp: Exclusive upper-bound timestamp. All returned records
                are guaranteed to have timestamps less than this value.
            clamp_to_tail: Clamp the start position to the tail when it
                exceeds the tail, instead of raising.
            wait: Number of seconds to wait for records before returning.
            ignore_command_records: Filter out command records from the batch.

        Returns:
            A :class:`ReadBatch` containing sequenced records and an optional
            tail position. Records can be empty only if ``limit``,
            ``until_timestamp``, or ``wait`` were provided.
        """
        params: dict[str, Any] = {}
        params.update(read_start_params(start))
        params.update(read_limit_params(limit))
        if until_timestamp is not None:
            params["until"] = until_timestamp
        if clamp_to_tail:
            params["clamp"] = "true"
        if wait is not None:
            params["wait"] = wait

        response = await self._retrier(
            self._client.unary_request,
            "GET",
            _stream_path(self.name, "/records"),
            params=params,
            headers={"accept": "application/x-protobuf"},
        )

        proto_batch = pb.ReadBatch()
        proto_batch.ParseFromString(response.content)
        return read_batch_from_proto(proto_batch, ignore_command_records)

    @fallible
    async def read_session(
        self,
        *,
        start: types.SeqNum | types.Timestamp | types.TailOffset,
        limit: types.ReadLimit | None = None,
        until_timestamp: int | None = None,
        clamp_to_tail: bool = False,
        wait: int | None = None,
        ignore_command_records: bool = False,
    ) -> AsyncIterable[types.ReadBatch]:
        """Read batches of records from a stream continuously.

        Args:
            start: Inclusive start position.
            limit: Maximum number of records or metered bytes to return across
                the entire session.
            until_timestamp: Exclusive upper-bound timestamp. All returned records
                are guaranteed to have timestamps less than this value.
            clamp_to_tail: Clamp the start position to the tail when it
                exceeds the tail, instead of raising.
            wait: Number of seconds to wait for new records when the tail is
                reached.
            ignore_command_records: Filter out command records from batches.

        Yields:
            :class:`ReadBatch` — each containing a batch of records and an
            optional tail position.

        Note:
            Sessions without bounds (no ``limit`` or ``until_timestamp``) default
            to infinite ``wait``, waiting for new records indefinitely. Sessions with bounds default to zero ``wait``, ending
            when the bounds are met or the tail is reached. Setting a non-zero
            ``wait`` makes a bounded session wait up to that many seconds for
            new records before ending.
        """
        async for batch in run_read_session(
            self._client,
            self.name,
            start,
            limit,
            until_timestamp,
            clamp_to_tail,
            wait,
            ignore_command_records,
            retry=self._retry,
        ):
            yield batch


def _s2_request_token() -> str:
    return uuid.uuid4().hex


def _encode_path_segment(value: str) -> str:
    return quote(value, safe="")


def _stream_path(name: str, suffix: str = "") -> str:
    return f"/v1/streams/{_encode_path_segment(name)}{suffix}"


def _access_token_path(id: str) -> str:
    return f"/v1/access-tokens/{_encode_path_segment(id)}"


def _metrics_params(
    set_value: str,
    start: int | None,
    end: int | None,
    interval: types.TimeseriesInterval | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"set": set_value}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    if interval is not None:
        params["interval"] = interval.value
    return params


async def _maybe_not_found(coro, *, ignore: bool) -> None:
    try:
        await coro
    except S2ServerError as e:
        if ignore and e.status_code == 404:
            return
        raise
