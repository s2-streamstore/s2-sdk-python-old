from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Generic, Iterable, Literal, TypeVar

from s2_sdk._exceptions import S2ClientError, fallible

T = TypeVar("T")

ONE_MIB = 1024 * 1024


def _parse_scheme(url: str) -> str:
    idx = url.find("://")
    if idx >= 0:
        return url[:idx].lower()
    return "https"


class _DocEnum(Enum):
    def __new__(cls, value, doc=None):
        self = object.__new__(cls)
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self

    def __repr__(self) -> str:
        return f"{type(self).__name__}.{self.name}"


class Compression(_DocEnum):
    """Compression algorithm for requests and responses."""

    NONE = "none"
    ZSTD = "zstd"
    GZIP = "gzip"


class AppendRetryPolicy(_DocEnum):
    """Policy controlling when append operations are retried."""

    ALL = "all", "Retry all retryable errors."
    NO_SIDE_EFFECTS = (
        "no-side-effects",
        "Retry only when no server-side mutation could have occurred.",
    )


class Endpoints:
    """S2 service endpoints. See `endpoints <https://s2.dev/docs/api/endpoints>`_."""

    __slots__ = ("_account", "_basin", "_direct")

    def __init__(self, account: str, basin: str):
        account_scheme = _parse_scheme(account)
        basin_scheme = _parse_scheme(basin)
        if account_scheme != basin_scheme:
            raise S2ClientError("Account and basin endpoints must have the same scheme")
        self._account = account
        self._basin = basin
        self._direct = "{basin}" not in basin

    @classmethod
    def default(cls) -> Endpoints:
        """Construct default S2 cloud endpoints."""
        return cls(
            account="https://aws.s2.dev",
            basin="https://{basin}.b.s2.dev",
        )

    @classmethod
    @fallible
    def from_env(cls) -> Endpoints:
        """Construct endpoints from ``S2_ACCOUNT_ENDPOINT`` and ``S2_BASIN_ENDPOINT`` environment variables."""
        account = os.getenv("S2_ACCOUNT_ENDPOINT")
        basin = os.getenv("S2_BASIN_ENDPOINT")
        if account and basin:
            return cls(account=account, basin=basin)
        raise S2ClientError(
            "Both S2_ACCOUNT_ENDPOINT and S2_BASIN_ENDPOINT must be set"
        )

    def _account_url(self) -> str:
        return self._account

    def _basin_url(self, basin_name: str) -> str:
        return self._basin.format(basin=basin_name)

    def _is_direct_basin(self) -> bool:
        return self._direct


@dataclass(slots=True)
class Timeout:
    """Timeout configuration."""

    request: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    """Timeout for read, write, and pool operations. Default is 5 seconds."""

    connection: timedelta = field(default_factory=lambda: timedelta(seconds=3))
    """Timeout for establishing connections. Default is 3 seconds."""


@dataclass(slots=True)
class Retry:
    """Retry configuration."""

    max_attempts: int = 3
    """Maximum number of attempts, including the initial try. Must be at least 1. Default is 3."""

    min_base_delay: timedelta = field(
        default_factory=lambda: timedelta(milliseconds=100)
    )
    """Minimum base delay between retries, before jitter. Default is 100 ms."""

    max_base_delay: timedelta = field(default_factory=lambda: timedelta(seconds=1))
    """Maximum base delay between retries, before jitter. Default is 1 second."""

    append_retry_policy: AppendRetryPolicy = AppendRetryPolicy.ALL
    """Policy controlling when append operations are retried. Default is ``ALL``."""

    def _max_retries(self) -> int:
        return max(self.max_attempts - 1, 0)


@dataclass(slots=True)
class Record:
    """A record to append."""

    body: bytes
    """Body of the record."""

    headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    """Headers for the record."""

    timestamp: int | None = None
    """Timestamp for the record. Precise semantics depend on the stream's timestamping
    configuration."""


@dataclass(slots=True)
class AppendInput:
    """Input for :meth:`~S2Stream.append` and :meth:`AppendSession.submit`."""

    records: list[Record]
    """Batch of records to append atomically. Must contain 1--1000 records totalling at most
    1 MiB in metered bytes."""

    match_seq_num: int | None = None
    """Expected sequence number for the first record in the batch. If unset, no matching is
    performed. If set and mismatched, the append fails."""

    fencing_token: str | None = None
    """Fencing token to match against the stream's current fencing token. If unset, no matching
    is performed. If set and mismatched, the append fails."""


@dataclass(slots=True)
class StreamPosition:
    """Position of a record in a stream."""

    seq_num: int
    """Sequence number of the record."""

    timestamp: int
    """Timestamp of the record."""


@dataclass(slots=True)
class AppendAck:
    """Acknowledgement for an :class:`AppendInput`."""

    start: StreamPosition
    """Sequence number and timestamp of the first appended record."""

    end: StreamPosition
    """Sequence number of the last appended record + 1, and timestamp of the last appended
    record. ``end.seq_num - start.seq_num`` is the number of records appended."""

    tail: StreamPosition
    """Next sequence number to be assigned on the stream, and timestamp of the last record
    on the stream. Can be greater than ``end`` in case of concurrent appends."""


@dataclass(slots=True)
class IndexedAppendAck:
    """Acknowledgement for an appended record."""

    seq_num: int
    """Sequence number assigned to the record."""

    batch: AppendAck
    """Acknowledgement for the containing batch."""


@dataclass(slots=True)
class Batching:
    """Configuration for auto-batching records."""

    max_records: int = 1000
    """Maximum number of records per batch. Must be between 1 and 1000. Default is 1000."""

    max_bytes: int = ONE_MIB
    """Maximum metered bytes per batch. Must be between 8 and 1 MiB. Default is 1 MiB."""

    linger: timedelta = field(default_factory=lambda: timedelta(milliseconds=5))
    """Maximum time to wait for more records before flushing a partial batch. Default is 5 ms."""


@dataclass(slots=True)
class ReadLimit:
    """Limits for read operations."""

    count: int | None = None
    """Maximum number of records to return."""

    bytes: int | None = None
    """Maximum cumulative size of records calculated using :func:`metered_bytes`."""


@dataclass(slots=True)
class SequencedRecord:
    """Record read from a stream."""

    seq_num: int
    """Sequence number assigned to this record."""

    body: bytes
    """Body of this record."""

    headers: list[tuple[bytes, bytes]]
    """Series of name-value pairs for this record."""

    timestamp: int
    """Timestamp for this record."""


@dataclass(slots=True)
class ReadBatch:
    """Batch of records from a read session."""

    records: list[SequencedRecord]
    """Records that are durably sequenced on the stream."""

    tail: StreamPosition | None = None
    """Tail position of the stream, present when reading recent records."""


class CommandRecord:
    """Factory class for creating command records."""

    FENCE = b"fence"
    TRIM = b"trim"

    @staticmethod
    def fence(token: str) -> Record:
        """Create a fence command record.

        The fencing token must not exceed 36 bytes when UTF-8 encoded.
        """
        encoded_token = token.encode()
        if len(encoded_token) > 36:
            raise S2ClientError("UTF-8 byte count of fencing token exceeds 36 bytes")
        return Record(body=encoded_token, headers=[(bytes(), CommandRecord.FENCE)])

    @staticmethod
    def trim(desired_first_seq_num: int) -> Record:
        """Create a trim command record.

        Has no effect if the sequence number is smaller than the first existing record.
        """
        return Record(
            body=desired_first_seq_num.to_bytes(8, "big"),
            headers=[(bytes(), CommandRecord.TRIM)],
        )


def metered_bytes(records: Iterable[Record | SequencedRecord]) -> int:
    """Each record is metered using the following formula:

    .. code-block:: python

        8 + 2 * len(headers)
        + sum((len(name) + len(value)) for (name, value) in headers)
        + len(body)

    """
    return sum(
        (
            8
            + 2 * len(record.headers)
            + sum((len(name) + len(value)) for (name, value) in record.headers)
            + len(record.body)
        )
        for record in records
    )


@dataclass(slots=True)
class SeqNum:
    """Read starting from this sequence number."""

    value: int


@dataclass(slots=True)
class Timestamp:
    """Read starting from this timestamp."""

    value: int


@dataclass(slots=True)
class TailOffset:
    """Read starting from this many records before the tail."""

    value: int


@dataclass(slots=True)
class Page(Generic[T]):
    """A page of values."""

    items: list[T]
    """Items in this page."""

    has_more: bool
    """Whether there are more pages."""


class StorageClass(_DocEnum):
    """Storage class for recent appends."""

    STANDARD = "standard", "Offers end-to-end latencies under 500 ms."
    EXPRESS = "express", "Offers end-to-end latencies under 50 ms."


class TimestampingMode(_DocEnum):
    """Timestamping mode for appends. Timestamps are milliseconds since Unix epoch."""

    CLIENT_PREFER = (
        "client-prefer",
        "Prefer client-specified timestamp if present, otherwise use arrival time.",
    )
    CLIENT_REQUIRE = (
        "client-require",
        "Require a client-specified timestamp and reject if absent.",
    )
    ARRIVAL = (
        "arrival",
        "Use the arrival time and ignore any client-specified timestamp.",
    )


class BasinScope(_DocEnum):
    """Scope of a basin."""

    AWS_US_EAST_1 = "aws:us-east-1", "AWS us-east-1 region."


class Operation(_DocEnum):
    """Granular operation for access token scoping."""

    LIST_BASINS = "list-basins"
    CREATE_BASIN = "create-basin"
    DELETE_BASIN = "delete-basin"
    RECONFIGURE_BASIN = "reconfigure-basin"
    GET_BASIN_CONFIG = "get-basin-config"
    ISSUE_ACCESS_TOKEN = "issue-access-token"
    REVOKE_ACCESS_TOKEN = "revoke-access-token"
    LIST_ACCESS_TOKENS = "list-access-tokens"
    LIST_STREAMS = "list-streams"
    CREATE_STREAM = "create-stream"
    DELETE_STREAM = "delete-stream"
    GET_STREAM_CONFIG = "get-stream-config"
    RECONFIGURE_STREAM = "reconfigure-stream"
    CHECK_TAIL = "check-tail"
    APPEND = "append"
    READ = "read"
    TRIM = "trim"
    FENCE = "fence"
    ACCOUNT_METRICS = "account-metrics"
    BASIN_METRICS = "basin-metrics"
    STREAM_METRICS = "stream-metrics"


class Permission(_DocEnum):
    """Permission level for operation groups."""

    READ = "read"
    WRITE = "write"
    READ_WRITE = "read-write"


class MetricUnit(_DocEnum):
    """Unit of a metric value."""

    BYTES = "bytes"
    OPERATIONS = "operations"


class TimeseriesInterval(_DocEnum):
    """Bucket interval for timeseries metrics."""

    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class AccountMetricSet(_DocEnum):
    """Available account-level metric sets."""

    ACTIVE_BASINS = "active-basins"
    ACCOUNT_OPS = "account-ops"


class BasinMetricSet(_DocEnum):
    """Available basin-level metric sets."""

    STORAGE = "storage"
    APPEND_OPS = "append-ops"
    READ_OPS = "read-ops"
    READ_THROUGHPUT = "read-throughput"
    APPEND_THROUGHPUT = "append-throughput"
    BASIN_OPS = "basin-ops"


class StreamMetricSet(_DocEnum):
    """Available stream-level metric sets."""

    STORAGE = "storage"


@dataclass(slots=True)
class Timestamping:
    """Timestamping behavior for appends."""

    mode: TimestampingMode | None = None
    """Timestamping mode. Defaults to ``CLIENT_PREFER``."""

    uncapped: bool | None = None
    """Allow client-specified timestamps to exceed the arrival time."""


@dataclass(slots=True)
class StreamConfig:
    """Stream configuration."""

    storage_class: StorageClass | None = None
    """Storage class for recent appends. Defaults to ``EXPRESS``."""

    retention_policy: int | Literal["infinite"] | None = None
    """Retention duration in seconds, or ``"infinite"``. Default is 7 days."""

    timestamping: Timestamping | None = None
    """Timestamping behavior for appends."""

    delete_on_empty_min_age: int | None = None
    """Minimum age in seconds before this stream can be automatically deleted if empty."""


@dataclass(slots=True)
class BasinConfig:
    """Basin configuration."""

    default_stream_config: StreamConfig | None = None
    """Default configuration for streams in this basin."""

    create_stream_on_append: bool | None = None
    """Create stream on append if it doesn't exist."""

    create_stream_on_read: bool | None = None
    """Create stream on read if it doesn't exist."""


@dataclass(slots=True)
class BasinInfo:
    """Basin information."""

    name: str
    """Basin name."""

    scope: BasinScope | None
    """Scope of the basin. ``None`` for self-hosted (S2-Lite) basins."""

    created_at: datetime
    """Creation time."""

    deleted_at: datetime | None
    """Deletion time if the basin is being deleted."""


@dataclass(slots=True)
class StreamInfo:
    """Stream information."""

    name: str
    """Stream name."""

    created_at: datetime
    """Creation time."""

    deleted_at: datetime | None
    """Deletion time if the stream is being deleted."""


@dataclass(slots=True)
class ExactMatch:
    """Match only the resource with this exact name."""

    value: str


@dataclass(slots=True)
class PrefixMatch:
    """Match all resources that start with this prefix."""

    value: str


@dataclass(slots=True)
class OperationGroupPermissions:
    """Permissions at the operation group level."""

    account: Permission | None = None
    """Permission for account operations."""

    basin: Permission | None = None
    """Permission for basin operations."""

    stream: Permission | None = None
    """Permission for stream operations."""


@dataclass(slots=True)
class AccessTokenScope:
    """Scope of an access token."""

    basins: ExactMatch | PrefixMatch | None = None
    """Permitted basins."""

    streams: ExactMatch | PrefixMatch | None = None
    """Permitted streams."""

    access_tokens: ExactMatch | PrefixMatch | None = None
    """Permitted access tokens."""

    op_groups: OperationGroupPermissions | None = None
    """Permissions at operation group level."""

    ops: list[Operation] = field(default_factory=list)
    """Permitted operations."""


@dataclass(slots=True)
class AccessTokenInfo:
    """Access token information."""

    id: str
    """Access token ID."""

    scope: AccessTokenScope
    """Scope of the access token."""

    expires_at: str | None
    """Expiration time."""

    auto_prefix_streams: bool
    """Whether to automatically prefix stream names during creation and strip the prefix during listing."""


@dataclass(slots=True)
class Scalar:
    """Single named metric value."""

    name: str
    unit: MetricUnit
    value: float


@dataclass(slots=True)
class Accumulation:
    """Timeseries of accumulated values over buckets."""

    name: str
    unit: MetricUnit
    interval: TimeseriesInterval | None
    values: list[tuple[int, float]]


@dataclass(slots=True)
class Gauge:
    """Timeseries of instantaneous values."""

    name: str
    unit: MetricUnit
    values: list[tuple[int, float]]


@dataclass(slots=True)
class Label:
    """Set of string labels."""

    name: str
    values: list[str]
