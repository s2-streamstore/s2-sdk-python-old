from __future__ import annotations

from functools import wraps
from inspect import isasyncgenfunction, iscoroutinefunction
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from s2_sdk._types import StreamPosition


class S2Error(Exception):
    """Base class for all S2 related exceptions."""


class S2ClientError(S2Error):
    """Error originating from the client."""


UNKNOWN_CODE = "unknown"


class S2ServerError(S2Error):
    """Error originating from the server.

    Attributes:
        code: Error code from the server. See `error codes <https://s2.dev/docs/api/error-codes#standard-errors>`_ for possible values.
        message: Human-readable error message.
        status_code: HTTP status code.
    """

    def __init__(
        self,
        code: str,
        message: str,
        status_code: int,
    ):
        self.code = code
        self.message = message
        self.status_code = status_code
        self._retry_after: float | None = None
        super().__init__(message)


class AppendConditionError(S2ServerError):
    """Append condition (fencing token or sequence number match) was not met."""


class FencingTokenMismatchError(AppendConditionError):
    """Fencing token did not match.

    Attributes:
        expected_fencing_token: The fencing token the server expected.
    """

    def __init__(
        self, code: str, message: str, status_code: int, expected_fencing_token: str
    ):
        self.expected_fencing_token = expected_fencing_token
        super().__init__(code, message, status_code)


class SeqNumMismatchError(AppendConditionError):
    """Sequence number did not match.

    Attributes:
        expected_seq_num: The sequence number the server expected.
    """

    def __init__(
        self, code: str, message: str, status_code: int, expected_seq_num: int
    ):
        self.expected_seq_num = expected_seq_num
        super().__init__(code, message, status_code)


class ReadUnwrittenError(S2ServerError):
    """Read from an unwritten position.

    Attributes:
        tail: The tail position of the stream.
    """

    def __init__(self, code: str, message: str, status_code: int, tail: StreamPosition):
        self.tail = tail
        super().__init__(code, message, status_code)


def raise_for_412(body: dict, code: str) -> None:
    if "fencing_token_mismatch" in body:
        info = body["fencing_token_mismatch"]
        expected = (
            info if isinstance(info, str) else info.get("expected_fencing_token", "")
        )
        raise FencingTokenMismatchError(
            code,
            f"Fencing token mismatch: {info}",
            412,
            expected_fencing_token=str(expected),
        )
    elif "seq_num_mismatch" in body:
        info = body["seq_num_mismatch"]
        expected = info if isinstance(info, int) else info.get("expected_seq_num", 0)
        raise SeqNumMismatchError(
            code,
            f"Sequence number mismatch: {info}",
            412,
            expected_seq_num=int(expected),
        )
    raise AppendConditionError(code, str(body), 412)


def raise_for_416(body: dict, code: str) -> None:
    from s2_sdk._types import StreamPosition

    tail = body.get("tail", {})
    raise ReadUnwrittenError(
        code,
        "Read from unwritten position",
        416,
        tail=StreamPosition(
            seq_num=tail.get("seq_num", 0),
            timestamp=tail.get("timestamp", 0),
        ),
    )


S2Error.__module__ = "s2_sdk"
S2ClientError.__module__ = "s2_sdk"
S2ServerError.__module__ = "s2_sdk"
AppendConditionError.__module__ = "s2_sdk"
FencingTokenMismatchError.__module__ = "s2_sdk"
SeqNumMismatchError.__module__ = "s2_sdk"
ReadUnwrittenError.__module__ = "s2_sdk"


class TransportError(Exception):
    """Network-level error (connection reset, timeout, protocol)."""


class ConnectError(TransportError):
    """Failed to establish TCP/TLS connection."""


class ReadTimeoutError(TransportError):
    """Timed out waiting for data."""


class ConnectionClosedError(TransportError):
    """Connection closed unexpectedly."""


class ProtocolError(TransportError):
    """HTTP/2 protocol error (RST_STREAM, GOAWAY)."""

    def __init__(self, message: str, error_code: int | None = None):
        self.error_code = error_code
        super().__init__(message)


def fallible(f):
    @wraps(f)
    def sync_wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            if isinstance(e, S2Error):
                raise e
            raise S2ClientError(e) from e

    @wraps(f)
    async def async_gen_wrapper(*args, **kwargs):
        try:
            async for val in f(*args, **kwargs):
                yield val
        except Exception as e:
            if isinstance(e, S2Error):
                raise e
            raise S2ClientError(e) from e

    @wraps(f)
    async def coro_wrapper(*args, **kwargs):
        try:
            return await f(*args, **kwargs)
        except Exception as e:
            if isinstance(e, S2Error):
                raise e
            raise S2ClientError(e) from e

    if iscoroutinefunction(f):
        return coro_wrapper
    elif isasyncgenfunction(f):
        return async_gen_wrapper
    else:
        return sync_wrapper
