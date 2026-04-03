"""S2S message framing protocol.

Message layout: [3 bytes: length] [1 byte: flag] [N bytes: body]
  The 3-byte length covers flag + body (i.e. everything after the length prefix).
Flag byte: [T][CC][RRRRR]
  T  = terminal (1 = last message)
  CC = compression (00=none, 01=zstd, 10=gzip)
  R  = reserved
Terminal body: [2 bytes: status code big-endian] [JSON error]
"""

import json
import struct
from collections.abc import AsyncIterator
from typing import NamedTuple

from s2_sdk._compression import compress, decompress
from s2_sdk._exceptions import (
    UNKNOWN_CODE,
    S2ClientError,
    S2ServerError,
    raise_for_412,
    raise_for_416,
)
from s2_sdk._types import Compression


class Message(NamedTuple):
    body: bytes
    terminal: bool
    compression: Compression


# Compression threshold (1 KiB)
COMPRESSION_THRESHOLD = 1024

# Flag byte: [T][CC][RRRRR]
_TERMINAL_BIT = 0b1000_0000
_COMPRESSION_MASK = 0b0110_0000
_COMPRESSION_SHIFT = 5

# Length of the flag field in bytes
_FLAG_LEN = 1

# Wire codes for compression in the flag byte
_COMPRESSION_CODE = {
    Compression.NONE: 0,
    Compression.ZSTD: 1,
    Compression.GZIP: 2,
}

_COMPRESSION_FROM_CODE = {v: k for k, v in _COMPRESSION_CODE.items()}


def frame_message(msg: Message) -> bytes:
    msg_len = _FLAG_LEN + len(msg.body)
    flag = 0
    if msg.terminal:
        flag |= _TERMINAL_BIT
    flag |= (_COMPRESSION_CODE[msg.compression] & 0x3) << _COMPRESSION_SHIFT

    return struct.pack(">I", msg_len)[1:] + bytes([flag]) + msg.body


def deframe_data(data: bytes) -> Message:
    if len(data) < 4:
        raise ValueError("Message too short")

    msg_len = int.from_bytes(data[0:3], "big")  # flag + body
    flag = data[3]

    terminal = bool(flag & _TERMINAL_BIT)
    code = (flag & _COMPRESSION_MASK) >> _COMPRESSION_SHIFT
    compression = _COMPRESSION_FROM_CODE.get(code, Compression.NONE)

    body_len = msg_len - _FLAG_LEN
    body = data[4 : 4 + body_len]
    if len(body) < body_len:
        raise ValueError("Incomplete message body")

    return Message(body, terminal, compression)


async def read_messages(byte_stream: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    """Read messages from an async byte stream.

    Yields decoded message bodies. Stops on terminal message.
    """
    buf = bytearray()
    async for chunk in byte_stream:
        buf.extend(chunk)
        while len(buf) >= 4:
            msg_len = int.from_bytes(buf[0:3], "big")
            frame_len = 3 + msg_len
            if len(buf) < frame_len:
                break

            flag = buf[3]
            terminal = bool(flag & _TERMINAL_BIT)
            compression_code = (flag & _COMPRESSION_MASK) >> _COMPRESSION_SHIFT
            compression = _COMPRESSION_FROM_CODE.get(compression_code, Compression.NONE)
            body = bytes(buf[4:frame_len])
            del buf[:frame_len]

            if terminal:
                _handle_terminal(body)
                return

            if compression != Compression.NONE:
                body = decompress(body, compression)

            yield body


def maybe_compress(body: bytes, compression: Compression) -> tuple[bytes, Compression]:
    if len(body) >= COMPRESSION_THRESHOLD and compression != Compression.NONE:
        return compress(body, compression), compression
    return body, Compression.NONE


def parse_error_info(body: bytes, status_code: int) -> S2ServerError:
    try:
        error = json.loads(body)
        message = error.get("message", body.decode("utf-8", errors="replace"))
        code = (
            error.get("code", UNKNOWN_CODE) if isinstance(error, dict) else UNKNOWN_CODE
        )
    except Exception:
        message = body.decode("utf-8", errors="replace")
        code = UNKNOWN_CODE
    return S2ServerError(code, message, status_code)


def _handle_terminal(body: bytes) -> None:
    """Handle a terminal message body.

    Terminal messages carry an HTTP status code and JSON error information.
    Always raises an exception — terminal messages are only sent on errors.
    """
    if len(body) < 2:
        raise S2ClientError("Session terminated")

    status_code = int.from_bytes(body[0:2], "big")

    error_json = body[2:]
    if not error_json:
        raise S2ServerError(
            UNKNOWN_CODE,
            f"Session terminated with status {status_code}",
            status_code,
        )

    try:
        error = json.loads(error_json)
    except (json.JSONDecodeError, ValueError):
        raise S2ServerError(
            UNKNOWN_CODE,
            error_json.decode("utf-8", errors="replace"),
            status_code,
        )

    code = error.get("code", UNKNOWN_CODE) if isinstance(error, dict) else UNKNOWN_CODE

    if status_code == 412:
        raise_for_412(error, code)

    if status_code == 416:
        raise_for_416(error, code)

    if isinstance(error, dict):
        message = error.get("message", str(error))
    else:
        message = str(error)
    raise S2ServerError(code, message, status_code)
