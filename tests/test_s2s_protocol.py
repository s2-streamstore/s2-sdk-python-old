import pytest

from s2_sdk._s2s._protocol import (
    Message,
    deframe_data,
    frame_message,
    maybe_compress,
)
from s2_sdk._types import Compression


class TestMessageFraming:
    def test_frame_and_deframe_no_compression(self):
        body = b"hello world"
        data = frame_message(
            Message(body, terminal=False, compression=Compression.NONE)
        )
        msg = deframe_data(data)

        assert msg.body == body
        assert msg.terminal is False
        assert msg.compression == Compression.NONE

    def test_frame_and_deframe_with_zstd(self):
        body = b"hello world" * 100
        compressed, comp = maybe_compress(body, Compression.ZSTD)
        data = frame_message(Message(compressed, terminal=False, compression=comp))
        msg = deframe_data(data)

        # deframe_data returns raw (still-compressed) body
        assert msg.compression == Compression.ZSTD
        assert msg.terminal is False
        from s2_sdk._compression import decompress

        assert decompress(msg.body, msg.compression) == body

    def test_terminal_message(self):
        body = b"\x00\x00some error"
        data = frame_message(Message(body, terminal=True, compression=Compression.NONE))
        msg = deframe_data(data)

        assert msg.terminal is True
        # Terminal messages are not decompressed
        assert msg.body == body

    def test_message_length_encoding(self):
        # Verify 3-byte length prefix covers flag + body
        body = b"x" * 256
        data = frame_message(
            Message(body, terminal=False, compression=Compression.NONE)
        )

        # First 3 bytes are length (big-endian), includes 1 byte flag + body
        length = int.from_bytes(data[0:3], "big")
        assert length == 257  # 1 (flag) + 256 (body)

    def test_empty_body(self):
        data = frame_message(Message(b"", terminal=False, compression=Compression.NONE))
        msg = deframe_data(data)

        assert msg.body == b""
        assert msg.terminal is False

    def test_message_too_short(self):
        with pytest.raises(ValueError, match="Message too short"):
            deframe_data(b"\x00\x00")


class TestMaybeCompress:
    def test_below_threshold(self):
        body = b"small"
        compressed, comp_code = maybe_compress(body, Compression.ZSTD)
        assert compressed == body
        assert comp_code == Compression.NONE

    def test_above_threshold(self):
        body = b"x" * 2048
        compressed, comp_code = maybe_compress(body, Compression.ZSTD)
        assert comp_code == Compression.ZSTD
        assert len(compressed) < len(body)

    def test_no_compression_requested(self):
        body = b"x" * 2048
        compressed, comp_code = maybe_compress(body, compression=Compression.NONE)
        assert compressed == body
        assert comp_code == Compression.NONE
