"""S2S message-level compression (zstd and gzip)."""

import gzip

import zstandard

from s2_sdk._types import Compression

_zstd_compressor = zstandard.ZstdCompressor()
_zstd_dctx = zstandard.ZstdDecompressor()


def compress(data: bytes, compression: Compression) -> bytes:
    match compression:
        case Compression.ZSTD:
            return _zstd_compressor.compress(data)
        case Compression.GZIP:
            return gzip.compress(data)
        case _:
            return data


def decompress(data: bytes, compression: Compression) -> bytes:
    match compression:
        case Compression.ZSTD:
            # stream_reader handles frames without content size in header.
            with _zstd_dctx.stream_reader(data) as reader:
                return reader.read()
        case Compression.GZIP:
            return gzip.decompress(data)
        case _:
            return data
