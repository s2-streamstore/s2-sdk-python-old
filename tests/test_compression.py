from s2_sdk._compression import compress, decompress
from s2_sdk._types import Compression


class TestCompression:
    def test_zstd_roundtrip(self):
        data = b"hello world" * 100
        compressed = compress(data, Compression.ZSTD)
        assert compressed != data
        decompressed = decompress(compressed, Compression.ZSTD)
        assert decompressed == data

    def test_gzip_roundtrip(self):
        data = b"hello world" * 100
        compressed = compress(data, Compression.GZIP)
        assert compressed != data
        decompressed = decompress(compressed, Compression.GZIP)
        assert decompressed == data

    def test_none_passthrough(self):
        data = b"hello world"
        assert compress(data, Compression.NONE) == data
        assert decompress(data, Compression.NONE) == data

    def test_zstd_compresses(self):
        data = b"aaaa" * 1000
        compressed = compress(data, Compression.ZSTD)
        assert len(compressed) < len(data)

    def test_gzip_compresses(self):
        data = b"aaaa" * 1000
        compressed = compress(data, Compression.GZIP)
        assert len(compressed) < len(data)
