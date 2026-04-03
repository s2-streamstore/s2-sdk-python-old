import pytest

from s2_sdk import CommandRecord, Record, S2ClientError
from s2_sdk._types import metered_bytes
from s2_sdk._validators import validate_append_input


def test_append_record_batch_rejects_empty():
    with pytest.raises(S2ClientError):
        validate_append_input(0, 0)


def test_append_record_batch_rejects_too_many_records():
    records = [Record(body=b"a") for _ in range(1001)]
    with pytest.raises(S2ClientError):
        validate_append_input(len(records), metered_bytes(records))


def test_append_record_rejects_too_large():
    record = Record(body=b"a" * (1024 * 1024 + 1))
    with pytest.raises(S2ClientError):
        validate_append_input(1, metered_bytes([record]))


def test_fencing_token_rejects_too_long():
    with pytest.raises(S2ClientError):
        CommandRecord.fence("a" * 37)
