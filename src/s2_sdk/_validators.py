import re

from s2_sdk._exceptions import S2ClientError
from s2_sdk._types import ONE_MIB

_BASIN_NAME_REGEX = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")


def validate_basin(name: str) -> None:
    if (
        isinstance(name, str)
        and (8 <= len(name) <= 48)
        and _BASIN_NAME_REGEX.match(name)
    ):
        return
    raise S2ClientError(f"Invalid basin name: {name}")


def validate_max_unacked(max_bytes: int, max_batches: int | None = None) -> None:
    if max_bytes < ONE_MIB:
        raise S2ClientError(
            f"max_unacked_bytes must be at least {ONE_MIB} (1 MiB), got {max_bytes}"
        )
    if max_batches is not None and max_batches < 1:
        raise S2ClientError(
            f"max_unacked_batches must be at least 1, got {max_batches}"
        )


def validate_batching(max_records: int, max_bytes: int) -> None:
    if not (1 <= max_records <= 1000):
        raise S2ClientError(
            f"max_records must be between 1 and 1000, got {max_records}"
        )
    if not (8 <= max_bytes <= ONE_MIB):
        raise S2ClientError(
            f"max_bytes must be between 8 and {ONE_MIB}, got {max_bytes}"
        )


def validate_retry(max_attempts: int) -> None:
    if max_attempts < 1:
        raise S2ClientError(
            f"Retry.max_attempts must be at least 1, got {max_attempts}"
        )


def validate_append_input(num_records: int, num_bytes: int) -> None:
    if 1 <= num_records <= 1000 and num_bytes <= ONE_MIB:
        return
    raise S2ClientError(
        f"Invalid append input: num_records={num_records}, metered_bytes={num_bytes}"
    )
