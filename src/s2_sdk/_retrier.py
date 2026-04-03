import asyncio
import random
from dataclasses import dataclass
from typing import Callable

from s2_sdk._exceptions import ConnectError, S2ServerError, TransportError
from s2_sdk._frame_signal import FrameSignal
from s2_sdk._types import AppendRetryPolicy


class Retrier:
    def __init__(
        self,
        should_retry_on: Callable[[Exception], bool],
        max_attempts: int,
        min_base_delay: float = 0.1,
        max_base_delay: float = 1.0,
    ):
        self.should_retry_on = should_retry_on
        self.max_attempts = max_attempts
        self.min_base_delay = min_base_delay
        self.max_base_delay = max_base_delay

    async def __call__(self, f: Callable, *args, **kwargs):
        backoffs = compute_backoffs(
            attempts=max(self.max_attempts - 1, 0),
            min_base_delay=self.min_base_delay,
            max_base_delay=self.max_base_delay,
        )
        attempt = 0
        while True:
            try:
                return await f(*args, **kwargs)
            except Exception as e:
                if attempt < len(backoffs) and self.should_retry_on(e):
                    delay = backoffs[attempt]
                    retry_after = getattr(e, "_retry_after", None)
                    if retry_after is not None:
                        delay = max(delay, retry_after)
                    await asyncio.sleep(delay)
                    attempt += 1
                else:
                    raise e


@dataclass(slots=True)
class Attempt:
    value: int


def compute_backoffs(
    attempts: int,
    min_base_delay: float = 0.1,
    max_base_delay: float = 1.0,
) -> list[float]:
    backoffs = []
    for n in range(attempts):
        base_delay = min(min_base_delay * 2**n, max_base_delay)
        jitter = random.uniform(0, base_delay)
        backoffs.append(base_delay + jitter)
    return backoffs


def is_safe_to_retry_unary(
    e: Exception,
    policy: AppendRetryPolicy | None,
) -> bool:
    match policy:
        case None | AppendRetryPolicy.ALL:
            policy_compliant = True
        case AppendRetryPolicy.NO_SIDE_EFFECTS:
            policy_compliant = has_no_side_effects(e)
    return policy_compliant and http_retry_on(e)


def is_safe_to_retry_session(
    e: Exception,
    policy: AppendRetryPolicy,
    has_inflight: bool,
    frame_signal: FrameSignal | None,
) -> bool:
    match policy:
        case AppendRetryPolicy.ALL:
            policy_compliant = True
        case AppendRetryPolicy.NO_SIDE_EFFECTS:
            not_signalled = frame_signal is not None and not frame_signal.is_signalled()
            policy_compliant = (
                not has_inflight or not_signalled or has_no_side_effects(e)
            )
    return policy_compliant and http_retry_on(e)


def http_retry_on(e: Exception) -> bool:
    if isinstance(e, S2ServerError):
        if e.status_code in (408, 429, 500, 502, 503, 504):
            return True
        if e.status_code == 409 and e.code == "transaction_conflict":
            return True
    if isinstance(e, TransportError):
        return True
    return False


def has_no_side_effects(e: Exception) -> bool:
    if isinstance(e, S2ServerError):
        return (e.status_code == 429 and e.code == "rate_limited") or (
            e.status_code == 502 and e.code == "hot_server"
        )
    if isinstance(e, ConnectError):
        cause = e.__cause__
        while cause is not None:
            if isinstance(cause, ConnectionRefusedError):
                return True
            cause = cause.__cause__
        return False
    return False
