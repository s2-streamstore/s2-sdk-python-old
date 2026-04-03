from s2_sdk._exceptions import ConnectError, ReadTimeoutError, S2ServerError
from s2_sdk._frame_signal import FrameSignal
from s2_sdk._retrier import (
    compute_backoffs,
    has_no_side_effects,
    is_safe_to_retry_session,
    is_safe_to_retry_unary,
)
from s2_sdk._types import AppendRetryPolicy


class TestComputeBackoffs:
    def test_backoffs_count(self):
        backoffs = compute_backoffs(5)
        assert len(backoffs) == 5

    def test_backoffs_range(self):
        backoffs = compute_backoffs(5, min_base_delay=0.1, max_base_delay=1.0)
        for b in backoffs:
            # Each backoff is base_delay + jitter where jitter in [0, base_delay]
            # so max is 2 * max_base_delay
            assert 0 <= b <= 2.0

    def test_backoffs_empty(self):
        backoffs = compute_backoffs(0)
        assert backoffs == []


class TestHasNoSideEffects:
    def test_rate_limited(self):
        e = S2ServerError("rate_limited", "rate limited", 429)
        assert has_no_side_effects(e) is True

    def test_hot_server(self):
        e = S2ServerError("hot_server", "hot server", 502)
        assert has_no_side_effects(e) is True

    def test_other_server_error(self):
        e = S2ServerError("internal", "internal", 500)
        assert has_no_side_effects(e) is False

    def test_429_wrong_code(self):
        e = S2ServerError("throttled", "throttled", 429)
        assert has_no_side_effects(e) is False

    def test_connect_error_connection_refused(self):
        cause = ConnectionRefusedError("connection refused")
        e = ConnectError("connection refused")
        e.__cause__ = cause
        assert has_no_side_effects(e) is True

    def test_connect_error_without_refused_cause(self):
        e = ConnectError("connection timed out")
        assert has_no_side_effects(e) is False

    def test_other_transport_error(self):
        e = ReadTimeoutError("timeout")
        assert has_no_side_effects(e) is False

    def test_generic_exception(self):
        e = RuntimeError("something")
        assert has_no_side_effects(e) is False


class TestSafeToRetryUnary:
    def test_no_policy_retries_retryable(self):
        e = S2ServerError("internal", "error", 500)
        assert is_safe_to_retry_unary(e, None) is True

    def test_all_policy_retries_retryable(self):
        e = S2ServerError("internal", "error", 500)
        assert is_safe_to_retry_unary(e, AppendRetryPolicy.ALL) is True

    def test_all_policy_skips_non_retryable(self):
        e = S2ServerError("bad_request", "bad request", 400)
        assert is_safe_to_retry_unary(e, AppendRetryPolicy.ALL) is False

    def test_nse_policy_retries_no_side_effect_error(self):
        e = S2ServerError("rate_limited", "rate limited", 429)
        assert is_safe_to_retry_unary(e, AppendRetryPolicy.NO_SIDE_EFFECTS) is True

    def test_nse_policy_retries_connect_error(self):
        cause = ConnectionRefusedError("connection refused")
        e = ConnectError("connection refused")
        e.__cause__ = cause
        assert is_safe_to_retry_unary(e, AppendRetryPolicy.NO_SIDE_EFFECTS) is True

    def test_nse_policy_skips_ambiguous_error(self):
        e = S2ServerError("internal", "error", 500)
        assert is_safe_to_retry_unary(e, AppendRetryPolicy.NO_SIDE_EFFECTS) is False

    def test_nse_policy_skips_timeout(self):
        e = ReadTimeoutError("timeout")
        assert is_safe_to_retry_unary(e, AppendRetryPolicy.NO_SIDE_EFFECTS) is False


class TestSafeToRetrySession:
    def test_all_policy_always_retries(self):
        e = S2ServerError("internal", "error", 500)
        assert is_safe_to_retry_session(e, AppendRetryPolicy.ALL, True, None) is True

    def test_all_policy_skips_non_retryable(self):
        e = S2ServerError("bad_request", "bad request", 400)
        assert is_safe_to_retry_session(e, AppendRetryPolicy.ALL, False, None) is False

    def test_nse_no_inflight_retries(self):
        e = S2ServerError("internal", "error", 500)
        assert (
            is_safe_to_retry_session(e, AppendRetryPolicy.NO_SIDE_EFFECTS, False, None)
            is True
        )

    def test_nse_inflight_signal_not_set_retries(self):
        signal = FrameSignal()
        e = S2ServerError("internal", "error", 500)
        assert (
            is_safe_to_retry_session(e, AppendRetryPolicy.NO_SIDE_EFFECTS, True, signal)
            is True
        )

    def test_nse_inflight_signal_set_no_side_effects_retries(self):
        signal = FrameSignal()
        signal.signal()
        e = S2ServerError("rate_limited", "rate limited", 429)
        assert (
            is_safe_to_retry_session(e, AppendRetryPolicy.NO_SIDE_EFFECTS, True, signal)
            is True
        )

    def test_nse_inflight_signal_set_ambiguous_skips(self):
        signal = FrameSignal()
        signal.signal()
        e = S2ServerError("internal", "error", 500)
        assert (
            is_safe_to_retry_session(e, AppendRetryPolicy.NO_SIDE_EFFECTS, True, signal)
            is False
        )

    def test_nse_inflight_no_signal_skips(self):
        """No FrameSignal at all + inflight + ambiguous error -> not safe."""
        e = S2ServerError("internal", "error", 500)
        assert (
            is_safe_to_retry_session(e, AppendRetryPolicy.NO_SIDE_EFFECTS, True, None)
            is False
        )
