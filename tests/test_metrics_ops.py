import time

import pytest

from s2_sdk import (
    S2,
    AccountMetricSet,
    Accumulation,
    BasinMetricSet,
    Gauge,
    Label,
    S2Basin,
    S2ServerError,
    S2Stream,
    Scalar,
    StreamMetricSet,
    TimeseriesInterval,
)


def _default_range() -> tuple[int, int]:
    """Return a (start, end) range covering the last hour."""
    now = int(time.time())
    return now - 3600, now


@pytest.mark.metrics
class TestMetricsOperations:
    async def test_account_metrics_active_basins(self, s2: S2):
        start, end = _default_range()
        metrics = await s2.account_metrics(
            set=AccountMetricSet.ACTIVE_BASINS, start=start, end=end
        )
        assert isinstance(metrics, list)
        for m in metrics:
            assert isinstance(m, (Scalar, Accumulation, Gauge, Label))

    async def test_account_metrics_account_ops(self, s2: S2):
        start, end = _default_range()
        metrics = await s2.account_metrics(
            set=AccountMetricSet.ACCOUNT_OPS, start=start, end=end
        )
        assert isinstance(metrics, list)

    async def test_account_metrics_account_ops_with_interval(self, s2: S2):
        start, end = _default_range()
        metrics = await s2.account_metrics(
            set=AccountMetricSet.ACCOUNT_OPS,
            start=start,
            end=end,
            interval=TimeseriesInterval.HOUR,
        )
        assert isinstance(metrics, list)

    async def test_account_metrics_empty_time_range(self, s2: S2):
        metrics = await s2.account_metrics(
            set=AccountMetricSet.ACTIVE_BASINS, start=0, end=1
        )
        assert isinstance(metrics, list)

    async def test_basin_metrics_storage(self, s2: S2, basin: S2Basin):
        start, end = _default_range()
        metrics = await s2.basin_metrics(
            basin.name, set=BasinMetricSet.STORAGE, start=start, end=end
        )
        assert isinstance(metrics, list)

    async def test_basin_metrics_ops(self, s2: S2, basin: S2Basin):
        start, end = _default_range()
        metrics = await s2.basin_metrics(
            basin.name, set=BasinMetricSet.BASIN_OPS, start=start, end=end
        )
        assert isinstance(metrics, list)

    async def test_basin_metrics_throughput(self, s2: S2, basin: S2Basin):
        start, end = _default_range()
        metrics = await s2.basin_metrics(
            basin.name, set=BasinMetricSet.APPEND_THROUGHPUT, start=start, end=end
        )
        assert isinstance(metrics, list)

    async def test_basin_metrics_empty_time_range(self, s2: S2, basin: S2Basin):
        metrics = await s2.basin_metrics(
            basin.name, set=BasinMetricSet.STORAGE, start=0, end=1
        )
        assert isinstance(metrics, list)

    async def test_stream_metrics_storage(
        self, s2: S2, shared_basin: S2Basin, stream: S2Stream
    ):
        start, end = _default_range()
        metrics = await s2.stream_metrics(
            shared_basin.name,
            stream.name,
            set=StreamMetricSet.STORAGE,
            start=start,
            end=end,
        )
        assert isinstance(metrics, list)

    async def test_stream_metrics_empty_time_range(
        self, s2: S2, shared_basin: S2Basin, stream: S2Stream
    ):
        metrics = await s2.stream_metrics(
            shared_basin.name, stream.name, set=StreamMetricSet.STORAGE, start=0, end=1
        )
        assert isinstance(metrics, list)

    async def test_account_metrics_account_ops_minute_interval(self, s2: S2):
        start, end = _default_range()
        metrics = await s2.account_metrics(
            set=AccountMetricSet.ACCOUNT_OPS,
            start=start,
            end=end,
            interval=TimeseriesInterval.MINUTE,
        )
        assert isinstance(metrics, list)

    async def test_account_metrics_account_ops_day_interval(self, s2: S2):
        start, end = _default_range()
        metrics = await s2.account_metrics(
            set=AccountMetricSet.ACCOUNT_OPS,
            start=start,
            end=end,
            interval=TimeseriesInterval.DAY,
        )
        assert isinstance(metrics, list)

    async def test_basin_metrics_append_ops(self, s2: S2, basin: S2Basin):
        start, end = _default_range()
        metrics = await s2.basin_metrics(
            basin.name, set=BasinMetricSet.APPEND_OPS, start=start, end=end
        )
        assert isinstance(metrics, list)

    async def test_basin_metrics_read_ops(self, s2: S2, basin: S2Basin):
        start, end = _default_range()
        metrics = await s2.basin_metrics(
            basin.name, set=BasinMetricSet.READ_OPS, start=start, end=end
        )
        assert isinstance(metrics, list)

    async def test_basin_metrics_read_throughput(self, s2: S2, basin: S2Basin):
        start, end = _default_range()
        metrics = await s2.basin_metrics(
            basin.name, set=BasinMetricSet.READ_THROUGHPUT, start=start, end=end
        )
        assert isinstance(metrics, list)

    async def test_account_metrics_invalid_time_ranges(self, s2: S2):
        now = int(time.time())
        # start > end is invalid
        with pytest.raises(S2ServerError):
            await s2.account_metrics(
                set=AccountMetricSet.ACTIVE_BASINS, start=now + 3600, end=now
            )

    async def test_account_metrics_all_sets(self, s2: S2):
        start, end = _default_range()
        for metric_set in AccountMetricSet:
            metrics = await s2.account_metrics(set=metric_set, start=start, end=end)
            assert isinstance(metrics, list)

    async def test_basin_metrics_invalid_time_ranges(self, s2: S2, basin: S2Basin):
        now = int(time.time())
        # start > end is invalid
        with pytest.raises(S2ServerError):
            await s2.basin_metrics(
                basin.name, set=BasinMetricSet.STORAGE, start=now + 3600, end=now
            )

    async def test_basin_metrics_all_sets(self, s2: S2, basin: S2Basin):
        start, end = _default_range()
        for metric_set in BasinMetricSet:
            metrics = await s2.basin_metrics(
                basin.name, set=metric_set, start=start, end=end
            )
            assert isinstance(metrics, list)

    async def test_stream_metrics_invalid_time_ranges(
        self, s2: S2, shared_basin: S2Basin, stream: S2Stream
    ):
        now = int(time.time())
        # start > end is invalid
        with pytest.raises(S2ServerError):
            await s2.stream_metrics(
                shared_basin.name,
                stream.name,
                set=StreamMetricSet.STORAGE,
                start=now + 3600,
                end=now,
            )
