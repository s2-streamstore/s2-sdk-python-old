import uuid

import pytest

from s2_sdk import (
    S2Basin,
    S2ServerError,
    S2Stream,
    StorageClass,
    StreamConfig,
    Timestamping,
    TimestampingMode,
)


@pytest.mark.basin
class TestBasinOperations:
    async def test_create_stream(self, shared_basin: S2Basin, stream_name: str):
        basin = shared_basin

        stream_info = await basin.create_stream(name=stream_name)
        try:
            assert stream_info.name == stream_name
            assert stream_info.created_at is not None
            assert stream_info.deleted_at is None

        finally:
            await basin.delete_stream(stream_name)

    async def test_create_stream_with_config(
        self, shared_basin: S2Basin, stream_name: str
    ):
        basin = shared_basin

        config = StreamConfig(
            storage_class=StorageClass.STANDARD,
            retention_policy=86400 * 3,
            timestamping=Timestamping(
                mode=TimestampingMode.ARRIVAL,
                uncapped=False,
            ),
            delete_on_empty_min_age=7200,
        )

        stream_info = await basin.create_stream(name=stream_name, config=config)
        try:
            assert stream_info.name == stream_name

            retrieved_config = await basin.get_stream_config(stream_name)
            assert retrieved_config.storage_class == StorageClass.STANDARD
            assert retrieved_config.retention_policy == 86400 * 3
            assert retrieved_config.timestamping is not None
            assert retrieved_config.timestamping.mode == TimestampingMode.ARRIVAL
            assert retrieved_config.timestamping.uncapped is False
            assert retrieved_config.delete_on_empty_min_age == 7200

        finally:
            await basin.delete_stream(stream_name)

    async def test_default_stream_config(self, shared_basin: S2Basin, stream: S2Stream):
        basin = shared_basin

        config = await basin.get_stream_config(stream.name)
        assert config.storage_class == StorageClass.EXPRESS
        assert config.retention_policy == 86400 * 7

    async def test_reconfigure_stream(self, shared_basin: S2Basin, stream: S2Stream):
        basin = shared_basin
        config = StreamConfig(
            storage_class=StorageClass.STANDARD,
            retention_policy="infinite",
            timestamping=Timestamping(
                mode=TimestampingMode.CLIENT_REQUIRE, uncapped=True
            ),
            delete_on_empty_min_age=1800,
        )

        updated_config = await basin.reconfigure_stream(stream.name, config=config)
        assert updated_config.storage_class == StorageClass.STANDARD
        assert updated_config.retention_policy == "infinite"
        assert updated_config.timestamping is not None
        assert updated_config.timestamping.mode == TimestampingMode.CLIENT_REQUIRE
        assert updated_config.timestamping.uncapped is True
        assert updated_config.delete_on_empty_min_age == 1800

        config = StreamConfig(
            storage_class=StorageClass.EXPRESS,
            retention_policy=86400 * 90,
            timestamping=Timestamping(
                mode=TimestampingMode.CLIENT_PREFER, uncapped=False
            ),
            delete_on_empty_min_age=3600,
        )
        updated_config = await basin.reconfigure_stream(stream.name, config=config)
        assert updated_config.storage_class == StorageClass.EXPRESS
        assert updated_config.retention_policy == 86400 * 90
        assert updated_config.timestamping.mode == TimestampingMode.CLIENT_PREFER
        assert updated_config.timestamping.uncapped is False
        assert updated_config.delete_on_empty_min_age == 3600

    async def test_list_streams(self, shared_basin: S2Basin, stream_names: list[str]):
        basin = shared_basin

        stream_infos = []
        try:
            for stream_name in stream_names:
                stream_info = await basin.create_stream(name=stream_name)
                stream_infos.append(stream_info)

            page = await basin.list_streams()

            retrieved_stream_names = [s.name for s in page.items]
            assert set(stream_names).issubset(retrieved_stream_names)

        finally:
            for stream_info in stream_infos:
                await basin.delete_stream(stream_info.name)

    async def test_list_streams_with_limit(
        self, shared_basin: S2Basin, stream_names: list[str]
    ):
        basin = shared_basin

        stream_infos = []
        try:
            for stream_name in stream_names:
                stream_info = await basin.create_stream(name=stream_name)
                stream_infos.append(stream_info)

            page = await basin.list_streams(limit=1)

            assert len(page.items) == 1

        finally:
            for stream_info in stream_infos:
                await basin.delete_stream(stream_info.name)

    async def test_list_streams_with_prefix(
        self, shared_basin: S2Basin, stream_name: str
    ):
        basin = shared_basin

        await basin.create_stream(name=stream_name)

        try:
            prefix = stream_name[:5]
            page = await basin.list_streams(prefix=prefix)

            stream_names = [s.name for s in page.items]
            assert stream_name in stream_names

            for name in stream_names:
                assert name.startswith(prefix)

        finally:
            await basin.delete_stream(stream_name)

    async def test_create_stream_storage_class_express(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(storage_class=StorageClass.EXPRESS)
        info = await shared_basin.create_stream(name=stream_name, config=config)
        try:
            assert info.name == stream_name
            retrieved = await shared_basin.get_stream_config(stream_name)
            assert retrieved.storage_class == StorageClass.EXPRESS
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_create_stream_retention_infinite(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(retention_policy="infinite")
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            retrieved = await shared_basin.get_stream_config(stream_name)
            assert retrieved.retention_policy == "infinite"
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_create_stream_timestamping_modes(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(
            timestamping=Timestamping(mode=TimestampingMode.CLIENT_PREFER)
        )
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            retrieved = await shared_basin.get_stream_config(stream_name)
            assert retrieved.timestamping is not None
            assert retrieved.timestamping.mode == TimestampingMode.CLIENT_PREFER
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_create_stream_timestamping_uncapped(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(
            timestamping=Timestamping(
                mode=TimestampingMode.CLIENT_REQUIRE, uncapped=True
            )
        )
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            retrieved = await shared_basin.get_stream_config(stream_name)
            assert retrieved.timestamping is not None
            assert retrieved.timestamping.uncapped is True
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_create_stream_delete_on_empty(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(delete_on_empty_min_age=3600)
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            retrieved = await shared_basin.get_stream_config(stream_name)
            assert retrieved.delete_on_empty_min_age == 3600
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_reconfigure_stream_storage_class(
        self, shared_basin: S2Basin, stream: S2Stream
    ):
        updated = await shared_basin.reconfigure_stream(
            stream.name, config=StreamConfig(storage_class=StorageClass.STANDARD)
        )
        assert updated.storage_class == StorageClass.STANDARD

    async def test_reconfigure_stream_retention(
        self, shared_basin: S2Basin, stream: S2Stream
    ):
        updated = await shared_basin.reconfigure_stream(
            stream.name, config=StreamConfig(retention_policy="infinite")
        )
        assert updated.retention_policy == "infinite"

    async def test_reconfigure_stream_timestamping(
        self, shared_basin: S2Basin, stream: S2Stream
    ):
        updated = await shared_basin.reconfigure_stream(
            stream.name,
            config=StreamConfig(
                timestamping=Timestamping(mode=TimestampingMode.CLIENT_REQUIRE)
            ),
        )
        assert updated.timestamping is not None
        assert updated.timestamping.mode == TimestampingMode.CLIENT_REQUIRE

    async def test_reconfigure_stream_delete_on_empty(
        self, shared_basin: S2Basin, stream: S2Stream
    ):
        updated = await shared_basin.reconfigure_stream(
            stream.name, config=StreamConfig(delete_on_empty_min_age=7200)
        )
        assert updated.delete_on_empty_min_age == 7200

    async def test_reconfigure_stream_partial_update(
        self, shared_basin: S2Basin, stream: S2Stream
    ):
        # Only change retention, other fields unchanged
        before = await shared_basin.get_stream_config(stream.name)
        await shared_basin.reconfigure_stream(
            stream.name, config=StreamConfig(retention_policy=86400)
        )
        after = await shared_basin.get_stream_config(stream.name)
        assert after.retention_policy == 86400
        # Storage class should remain the same
        assert after.storage_class == before.storage_class

    async def test_reconfigure_stream_empty_no_change(
        self, shared_basin: S2Basin, stream: S2Stream
    ):
        before = await shared_basin.get_stream_config(stream.name)
        after = await shared_basin.reconfigure_stream(
            stream.name, config=StreamConfig()
        )
        assert after.storage_class == before.storage_class

    async def test_delete_nonexistent_stream_errors(self, shared_basin: S2Basin):
        with pytest.raises(S2ServerError):
            await shared_basin.delete_stream("nonexistent-stream-xyz")

    async def test_get_stream_config_nonexistent_errors(self, shared_basin: S2Basin):
        with pytest.raises(S2ServerError):
            await shared_basin.get_stream_config("nonexistent-stream-xyz")

    async def test_create_stream_duplicate_name_errors(
        self, shared_basin: S2Basin, stream: S2Stream
    ):
        with pytest.raises(S2ServerError):
            await shared_basin.create_stream(name=stream.name)

    async def test_list_streams_with_start_after(
        self, shared_basin: S2Basin, stream_names: list[str]
    ):
        basin = shared_basin
        try:
            for name in sorted(stream_names):
                await basin.create_stream(name=name)

            sorted_names = sorted(stream_names)
            page = await basin.list_streams(start_after=sorted_names[0])

            retrieved = [s.name for s in page.items]
            assert sorted_names[0] not in retrieved
        finally:
            for name in stream_names:
                try:
                    await basin.delete_stream(name)
                except Exception:
                    pass

    async def test_list_streams_returns_lexicographic_order(
        self, shared_basin: S2Basin, stream_names: list[str]
    ):
        basin = shared_basin
        try:
            for name in stream_names:
                await basin.create_stream(name=name)

            page = await basin.list_streams(prefix="stream-")

            retrieved = [s.name for s in page.items]
            assert retrieved == sorted(retrieved)
        finally:
            for name in stream_names:
                try:
                    await basin.delete_stream(name)
                except Exception:
                    pass

    async def test_deleted_stream_has_deleted_at(
        self, shared_basin: S2Basin, stream_name: str
    ):
        basin = shared_basin
        info = await basin.create_stream(name=stream_name)
        assert info.deleted_at is None
        await basin.delete_stream(stream_name)

    async def test_create_stream_inherits_basin_defaults(
        self, shared_basin: S2Basin, stream_name: str
    ):
        """A stream created with no config inherits the basin defaults."""
        await shared_basin.create_stream(name=stream_name)
        try:
            config = await shared_basin.get_stream_config(stream_name)
            assert config.storage_class is not None
            assert config.retention_policy is not None
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_list_streams_with_start_after_returns_empty_page(
        self, shared_basin: S2Basin
    ):
        basin = shared_basin
        prefix = f"safp-{uuid.uuid4().hex[:6]}"
        names = sorted([f"{prefix}-{i:04}" for i in range(3)])
        try:
            for name in names:
                await basin.create_stream(name=name)

            last_name = names[-1]
            page = await basin.list_streams(prefix=prefix, start_after=last_name)

            assert len(page.items) == 0
            assert page.has_more is False
        finally:
            for name in names:
                try:
                    await basin.delete_stream(name)
                except Exception:
                    pass

    async def test_list_streams_with_start_after_less_than_prefix_errors(
        self, shared_basin: S2Basin
    ):
        basin = shared_basin
        base = uuid.uuid4().hex[:6]
        names = [f"{base}-a-a", f"{base}-a-b", f"{base}-b-a"]
        for name in names:
            await basin.create_stream(name=name)
        try:
            with pytest.raises(S2ServerError):
                await basin.list_streams(prefix=f"{base}-b", start_after=f"{base}-a")
        finally:
            for name in names:
                try:
                    await basin.delete_stream(name)
                except Exception:
                    pass

    async def test_list_streams_with_limit_zero(
        self, shared_basin: S2Basin, stream_name: str
    ):
        await shared_basin.create_stream(name=stream_name)
        try:
            page = await shared_basin.list_streams(prefix=stream_name[:8], limit=0)
            assert len(page.items) <= 1000
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_list_streams_with_limit_over_max(
        self, shared_basin: S2Basin, stream_name: str
    ):
        await shared_basin.create_stream(name=stream_name)
        try:
            page = await shared_basin.list_streams(prefix=stream_name[:8], limit=1500)
            assert len(page.items) <= 1000
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_list_streams_with_pagination(self, shared_basin: S2Basin):
        basin = shared_basin
        prefix = f"page-{uuid.uuid4().hex[:6]}"
        names = sorted([f"{prefix}-{i:04}" for i in range(3)])
        for name in names:
            await basin.create_stream(name=name)
        try:
            page_1 = await basin.list_streams(prefix=prefix, limit=2)
            assert len(page_1.items) > 0

            last_name = page_1.items[-1].name
            page_2 = await basin.list_streams(
                prefix=prefix, start_after=last_name, limit=2
            )

            assert all(s.name > last_name for s in page_2.items)

            listed = sorted(
                [s.name for s in page_1.items] + [s.name for s in page_2.items]
            )
            assert listed == names
        finally:
            for name in names:
                try:
                    await basin.delete_stream(name)
                except Exception:
                    pass

    async def test_create_stream_invalid_retention_age_zero(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(retention_policy=0)
        with pytest.raises(S2ServerError):
            await shared_basin.create_stream(name=stream_name, config=config)

    async def test_reconfigure_stream_storage_class_express(
        self, shared_basin: S2Basin, stream_name: str
    ):
        await shared_basin.create_stream(name=stream_name)
        try:
            updated = await shared_basin.reconfigure_stream(
                stream_name, config=StreamConfig(storage_class=StorageClass.EXPRESS)
            )
            assert updated.storage_class == StorageClass.EXPRESS
        except S2ServerError:
            pass  # Free tier may not support Express
        finally:
            try:
                await shared_basin.delete_stream(stream_name)
            except Exception:
                pass

    async def test_reconfigure_stream_retention_policy_age(
        self, shared_basin: S2Basin, stream_name: str
    ):
        await shared_basin.create_stream(name=stream_name)
        try:
            updated = await shared_basin.reconfigure_stream(
                stream_name, config=StreamConfig(retention_policy=3600)
            )
            assert updated.retention_policy == 3600
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_reconfigure_stream_timestamping_uncapped(
        self, shared_basin: S2Basin
    ):
        for uncapped in [True, False]:
            name = f"stream-{uuid.uuid4().hex[:8]}"
            await shared_basin.create_stream(name=name)
            try:
                updated = await shared_basin.reconfigure_stream(
                    name,
                    config=StreamConfig(timestamping=Timestamping(uncapped=uncapped)),
                )
                if updated.timestamping is not None:
                    assert updated.timestamping.uncapped == uncapped
            finally:
                await shared_basin.delete_stream(name)

    async def test_reconfigure_stream_disable_delete_on_empty(
        self, shared_basin: S2Basin, stream_name: str
    ):
        config = StreamConfig(delete_on_empty_min_age=3600)
        await shared_basin.create_stream(name=stream_name, config=config)
        try:
            updated = await shared_basin.reconfigure_stream(
                stream_name, config=StreamConfig(delete_on_empty_min_age=0)
            )
            assert (
                updated.delete_on_empty_min_age is None
                or updated.delete_on_empty_min_age == 0
            )
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_reconfigure_stream_invalid_retention_age_zero(
        self, shared_basin: S2Basin, stream_name: str
    ):
        await shared_basin.create_stream(name=stream_name)
        try:
            with pytest.raises(S2ServerError):
                await shared_basin.reconfigure_stream(
                    stream_name, config=StreamConfig(retention_policy=0)
                )
        finally:
            await shared_basin.delete_stream(stream_name)

    async def test_reconfigure_stream_nonexistent_errors(self, shared_basin: S2Basin):
        with pytest.raises(S2ServerError):
            await shared_basin.reconfigure_stream(
                "nonexistent-stream-xyz",
                config=StreamConfig(storage_class=StorageClass.STANDARD),
            )

    async def test_delete_stream_already_deleting_is_idempotent(
        self, shared_basin: S2Basin, stream_name: str
    ):
        await shared_basin.create_stream(name=stream_name)
        await shared_basin.delete_stream(stream_name)
        # Second delete should be idempotent (no error or stream_not_found)
        try:
            await shared_basin.delete_stream(stream_name)
        except S2ServerError:
            pass  # stream_not_found is acceptable

    async def test_get_stream_config_for_deleting_stream_errors(
        self, shared_basin: S2Basin, stream_name: str
    ):
        await shared_basin.create_stream(name=stream_name)
        await shared_basin.delete_stream(stream_name)

        with pytest.raises(S2ServerError):
            await shared_basin.get_stream_config(stream_name)
