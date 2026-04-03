from datetime import datetime, timedelta, timezone

import pytest

from s2_sdk import (
    S2,
    AccessTokenScope,
    BasinConfig,
    Operation,
    OperationGroupPermissions,
    Permission,
    PrefixMatch,
    S2Basin,
    S2ServerError,
    StorageClass,
    StreamConfig,
    Timestamping,
    TimestampingMode,
)
from tests.conftest import BASIN_PREFIX


@pytest.mark.account
class TestAccountOperations:
    async def test_create_basin(self, s2: S2, basin_name: str):
        basin_info = await s2.create_basin(name=basin_name)

        try:
            assert basin_info.name == basin_name
            assert basin_info.created_at is not None
        finally:
            await s2.delete_basin(basin_name)

    async def test_create_basin_with_config(self, s2: S2, basin_name: str):
        config = BasinConfig(
            default_stream_config=StreamConfig(
                storage_class=StorageClass.STANDARD,
                retention_policy=86400 * 7,
                timestamping=Timestamping(
                    mode=TimestampingMode.CLIENT_REQUIRE,
                    uncapped=True,
                ),
                delete_on_empty_min_age=3600,
            ),
            create_stream_on_append=True,
        )

        basin_info = await s2.create_basin(name=basin_name, config=config)

        try:
            assert basin_info.name == basin_name

            retrieved_config = await s2.get_basin_config(basin_name)
            assert retrieved_config.default_stream_config is not None
            assert (
                retrieved_config.default_stream_config.storage_class
                == StorageClass.STANDARD
            )
            assert retrieved_config.default_stream_config.retention_policy == 86400 * 7
            assert (
                retrieved_config.default_stream_config.timestamping.mode
                == TimestampingMode.CLIENT_REQUIRE
            )
            assert retrieved_config.default_stream_config.timestamping.uncapped is True
            assert (
                retrieved_config.default_stream_config.delete_on_empty_min_age == 3600
            )
            assert retrieved_config.create_stream_on_append is True
        finally:
            await s2.delete_basin(basin_name)

    async def test_reconfigure_basin(self, s2: S2, basin: S2Basin):
        config = BasinConfig(
            default_stream_config=StreamConfig(
                storage_class=StorageClass.STANDARD,
                retention_policy=3600,
            ),
            create_stream_on_append=True,
        )

        updated_config = await s2.reconfigure_basin(basin.name, config=config)

        assert updated_config.default_stream_config is not None
        assert (
            updated_config.default_stream_config.storage_class == StorageClass.STANDARD
        )
        assert updated_config.default_stream_config.retention_policy == 3600
        assert updated_config.create_stream_on_append is True

    async def test_list_basins(self, s2: S2, basin_names: list[str]):
        basin_infos = []
        try:
            for basin_name in basin_names:
                basin_info = await s2.create_basin(name=basin_name)
                basin_infos.append(basin_info)

            page = await s2.list_basins()

            retrieved_basin_names = [b.name for b in page.items]
            assert set(basin_names).issubset(retrieved_basin_names)

        finally:
            for basin_info in basin_infos:
                await s2.delete_basin(basin_info.name)

    async def test_list_basins_with_limit(self, s2: S2, basin_names: list[str]):
        basin_infos = []
        try:
            for basin_name in basin_names:
                basin_info = await s2.create_basin(name=basin_name)
                basin_infos.append(basin_info)

            page = await s2.list_basins(limit=1)

            assert len(page.items) == 1

        finally:
            for basin_info in basin_infos:
                await s2.delete_basin(basin_info.name)

    async def test_list_basins_with_prefix(self, s2: S2, basin_name: str):
        await s2.create_basin(name=basin_name)

        try:
            prefix = basin_name[:12]
            page = await s2.list_basins(prefix=prefix)

            names = [b.name for b in page.items]
            assert basin_name in names

            for name in names:
                assert name.startswith(prefix)

        finally:
            await s2.delete_basin(basin_name)

    @pytest.mark.cloud_only
    async def test_issue_access_token(self, s2: S2, token_id: str, basin_prefix: str):
        scope = AccessTokenScope(
            basins=PrefixMatch(basin_prefix),
            streams=PrefixMatch(""),
            op_groups=OperationGroupPermissions(
                basin=Permission.READ,
                stream=Permission.READ,
            ),
        )

        token = await s2.issue_access_token(id=token_id, scope=scope)

        try:
            assert isinstance(token, str)
            assert len(token) > 0
        finally:
            await s2.revoke_access_token(token_id)

    @pytest.mark.cloud_only
    async def test_issue_access_token_with_expiry(self, s2: S2, token_id: str):
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()

        scope = AccessTokenScope(
            streams=PrefixMatch(""),
            ops=[Operation.READ, Operation.CHECK_TAIL],
        )

        token = await s2.issue_access_token(
            id=token_id,
            scope=scope,
            expires_at=expires_at,
        )

        try:
            assert isinstance(token, str)
            assert len(token) > 0

            page = await s2.list_access_tokens(prefix=token_id)

            token_info = next((t for t in page.items if t.id == token_id), None)
            assert token_info is not None
            assert token_info.expires_at is not None
            assert token_info.scope.streams is not None

        finally:
            await s2.revoke_access_token(token_id)

    @pytest.mark.cloud_only
    async def test_issue_access_token_with_auto_prefix(self, s2: S2, token_id: str):
        scope = AccessTokenScope(
            streams=PrefixMatch("prefix/"),
            op_groups=OperationGroupPermissions(stream=Permission.READ_WRITE),
        )

        token = await s2.issue_access_token(
            id=token_id,
            scope=scope,
            auto_prefix_streams=True,
        )

        try:
            assert isinstance(token, str)
            assert len(token) > 0

            page = await s2.list_access_tokens(prefix=token_id, limit=1)

            assert len(page.items) == 1

            token_info = page.items[0]
            assert token_info is not None
            assert token_info.auto_prefix_streams is True

        finally:
            await s2.revoke_access_token(token_id)

    @pytest.mark.cloud_only
    async def test_get_basin_config(self, s2: S2, basin: S2Basin):
        config = await s2.get_basin_config(basin.name)
        assert config is not None
        assert config.default_stream_config is not None

    async def test_delete_nonexistent_basin_errors(self, s2: S2):
        with pytest.raises(S2ServerError):
            await s2.delete_basin("nonexistent-basin-xyz")

    async def test_list_basins_with_prefix_and_start_after(
        self, s2: S2, basin_names: list[str]
    ):
        basin_infos = []
        try:
            for basin_name in sorted(basin_names):
                basin_info = await s2.create_basin(name=basin_name)
                basin_infos.append(basin_info)

            sorted_names = sorted(basin_names)
            page = await s2.list_basins(
                prefix=BASIN_PREFIX, start_after=sorted_names[0], limit=100
            )

            retrieved = [b.name for b in page.items]
            # start_after is exclusive, so the first basin should not appear
            assert sorted_names[0] not in retrieved
        finally:
            for basin_info in basin_infos:
                await s2.delete_basin(basin_info.name)

    @pytest.mark.cloud_only
    async def test_list_access_tokens_with_limit(self, s2: S2, token_id: str):
        scope = AccessTokenScope(
            streams=PrefixMatch(""),
            op_groups=OperationGroupPermissions(stream=Permission.READ),
        )
        await s2.issue_access_token(id=token_id, scope=scope)

        try:
            page = await s2.list_access_tokens(limit=1)
            assert len(page.items) <= 1
        finally:
            await s2.revoke_access_token(token_id)

    @pytest.mark.cloud_only
    async def test_list_access_tokens_with_prefix(self, s2: S2, token_id: str):
        scope = AccessTokenScope(
            streams=PrefixMatch(""),
            op_groups=OperationGroupPermissions(stream=Permission.READ),
        )
        await s2.issue_access_token(id=token_id, scope=scope)

        try:
            page = await s2.list_access_tokens(prefix=token_id)
            names = [t.id for t in page.items]
            assert token_id in names
        finally:
            await s2.revoke_access_token(token_id)

    @pytest.mark.cloud_only
    async def test_issue_access_token_with_no_permitted_ops_errors(
        self, s2: S2, token_id: str
    ):
        scope = AccessTokenScope()

        with pytest.raises(S2ServerError):
            await s2.issue_access_token(id=token_id, scope=scope)

    @pytest.mark.cloud_only
    async def test_issue_access_token_with_auto_prefix_without_prefix_errors(
        self, s2: S2, token_id: str
    ):
        scope = AccessTokenScope(
            op_groups=OperationGroupPermissions(stream=Permission.READ_WRITE),
        )

        with pytest.raises(S2ServerError):
            await s2.issue_access_token(
                id=token_id, scope=scope, auto_prefix_streams=True
            )
