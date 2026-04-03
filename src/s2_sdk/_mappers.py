from datetime import datetime
from typing import Any, Literal

import s2_sdk._generated.s2.v1.s2_pb2 as pb
from s2_sdk._types import (
    AccessTokenInfo,
    AccessTokenScope,
    Accumulation,
    AppendAck,
    AppendInput,
    BasinConfig,
    BasinInfo,
    BasinScope,
    ExactMatch,
    Gauge,
    Label,
    MetricUnit,
    Operation,
    OperationGroupPermissions,
    Permission,
    PrefixMatch,
    ReadBatch,
    ReadLimit,
    Record,
    Scalar,
    SeqNum,
    SequencedRecord,
    StorageClass,
    StreamConfig,
    StreamInfo,
    StreamPosition,
    TailOffset,
    TimeseriesInterval,
    Timestamp,
    Timestamping,
    TimestampingMode,
)

_ReadStart = SeqNum | Timestamp | TailOffset


def basin_config_to_json(config: BasinConfig | None) -> dict[str, Any] | None:
    if config is None:
        return None
    result: dict[str, Any] = {}
    if config.default_stream_config is not None:
        result["default_stream_config"] = stream_config_to_json(
            config.default_stream_config
        )
    if config.create_stream_on_append is not None:
        result["create_stream_on_append"] = config.create_stream_on_append
    if config.create_stream_on_read is not None:
        result["create_stream_on_read"] = config.create_stream_on_read
    return result


def basin_config_from_json(data: dict[str, Any]) -> BasinConfig:
    dsc = data.get("default_stream_config")
    return BasinConfig(
        default_stream_config=stream_config_from_json(dsc) if dsc else None,
        create_stream_on_append=data.get("create_stream_on_append"),
        create_stream_on_read=data.get("create_stream_on_read"),
    )


def basin_reconfiguration_to_json(config: BasinConfig) -> dict[str, Any]:
    return basin_config_to_json(config) or {}


def stream_config_to_json(config: StreamConfig | None) -> dict[str, Any] | None:
    if config is None:
        return None
    result: dict[str, Any] = {}
    if config.storage_class is not None:
        result["storage_class"] = config.storage_class.value
    if config.retention_policy is not None:
        result["retention_policy"] = _retention_policy_to_json(config.retention_policy)
    if config.timestamping is not None:
        ts: dict[str, Any] = {}
        if config.timestamping.mode is not None:
            ts["mode"] = config.timestamping.mode.value
        if config.timestamping.uncapped is not None:
            ts["uncapped"] = config.timestamping.uncapped
        result["timestamping"] = ts
    if config.delete_on_empty_min_age is not None:
        result["delete_on_empty"] = {"min_age_secs": config.delete_on_empty_min_age}
    return result


def stream_reconfiguration_to_json(config: StreamConfig) -> dict[str, Any]:
    return stream_config_to_json(config) or {}


def stream_config_from_json(data: dict[str, Any]) -> StreamConfig:
    retention_policy: int | Literal["infinite"] | None = None
    rp = data.get("retention_policy")
    if rp is not None:
        retention_policy = _retention_policy_from_json(rp)

    timestamping = None
    ts = data.get("timestamping")
    if ts is not None:
        mode_val = ts.get("mode")
        timestamping = Timestamping(
            mode=TimestampingMode(mode_val) if mode_val else None,
            uncapped=ts.get("uncapped"),
        )

    doe = data.get("delete_on_empty")
    delete_on_empty_min_age = doe.get("min_age_secs") if doe else None

    sc = data.get("storage_class")
    return StreamConfig(
        storage_class=StorageClass(sc) if sc else None,
        retention_policy=retention_policy,
        timestamping=timestamping,
        delete_on_empty_min_age=delete_on_empty_min_age,
    )


def _retention_policy_to_json(rp: int | Literal["infinite"]) -> dict[str, Any]:
    if rp == "infinite":
        return {"infinite": {}}
    return {"age": rp}


def _retention_policy_from_json(data: dict[str, Any]) -> int | Literal["infinite"]:
    if "infinite" in data:
        return "infinite"
    return data["age"]


def basin_info_from_json(data: dict[str, Any]) -> BasinInfo:
    created_at = datetime.fromisoformat(data["created_at"])
    deleted_at_str = data.get("deleted_at")
    deleted_at = datetime.fromisoformat(deleted_at_str) if deleted_at_str else None
    return BasinInfo(
        name=data["name"],
        scope=BasinScope(data["scope"]) if data.get("scope") else None,
        created_at=created_at,
        deleted_at=deleted_at,
    )


def stream_info_from_json(data: dict[str, Any]) -> StreamInfo:
    created_at = datetime.fromisoformat(data["created_at"])
    deleted_at_str = data.get("deleted_at")
    deleted_at = datetime.fromisoformat(deleted_at_str) if deleted_at_str else None
    return StreamInfo(
        name=data["name"],
        created_at=created_at,
        deleted_at=deleted_at,
    )


def _resource_set_to_json(
    rs: ExactMatch | PrefixMatch | None,
) -> dict[str, str] | None:
    if rs is None:
        return None
    if isinstance(rs, ExactMatch):
        return {"exact": rs.value}
    return {"prefix": rs.value}


def _resource_set_from_json(
    data: dict[str, Any] | None,
) -> ExactMatch | PrefixMatch | None:
    if data is None:
        return None
    if "exact" in data:
        return ExactMatch(data["exact"])
    if "prefix" in data:
        return PrefixMatch(data["prefix"])
    return None


def _rw_perms_to_json(perm: Permission | None) -> dict[str, bool] | None:
    if perm is None:
        return None
    match perm:
        case Permission.READ:
            return {"read": True}
        case Permission.WRITE:
            return {"write": True}
        case Permission.READ_WRITE:
            return {"read": True, "write": True}


def _rw_perms_from_json(data: dict[str, Any] | None) -> Permission | None:
    if data is None:
        return None
    read = data.get("read", False)
    write = data.get("write", False)
    if read and write:
        return Permission.READ_WRITE
    elif read:
        return Permission.READ
    elif write:
        return Permission.WRITE
    return None


def access_token_info_to_json(
    id: str,
    scope: AccessTokenScope,
    auto_prefix_streams: bool,
    expires_at: str | None,
) -> dict[str, Any]:
    scope_json: dict[str, Any] = {}
    if scope.basins is not None:
        scope_json["basins"] = _resource_set_to_json(scope.basins)
    if scope.streams is not None:
        scope_json["streams"] = _resource_set_to_json(scope.streams)
    if scope.access_tokens is not None:
        scope_json["access_tokens"] = _resource_set_to_json(scope.access_tokens)
    if scope.op_groups is not None:
        og: dict[str, Any] = {}
        if scope.op_groups.account is not None:
            og["account"] = _rw_perms_to_json(scope.op_groups.account)
        if scope.op_groups.basin is not None:
            og["basin"] = _rw_perms_to_json(scope.op_groups.basin)
        if scope.op_groups.stream is not None:
            og["stream"] = _rw_perms_to_json(scope.op_groups.stream)
        scope_json["op_groups"] = og
    if scope.ops:
        scope_json["ops"] = [op.value for op in scope.ops]

    result: dict[str, Any] = {
        "id": id,
        "scope": scope_json,
        "auto_prefix_streams": auto_prefix_streams,
    }
    if expires_at is not None:
        result["expires_at"] = expires_at
    return result


def access_token_info_from_json(data: dict[str, Any]) -> AccessTokenInfo:
    scope_data = data["scope"]
    og_data = scope_data.get("op_groups")
    op_groups = None
    if og_data:
        op_groups = OperationGroupPermissions(
            account=_rw_perms_from_json(og_data.get("account")),
            basin=_rw_perms_from_json(og_data.get("basin")),
            stream=_rw_perms_from_json(og_data.get("stream")),
        )

    ops_data = scope_data.get("ops")
    ops = [Operation(op) for op in ops_data] if ops_data else []

    return AccessTokenInfo(
        id=data["id"],
        scope=AccessTokenScope(
            basins=_resource_set_from_json(scope_data.get("basins")),
            streams=_resource_set_from_json(scope_data.get("streams")),
            access_tokens=_resource_set_from_json(scope_data.get("access_tokens")),
            op_groups=op_groups,
            ops=ops,
        ),
        expires_at=data.get("expires_at"),
        auto_prefix_streams=data.get("auto_prefix_streams", False),
    )


def metric_set_from_json(
    data: dict[str, Any],
) -> list[Scalar | Accumulation | Gauge | Label]:
    result: list[Scalar | Accumulation | Gauge | Label] = []
    for metric in data.get("values", []):
        if "scalar" in metric:
            s = metric["scalar"]
            result.append(
                Scalar(
                    name=s["name"],
                    unit=MetricUnit(s["unit"]),
                    value=s["value"],
                )
            )
        elif "accumulation" in metric:
            a = metric["accumulation"]
            bl = a.get("interval")
            result.append(
                Accumulation(
                    name=a["name"],
                    unit=MetricUnit(a["unit"]),
                    interval=TimeseriesInterval(bl) if bl else None,
                    values=[(int(v[0]), float(v[1])) for v in a["values"]],
                )
            )
        elif "gauge" in metric:
            g = metric["gauge"]
            result.append(
                Gauge(
                    name=g["name"],
                    unit=MetricUnit(g["unit"]),
                    values=[(int(v[0]), float(v[1])) for v in g["values"]],
                )
            )
        elif "label" in metric:
            lb = metric["label"]
            result.append(
                Label(
                    name=lb["name"],
                    values=lb["values"],
                )
            )
    return result


def tail_from_json(data: dict[str, Any]) -> StreamPosition:
    tail = data["tail"]
    return StreamPosition(seq_num=tail["seq_num"], timestamp=tail["timestamp"])


def append_record_to_proto(record: Record) -> pb.AppendRecord:
    headers = [pb.Header(name=name, value=value) for (name, value) in record.headers]
    return pb.AppendRecord(
        timestamp=record.timestamp, headers=headers, body=record.body
    )


def append_input_to_proto(inp: AppendInput) -> pb.AppendInput:
    records = [append_record_to_proto(r) for r in inp.records]
    return pb.AppendInput(
        records=records,
        match_seq_num=inp.match_seq_num,
        fencing_token=inp.fencing_token,
    )


def append_ack_from_proto(ack: pb.AppendAck) -> AppendAck:
    return AppendAck(
        start=StreamPosition(ack.start.seq_num, ack.start.timestamp),
        end=StreamPosition(ack.end.seq_num, ack.end.timestamp),
        tail=StreamPosition(ack.tail.seq_num, ack.tail.timestamp),
    )


def read_batch_from_proto(
    batch: pb.ReadBatch, ignore_command_records: bool = False
) -> ReadBatch:
    records = []
    for sr in batch.records:
        if ignore_command_records and _is_command_record(sr):
            continue
        records.append(
            SequencedRecord(
                seq_num=sr.seq_num,
                body=sr.body,
                headers=[(h.name, h.value) for h in sr.headers],
                timestamp=sr.timestamp,
            )
        )
    tail = None
    if batch.HasField("tail"):
        tail = StreamPosition(
            seq_num=batch.tail.seq_num,
            timestamp=batch.tail.timestamp,
        )
    return ReadBatch(records=records, tail=tail)


def sequenced_record_from_proto(sr: pb.SequencedRecord) -> SequencedRecord:
    return SequencedRecord(
        seq_num=sr.seq_num,
        body=sr.body,
        headers=[(h.name, h.value) for h in sr.headers],
        timestamp=sr.timestamp,
    )


def _is_command_record(sr: pb.SequencedRecord) -> bool:
    if len(sr.headers) == 1 and sr.headers[0].name == b"":
        return True
    return False


def read_start_params(start: _ReadStart) -> dict[str, Any]:
    if isinstance(start, SeqNum):
        return {"seq_num": start.value}
    elif isinstance(start, Timestamp):
        return {"timestamp": start.value}
    elif isinstance(start, TailOffset):
        return {"tail_offset": start.value}
    from s2_sdk._exceptions import S2ClientError

    raise S2ClientError("start doesn't match any of the expected types")


def read_limit_params(limit: ReadLimit | None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if limit:
        if limit.count is not None:
            params["count"] = limit.count
        if limit.bytes is not None:
            params["bytes"] = limit.bytes
    return params
