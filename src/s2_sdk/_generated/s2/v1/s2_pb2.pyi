from typing import ClassVar as _ClassVar
from typing import Iterable as _Iterable
from typing import Mapping as _Mapping
from typing import Optional as _Optional
from typing import Union as _Union

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class StreamPosition(_message.Message):
    __slots__ = ("seq_num", "timestamp")
    SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    seq_num: int
    timestamp: int
    def __init__(
        self, seq_num: _Optional[int] = ..., timestamp: _Optional[int] = ...
    ) -> None: ...

class Header(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: bytes
    value: bytes
    def __init__(
        self, name: _Optional[bytes] = ..., value: _Optional[bytes] = ...
    ) -> None: ...

class AppendRecord(_message.Message):
    __slots__ = ("timestamp", "headers", "body")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    headers: _containers.RepeatedCompositeFieldContainer[Header]
    body: bytes
    def __init__(
        self,
        timestamp: _Optional[int] = ...,
        headers: _Optional[_Iterable[_Union[Header, _Mapping]]] = ...,
        body: _Optional[bytes] = ...,
    ) -> None: ...

class AppendInput(_message.Message):
    __slots__ = ("records", "match_seq_num", "fencing_token")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    MATCH_SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    FENCING_TOKEN_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[AppendRecord]
    match_seq_num: int
    fencing_token: str
    def __init__(
        self,
        records: _Optional[_Iterable[_Union[AppendRecord, _Mapping]]] = ...,
        match_seq_num: _Optional[int] = ...,
        fencing_token: _Optional[str] = ...,
    ) -> None: ...

class AppendAck(_message.Message):
    __slots__ = ("start", "end", "tail")
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    TAIL_FIELD_NUMBER: _ClassVar[int]
    start: StreamPosition
    end: StreamPosition
    tail: StreamPosition
    def __init__(
        self,
        start: _Optional[_Union[StreamPosition, _Mapping]] = ...,
        end: _Optional[_Union[StreamPosition, _Mapping]] = ...,
        tail: _Optional[_Union[StreamPosition, _Mapping]] = ...,
    ) -> None: ...

class SequencedRecord(_message.Message):
    __slots__ = ("seq_num", "timestamp", "headers", "body")
    SEQ_NUM_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    seq_num: int
    timestamp: int
    headers: _containers.RepeatedCompositeFieldContainer[Header]
    body: bytes
    def __init__(
        self,
        seq_num: _Optional[int] = ...,
        timestamp: _Optional[int] = ...,
        headers: _Optional[_Iterable[_Union[Header, _Mapping]]] = ...,
        body: _Optional[bytes] = ...,
    ) -> None: ...

class ReadBatch(_message.Message):
    __slots__ = ("records", "tail")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    TAIL_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[SequencedRecord]
    tail: StreamPosition
    def __init__(
        self,
        records: _Optional[_Iterable[_Union[SequencedRecord, _Mapping]]] = ...,
        tail: _Optional[_Union[StreamPosition, _Mapping]] = ...,
    ) -> None: ...
