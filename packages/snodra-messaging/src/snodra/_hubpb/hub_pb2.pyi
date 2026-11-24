from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ProcessorState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROCESSOR_STATE_UNSPECIFIED: _ClassVar[ProcessorState]
    PROCESSOR_STARTING: _ClassVar[ProcessorState]
    PROCESSOR_RUNNING: _ClassVar[ProcessorState]
    PROCESSOR_FAILING: _ClassVar[ProcessorState]
    PROCESSOR_STOPPED: _ClassVar[ProcessorState]
    PROCESSOR_PAUSED: _ClassVar[ProcessorState]

class ControlRefusal(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONTROL_REFUSAL_UNSPECIFIED: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_UNKNOWN_PROCESSOR: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_LOCKED: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_NOT_PREPARED: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_RESET_HANDLER_FAILED: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_INVALID: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_FAILED: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_NOT_FOUND: _ClassVar[ControlRefusal]
    CONTROL_REFUSAL_UNSUPPORTED: _ClassVar[ControlRefusal]

class RequestMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNARY: _ClassVar[RequestMode]
    SCATTER_GATHER: _ClassVar[RequestMode]
    STREAMING: _ClassVar[RequestMode]
    SUBSCRIPTION: _ClassVar[RequestMode]

class RequestKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REQUEST_KIND_UNSPECIFIED: _ClassVar[RequestKind]
    COMMAND: _ClassVar[RequestKind]
    QUERY: _ClassVar[RequestKind]

class ErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ERROR_CODE_UNSPECIFIED: _ClassVar[ErrorCode]
    NO_HANDLER: _ClassVar[ErrorCode]
    TIMEOUT: _ClassVar[ErrorCode]
    HANDLER_DISCONNECTED: _ClassVar[ErrorCode]
    HANDLER_ERROR: _ClassVar[ErrorCode]
    UNAUTHENTICATED: _ClassVar[ErrorCode]
    INVALID_REQUEST: _ClassVar[ErrorCode]
    PERMISSION_DENIED: _ClassVar[ErrorCode]
    RESOURCE_EXHAUSTED: _ClassVar[ErrorCode]
    CANCELLED: _ClassVar[ErrorCode]
PROCESSOR_STATE_UNSPECIFIED: ProcessorState
PROCESSOR_STARTING: ProcessorState
PROCESSOR_RUNNING: ProcessorState
PROCESSOR_FAILING: ProcessorState
PROCESSOR_STOPPED: ProcessorState
PROCESSOR_PAUSED: ProcessorState
CONTROL_REFUSAL_UNSPECIFIED: ControlRefusal
CONTROL_REFUSAL_UNKNOWN_PROCESSOR: ControlRefusal
CONTROL_REFUSAL_LOCKED: ControlRefusal
CONTROL_REFUSAL_NOT_PREPARED: ControlRefusal
CONTROL_REFUSAL_SUBSCRIBERS_ACTIVE: ControlRefusal
CONTROL_REFUSAL_RESET_HANDLER_FAILED: ControlRefusal
CONTROL_REFUSAL_INVALID: ControlRefusal
CONTROL_REFUSAL_FAILED: ControlRefusal
CONTROL_REFUSAL_NOT_FOUND: ControlRefusal
CONTROL_REFUSAL_UNSUPPORTED: ControlRefusal
UNARY: RequestMode
SCATTER_GATHER: RequestMode
STREAMING: RequestMode
SUBSCRIPTION: RequestMode
REQUEST_KIND_UNSPECIFIED: RequestKind
COMMAND: RequestKind
QUERY: RequestKind
ERROR_CODE_UNSPECIFIED: ErrorCode
NO_HANDLER: ErrorCode
TIMEOUT: ErrorCode
HANDLER_DISCONNECTED: ErrorCode
HANDLER_ERROR: ErrorCode
UNAUTHENTICATED: ErrorCode
INVALID_REQUEST: ErrorCode
PERMISSION_DENIED: ErrorCode
RESOURCE_EXHAUSTED: ErrorCode
CANCELLED: ErrorCode

class ClientMessage(_message.Message):
    __slots__ = ("register", "request", "reply", "heartbeat", "permits", "part", "end", "flow", "cancel", "status", "credentials", "control_reply")
    REGISTER_FIELD_NUMBER: _ClassVar[int]
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    REPLY_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    PERMITS_FIELD_NUMBER: _ClassVar[int]
    PART_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    FLOW_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    CONTROL_REPLY_FIELD_NUMBER: _ClassVar[int]
    register: Register
    request: Request
    reply: Reply
    heartbeat: Heartbeat
    permits: Permits
    part: ReplyPart
    end: ReplyEnd
    flow: Flow
    cancel: Cancel
    status: InstanceStatus
    credentials: Credentials
    control_reply: ControlReply
    def __init__(self, register: _Optional[_Union[Register, _Mapping]] = ..., request: _Optional[_Union[Request, _Mapping]] = ..., reply: _Optional[_Union[Reply, _Mapping]] = ..., heartbeat: _Optional[_Union[Heartbeat, _Mapping]] = ..., permits: _Optional[_Union[Permits, _Mapping]] = ..., part: _Optional[_Union[ReplyPart, _Mapping]] = ..., end: _Optional[_Union[ReplyEnd, _Mapping]] = ..., flow: _Optional[_Union[Flow, _Mapping]] = ..., cancel: _Optional[_Union[Cancel, _Mapping]] = ..., status: _Optional[_Union[InstanceStatus, _Mapping]] = ..., credentials: _Optional[_Union[Credentials, _Mapping]] = ..., control_reply: _Optional[_Union[ControlReply, _Mapping]] = ...) -> None: ...

class ServerMessage(_message.Message):
    __slots__ = ("request", "reply", "error", "heartbeat", "register_ack", "part", "end", "flow", "cancel", "control")
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    REPLY_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_FIELD_NUMBER: _ClassVar[int]
    REGISTER_ACK_FIELD_NUMBER: _ClassVar[int]
    PART_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    FLOW_FIELD_NUMBER: _ClassVar[int]
    CANCEL_FIELD_NUMBER: _ClassVar[int]
    CONTROL_FIELD_NUMBER: _ClassVar[int]
    request: Request
    reply: Reply
    error: Error
    heartbeat: Heartbeat
    register_ack: RegisterAck
    part: ReplyPart
    end: ReplyEnd
    flow: Flow
    cancel: Cancel
    control: ControlRequest
    def __init__(self, request: _Optional[_Union[Request, _Mapping]] = ..., reply: _Optional[_Union[Reply, _Mapping]] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., heartbeat: _Optional[_Union[Heartbeat, _Mapping]] = ..., register_ack: _Optional[_Union[RegisterAck, _Mapping]] = ..., part: _Optional[_Union[ReplyPart, _Mapping]] = ..., end: _Optional[_Union[ReplyEnd, _Mapping]] = ..., flow: _Optional[_Union[Flow, _Mapping]] = ..., cancel: _Optional[_Union[Cancel, _Mapping]] = ..., control: _Optional[_Union[ControlRequest, _Mapping]] = ...) -> None: ...

class Register(_message.Message):
    __slots__ = ("service_name", "instance_id", "command_types", "query_types", "credentials", "initial_permits", "load_factor", "tags", "client_version", "shared_query_types", "subscription_query_types")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    COMMAND_TYPES_FIELD_NUMBER: _ClassVar[int]
    QUERY_TYPES_FIELD_NUMBER: _ClassVar[int]
    CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    INITIAL_PERMITS_FIELD_NUMBER: _ClassVar[int]
    LOAD_FACTOR_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    CLIENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SHARED_QUERY_TYPES_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPTION_QUERY_TYPES_FIELD_NUMBER: _ClassVar[int]
    service_name: str
    instance_id: str
    command_types: _containers.RepeatedScalarFieldContainer[str]
    query_types: _containers.RepeatedScalarFieldContainer[str]
    credentials: Credentials
    initial_permits: int
    load_factor: int
    tags: _containers.ScalarMap[str, str]
    client_version: str
    shared_query_types: _containers.RepeatedScalarFieldContainer[str]
    subscription_query_types: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, service_name: _Optional[str] = ..., instance_id: _Optional[str] = ..., command_types: _Optional[_Iterable[str]] = ..., query_types: _Optional[_Iterable[str]] = ..., credentials: _Optional[_Union[Credentials, _Mapping]] = ..., initial_permits: _Optional[int] = ..., load_factor: _Optional[int] = ..., tags: _Optional[_Mapping[str, str]] = ..., client_version: _Optional[str] = ..., shared_query_types: _Optional[_Iterable[str]] = ..., subscription_query_types: _Optional[_Iterable[str]] = ...) -> None: ...

class Permits(_message.Message):
    __slots__ = ("count", "grant")
    COUNT_FIELD_NUMBER: _ClassVar[int]
    GRANT_FIELD_NUMBER: _ClassVar[int]
    count: int
    grant: int
    def __init__(self, count: _Optional[int] = ..., grant: _Optional[int] = ...) -> None: ...

class InstanceStatus(_message.Message):
    __slots__ = ("connected_hub_ids", "handlers_in_flight", "processors")
    CONNECTED_HUB_IDS_FIELD_NUMBER: _ClassVar[int]
    HANDLERS_IN_FLIGHT_FIELD_NUMBER: _ClassVar[int]
    PROCESSORS_FIELD_NUMBER: _ClassVar[int]
    connected_hub_ids: _containers.RepeatedScalarFieldContainer[str]
    handlers_in_flight: int
    processors: _containers.RepeatedCompositeFieldContainer[ProcessorStatus]
    def __init__(self, connected_hub_ids: _Optional[_Iterable[str]] = ..., handlers_in_flight: _Optional[int] = ..., processors: _Optional[_Iterable[_Union[ProcessorStatus, _Mapping]]] = ...) -> None: ...

class ProcessorStatus(_message.Message):
    __slots__ = ("name", "subscription", "source", "state", "segments", "processed_total", "failed_total", "dead_letters", "last_error", "last_error_at_ms", "event_types", "dead_letter_mode", "replaying", "operation_id", "event_store", "capabilities")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SUBSCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    SEGMENTS_FIELD_NUMBER: _ClassVar[int]
    PROCESSED_TOTAL_FIELD_NUMBER: _ClassVar[int]
    FAILED_TOTAL_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTERS_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_AT_MS_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPES_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_MODE_FIELD_NUMBER: _ClassVar[int]
    REPLAYING_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_STORE_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    name: str
    subscription: str
    source: str
    state: ProcessorState
    segments: _containers.RepeatedCompositeFieldContainer[SegmentStatus]
    processed_total: int
    failed_total: int
    dead_letters: int
    last_error: str
    last_error_at_ms: int
    event_types: _containers.RepeatedScalarFieldContainer[str]
    dead_letter_mode: str
    replaying: bool
    operation_id: str
    event_store: str
    capabilities: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[str] = ..., subscription: _Optional[str] = ..., source: _Optional[str] = ..., state: _Optional[_Union[ProcessorState, str]] = ..., segments: _Optional[_Iterable[_Union[SegmentStatus, _Mapping]]] = ..., processed_total: _Optional[int] = ..., failed_total: _Optional[int] = ..., dead_letters: _Optional[int] = ..., last_error: _Optional[str] = ..., last_error_at_ms: _Optional[int] = ..., event_types: _Optional[_Iterable[str]] = ..., dead_letter_mode: _Optional[str] = ..., replaying: _Optional[bool] = ..., operation_id: _Optional[str] = ..., event_store: _Optional[str] = ..., capabilities: _Optional[_Iterable[str]] = ...) -> None: ...

class SegmentStatus(_message.Message):
    __slots__ = ("segment", "position", "committed", "head", "lag", "replay_until")
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_FIELD_NUMBER: _ClassVar[int]
    HEAD_FIELD_NUMBER: _ClassVar[int]
    LAG_FIELD_NUMBER: _ClassVar[int]
    REPLAY_UNTIL_FIELD_NUMBER: _ClassVar[int]
    segment: str
    position: str
    committed: str
    head: str
    lag: int
    replay_until: str
    def __init__(self, segment: _Optional[str] = ..., position: _Optional[str] = ..., committed: _Optional[str] = ..., head: _Optional[str] = ..., lag: _Optional[int] = ..., replay_until: _Optional[str] = ...) -> None: ...

class ControlRequest(_message.Message):
    __slots__ = ("control_id", "processor", "pause", "resume", "reset_prepare", "reset_execute", "reset_finish", "dead_letter_list", "dead_letter_retry", "dead_letter_retry_all", "dead_letter_discard")
    CONTROL_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSOR_FIELD_NUMBER: _ClassVar[int]
    PAUSE_FIELD_NUMBER: _ClassVar[int]
    RESUME_FIELD_NUMBER: _ClassVar[int]
    RESET_PREPARE_FIELD_NUMBER: _ClassVar[int]
    RESET_EXECUTE_FIELD_NUMBER: _ClassVar[int]
    RESET_FINISH_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_LIST_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_RETRY_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_RETRY_ALL_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTER_DISCARD_FIELD_NUMBER: _ClassVar[int]
    control_id: str
    processor: str
    pause: ProcessorPause
    resume: ProcessorResume
    reset_prepare: ResetPrepare
    reset_execute: ResetExecute
    reset_finish: ResetFinish
    dead_letter_list: DeadLetterList
    dead_letter_retry: DeadLetterRetry
    dead_letter_retry_all: DeadLetterRetryAll
    dead_letter_discard: DeadLetterDiscard
    def __init__(self, control_id: _Optional[str] = ..., processor: _Optional[str] = ..., pause: _Optional[_Union[ProcessorPause, _Mapping]] = ..., resume: _Optional[_Union[ProcessorResume, _Mapping]] = ..., reset_prepare: _Optional[_Union[ResetPrepare, _Mapping]] = ..., reset_execute: _Optional[_Union[ResetExecute, _Mapping]] = ..., reset_finish: _Optional[_Union[ResetFinish, _Mapping]] = ..., dead_letter_list: _Optional[_Union[DeadLetterList, _Mapping]] = ..., dead_letter_retry: _Optional[_Union[DeadLetterRetry, _Mapping]] = ..., dead_letter_retry_all: _Optional[_Union[DeadLetterRetryAll, _Mapping]] = ..., dead_letter_discard: _Optional[_Union[DeadLetterDiscard, _Mapping]] = ...) -> None: ...

class ProcessorPause(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ProcessorResume(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResetPrepare(_message.Message):
    __slots__ = ("operation_id", "lease_ms")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_MS_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    lease_ms: int
    def __init__(self, operation_id: _Optional[str] = ..., lease_ms: _Optional[int] = ...) -> None: ...

class ResetExecute(_message.Message):
    __slots__ = ("operation_id", "target")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    target: ResetTarget
    def __init__(self, operation_id: _Optional[str] = ..., target: _Optional[_Union[ResetTarget, _Mapping]] = ...) -> None: ...

class ResetFinish(_message.Message):
    __slots__ = ("operation_id", "aborted")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ABORTED_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    aborted: bool
    def __init__(self, operation_id: _Optional[str] = ..., aborted: _Optional[bool] = ...) -> None: ...

class ResetTarget(_message.Message):
    __slots__ = ("earliest", "latest", "timestamp_ms", "positions")
    EARLIEST_FIELD_NUMBER: _ClassVar[int]
    LATEST_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    POSITIONS_FIELD_NUMBER: _ClassVar[int]
    earliest: bool
    latest: bool
    timestamp_ms: int
    positions: SegmentPositions
    def __init__(self, earliest: _Optional[bool] = ..., latest: _Optional[bool] = ..., timestamp_ms: _Optional[int] = ..., positions: _Optional[_Union[SegmentPositions, _Mapping]] = ...) -> None: ...

class SegmentPositions(_message.Message):
    __slots__ = ("positions",)
    POSITIONS_FIELD_NUMBER: _ClassVar[int]
    positions: _containers.RepeatedCompositeFieldContainer[SegmentPosition]
    def __init__(self, positions: _Optional[_Iterable[_Union[SegmentPosition, _Mapping]]] = ...) -> None: ...

class SegmentPosition(_message.Message):
    __slots__ = ("segment", "position")
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    segment: str
    position: str
    def __init__(self, segment: _Optional[str] = ..., position: _Optional[str] = ...) -> None: ...

class DeadLetterList(_message.Message):
    __slots__ = ("limit",)
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    limit: int
    def __init__(self, limit: _Optional[int] = ...) -> None: ...

class DeadLetterRetry(_message.Message):
    __slots__ = ("letter_id",)
    LETTER_ID_FIELD_NUMBER: _ClassVar[int]
    letter_id: str
    def __init__(self, letter_id: _Optional[str] = ...) -> None: ...

class DeadLetterRetryAll(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class DeadLetterDiscard(_message.Message):
    __slots__ = ("letter_id",)
    LETTER_ID_FIELD_NUMBER: _ClassVar[int]
    letter_id: str
    def __init__(self, letter_id: _Optional[str] = ...) -> None: ...

class ControlReply(_message.Message):
    __slots__ = ("control_id", "ok", "refused", "reset", "dead_letters", "retry", "retry_all")
    CONTROL_ID_FIELD_NUMBER: _ClassVar[int]
    OK_FIELD_NUMBER: _ClassVar[int]
    REFUSED_FIELD_NUMBER: _ClassVar[int]
    RESET_FIELD_NUMBER: _ClassVar[int]
    DEAD_LETTERS_FIELD_NUMBER: _ClassVar[int]
    RETRY_FIELD_NUMBER: _ClassVar[int]
    RETRY_ALL_FIELD_NUMBER: _ClassVar[int]
    control_id: str
    ok: ControlOk
    refused: ControlRefused
    reset: ResetOutcome
    dead_letters: DeadLetterPage
    retry: DeadLetterRetryOutcome
    retry_all: DeadLetterRetryAllOutcome
    def __init__(self, control_id: _Optional[str] = ..., ok: _Optional[_Union[ControlOk, _Mapping]] = ..., refused: _Optional[_Union[ControlRefused, _Mapping]] = ..., reset: _Optional[_Union[ResetOutcome, _Mapping]] = ..., dead_letters: _Optional[_Union[DeadLetterPage, _Mapping]] = ..., retry: _Optional[_Union[DeadLetterRetryOutcome, _Mapping]] = ..., retry_all: _Optional[_Union[DeadLetterRetryAllOutcome, _Mapping]] = ...) -> None: ...

class ControlOk(_message.Message):
    __slots__ = ("detail",)
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    detail: str
    def __init__(self, detail: _Optional[str] = ...) -> None: ...

class ControlRefused(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: ControlRefusal
    message: str
    def __init__(self, code: _Optional[_Union[ControlRefusal, str]] = ..., message: _Optional[str] = ...) -> None: ...

class ResetOutcome(_message.Message):
    __slots__ = ("ranges", "markers_deleted", "reset_handlers")
    RANGES_FIELD_NUMBER: _ClassVar[int]
    MARKERS_DELETED_FIELD_NUMBER: _ClassVar[int]
    RESET_HANDLERS_FIELD_NUMBER: _ClassVar[int]
    ranges: _containers.RepeatedCompositeFieldContainer[ReplayRange]
    markers_deleted: int
    reset_handlers: int
    def __init__(self, ranges: _Optional[_Iterable[_Union[ReplayRange, _Mapping]]] = ..., markers_deleted: _Optional[int] = ..., reset_handlers: _Optional[int] = ...) -> None: ...

class ReplayRange(_message.Message):
    __slots__ = ("segment", "until")
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    segment: str
    until: str
    def __init__(self, segment: _Optional[str] = ..., until: _Optional[str] = ..., **kwargs) -> None: ...

class DeadLetterPage(_message.Message):
    __slots__ = ("letters", "total")
    LETTERS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    letters: _containers.RepeatedCompositeFieldContainer[DeadLetterSummary]
    total: int
    def __init__(self, letters: _Optional[_Iterable[_Union[DeadLetterSummary, _Mapping]]] = ..., total: _Optional[int] = ...) -> None: ...

class DeadLetterSummary(_message.Message):
    __slots__ = ("id", "processor", "source", "segment", "position", "key", "event_id", "event_type", "reason", "error_type", "error_message", "blocked_by", "attempts", "parked_at_ms", "last_attempt_at_ms")
    ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSOR_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SEGMENT_FIELD_NUMBER: _ClassVar[int]
    POSITION_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_BY_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    PARKED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_ATTEMPT_AT_MS_FIELD_NUMBER: _ClassVar[int]
    id: str
    processor: str
    source: str
    segment: str
    position: str
    key: str
    event_id: str
    event_type: str
    reason: str
    error_type: str
    error_message: str
    blocked_by: str
    attempts: int
    parked_at_ms: int
    last_attempt_at_ms: int
    def __init__(self, id: _Optional[str] = ..., processor: _Optional[str] = ..., source: _Optional[str] = ..., segment: _Optional[str] = ..., position: _Optional[str] = ..., key: _Optional[str] = ..., event_id: _Optional[str] = ..., event_type: _Optional[str] = ..., reason: _Optional[str] = ..., error_type: _Optional[str] = ..., error_message: _Optional[str] = ..., blocked_by: _Optional[str] = ..., attempts: _Optional[int] = ..., parked_at_ms: _Optional[int] = ..., last_attempt_at_ms: _Optional[int] = ...) -> None: ...

class DeadLetterRetryOutcome(_message.Message):
    __slots__ = ("result", "detail")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    DETAIL_FIELD_NUMBER: _ClassVar[int]
    result: str
    detail: str
    def __init__(self, result: _Optional[str] = ..., detail: _Optional[str] = ...) -> None: ...

class DeadLetterRetryAllOutcome(_message.Message):
    __slots__ = ("retried", "failed", "blocked", "skipped")
    RETRIED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    BLOCKED_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_FIELD_NUMBER: _ClassVar[int]
    retried: int
    failed: int
    blocked: int
    skipped: int
    def __init__(self, retried: _Optional[int] = ..., failed: _Optional[int] = ..., blocked: _Optional[int] = ..., skipped: _Optional[int] = ...) -> None: ...

class Credentials(_message.Message):
    __slots__ = ("token", "jwt")
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    JWT_FIELD_NUMBER: _ClassVar[int]
    token: str
    jwt: str
    def __init__(self, token: _Optional[str] = ..., jwt: _Optional[str] = ...) -> None: ...

class RegisterAck(_message.Message):
    __slots__ = ("hub_id", "heartbeat_interval_ms", "heartbeat_timeout_ms", "default_timeout_ms", "max_timeout_ms", "context", "type_timeouts_ms")
    class TypeTimeoutsMsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    HUB_ID_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_INTERVAL_MS_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    TYPE_TIMEOUTS_MS_FIELD_NUMBER: _ClassVar[int]
    hub_id: str
    heartbeat_interval_ms: int
    heartbeat_timeout_ms: int
    default_timeout_ms: int
    max_timeout_ms: int
    context: str
    type_timeouts_ms: _containers.ScalarMap[str, int]
    def __init__(self, hub_id: _Optional[str] = ..., heartbeat_interval_ms: _Optional[int] = ..., heartbeat_timeout_ms: _Optional[int] = ..., default_timeout_ms: _Optional[int] = ..., max_timeout_ms: _Optional[int] = ..., context: _Optional[str] = ..., type_timeouts_ms: _Optional[_Mapping[str, int]] = ...) -> None: ...

class Request(_message.Message):
    __slots__ = ("id", "kind", "type", "routing_key", "payload", "headers", "timeout_ms", "mode", "priority", "initial_credit", "max_results")
    class HeadersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    ROUTING_KEY_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    INITIAL_CREDIT_FIELD_NUMBER: _ClassVar[int]
    MAX_RESULTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    kind: RequestKind
    type: str
    routing_key: str
    payload: bytes
    headers: _containers.ScalarMap[str, str]
    timeout_ms: int
    mode: RequestMode
    priority: int
    initial_credit: int
    max_results: int
    def __init__(self, id: _Optional[str] = ..., kind: _Optional[_Union[RequestKind, str]] = ..., type: _Optional[str] = ..., routing_key: _Optional[str] = ..., payload: _Optional[bytes] = ..., headers: _Optional[_Mapping[str, str]] = ..., timeout_ms: _Optional[int] = ..., mode: _Optional[_Union[RequestMode, str]] = ..., priority: _Optional[int] = ..., initial_credit: _Optional[int] = ..., max_results: _Optional[int] = ...) -> None: ...

class Reply(_message.Message):
    __slots__ = ("request_id", "payload", "error")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    payload: bytes
    error: Error
    def __init__(self, request_id: _Optional[str] = ..., payload: _Optional[bytes] = ..., error: _Optional[_Union[Error, _Mapping]] = ...) -> None: ...

class Error(_message.Message):
    __slots__ = ("request_id", "code", "message", "details")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    DETAILS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    code: ErrorCode
    message: str
    details: bytes
    def __init__(self, request_id: _Optional[str] = ..., code: _Optional[_Union[ErrorCode, str]] = ..., message: _Optional[str] = ..., details: _Optional[bytes] = ...) -> None: ...

class ReplyPart(_message.Message):
    __slots__ = ("request_id", "payload", "error", "responder_service", "responder_instance", "seq")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_SERVICE_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_INSTANCE_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    payload: bytes
    error: Error
    responder_service: str
    responder_instance: str
    seq: int
    def __init__(self, request_id: _Optional[str] = ..., payload: _Optional[bytes] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., responder_service: _Optional[str] = ..., responder_instance: _Optional[str] = ..., seq: _Optional[int] = ...) -> None: ...

class ReplyEnd(_message.Message):
    __slots__ = ("request_id", "error", "parts", "failed", "timed_out")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    PARTS_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    TIMED_OUT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    error: Error
    parts: int
    failed: int
    timed_out: int
    def __init__(self, request_id: _Optional[str] = ..., error: _Optional[_Union[Error, _Mapping]] = ..., parts: _Optional[int] = ..., failed: _Optional[int] = ..., timed_out: _Optional[int] = ...) -> None: ...

class Flow(_message.Message):
    __slots__ = ("request_id", "credits")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    CREDITS_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    credits: int
    def __init__(self, request_id: _Optional[str] = ..., credits: _Optional[int] = ...) -> None: ...

class Cancel(_message.Message):
    __slots__ = ("request_id", "reason")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    reason: str
    def __init__(self, request_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class Heartbeat(_message.Message):
    __slots__ = ("sent_at_ms",)
    SENT_AT_MS_FIELD_NUMBER: _ClassVar[int]
    sent_at_ms: int
    def __init__(self, sent_at_ms: _Optional[int] = ...) -> None: ...

class TopologyRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TopologyResponse(_message.Message):
    __slots__ = ("hub_started_at_ms", "now_ms", "heartbeat_timeout_ms", "services", "types", "totals", "hub_id")
    HUB_STARTED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    NOW_MS_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    SERVICES_FIELD_NUMBER: _ClassVar[int]
    TYPES_FIELD_NUMBER: _ClassVar[int]
    TOTALS_FIELD_NUMBER: _ClassVar[int]
    HUB_ID_FIELD_NUMBER: _ClassVar[int]
    hub_started_at_ms: int
    now_ms: int
    heartbeat_timeout_ms: int
    services: _containers.RepeatedCompositeFieldContainer[ServiceTopology]
    types: _containers.RepeatedCompositeFieldContainer[TypeOwnership]
    totals: TopologyTotals
    hub_id: str
    def __init__(self, hub_started_at_ms: _Optional[int] = ..., now_ms: _Optional[int] = ..., heartbeat_timeout_ms: _Optional[int] = ..., services: _Optional[_Iterable[_Union[ServiceTopology, _Mapping]]] = ..., types: _Optional[_Iterable[_Union[TypeOwnership, _Mapping]]] = ..., totals: _Optional[_Union[TopologyTotals, _Mapping]] = ..., hub_id: _Optional[str] = ...) -> None: ...

class ServiceTopology(_message.Message):
    __slots__ = ("service_name", "instances")
    SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    INSTANCES_FIELD_NUMBER: _ClassVar[int]
    service_name: str
    instances: _containers.RepeatedCompositeFieldContainer[InstanceTopology]
    def __init__(self, service_name: _Optional[str] = ..., instances: _Optional[_Iterable[_Union[InstanceTopology, _Mapping]]] = ...) -> None: ...

class InstanceTopology(_message.Message):
    __slots__ = ("instance_id", "connected_since_ms", "last_heartbeat_age_ms", "stale", "in_flight_as_handler", "in_flight_as_sender", "command_types", "query_types", "client_version", "permits_available", "queued", "load_factor", "tags", "connected_hub_ids", "status_age_ms", "shared_query_types", "processors")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_SINCE_MS_FIELD_NUMBER: _ClassVar[int]
    LAST_HEARTBEAT_AGE_MS_FIELD_NUMBER: _ClassVar[int]
    STALE_FIELD_NUMBER: _ClassVar[int]
    IN_FLIGHT_AS_HANDLER_FIELD_NUMBER: _ClassVar[int]
    IN_FLIGHT_AS_SENDER_FIELD_NUMBER: _ClassVar[int]
    COMMAND_TYPES_FIELD_NUMBER: _ClassVar[int]
    QUERY_TYPES_FIELD_NUMBER: _ClassVar[int]
    CLIENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    PERMITS_AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    QUEUED_FIELD_NUMBER: _ClassVar[int]
    LOAD_FACTOR_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    CONNECTED_HUB_IDS_FIELD_NUMBER: _ClassVar[int]
    STATUS_AGE_MS_FIELD_NUMBER: _ClassVar[int]
    SHARED_QUERY_TYPES_FIELD_NUMBER: _ClassVar[int]
    PROCESSORS_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    connected_since_ms: int
    last_heartbeat_age_ms: int
    stale: bool
    in_flight_as_handler: int
    in_flight_as_sender: int
    command_types: _containers.RepeatedScalarFieldContainer[str]
    query_types: _containers.RepeatedScalarFieldContainer[str]
    client_version: str
    permits_available: int
    queued: int
    load_factor: int
    tags: _containers.ScalarMap[str, str]
    connected_hub_ids: _containers.RepeatedScalarFieldContainer[str]
    status_age_ms: int
    shared_query_types: _containers.RepeatedScalarFieldContainer[str]
    processors: _containers.RepeatedCompositeFieldContainer[ProcessorStatus]
    def __init__(self, instance_id: _Optional[str] = ..., connected_since_ms: _Optional[int] = ..., last_heartbeat_age_ms: _Optional[int] = ..., stale: _Optional[bool] = ..., in_flight_as_handler: _Optional[int] = ..., in_flight_as_sender: _Optional[int] = ..., command_types: _Optional[_Iterable[str]] = ..., query_types: _Optional[_Iterable[str]] = ..., client_version: _Optional[str] = ..., permits_available: _Optional[int] = ..., queued: _Optional[int] = ..., load_factor: _Optional[int] = ..., tags: _Optional[_Mapping[str, str]] = ..., connected_hub_ids: _Optional[_Iterable[str]] = ..., status_age_ms: _Optional[int] = ..., shared_query_types: _Optional[_Iterable[str]] = ..., processors: _Optional[_Iterable[_Union[ProcessorStatus, _Mapping]]] = ...) -> None: ...

class TypeOwnership(_message.Message):
    __slots__ = ("type", "kind", "live_instances", "orphaned", "orphaned_for_ms", "shared", "owner_services")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LIVE_INSTANCES_FIELD_NUMBER: _ClassVar[int]
    ORPHANED_FIELD_NUMBER: _ClassVar[int]
    ORPHANED_FOR_MS_FIELD_NUMBER: _ClassVar[int]
    SHARED_FIELD_NUMBER: _ClassVar[int]
    OWNER_SERVICES_FIELD_NUMBER: _ClassVar[int]
    type: str
    kind: RequestKind
    live_instances: int
    orphaned: bool
    orphaned_for_ms: int
    shared: bool
    owner_services: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, type: _Optional[str] = ..., kind: _Optional[_Union[RequestKind, str]] = ..., live_instances: _Optional[int] = ..., orphaned: _Optional[bool] = ..., orphaned_for_ms: _Optional[int] = ..., shared: _Optional[bool] = ..., owner_services: _Optional[_Iterable[str]] = ...) -> None: ...

class TopologyTotals(_message.Message):
    __slots__ = ("services", "instances", "command_types", "query_types", "in_flight", "queued")
    SERVICES_FIELD_NUMBER: _ClassVar[int]
    INSTANCES_FIELD_NUMBER: _ClassVar[int]
    COMMAND_TYPES_FIELD_NUMBER: _ClassVar[int]
    QUERY_TYPES_FIELD_NUMBER: _ClassVar[int]
    IN_FLIGHT_FIELD_NUMBER: _ClassVar[int]
    QUEUED_FIELD_NUMBER: _ClassVar[int]
    services: int
    instances: int
    command_types: int
    query_types: int
    in_flight: int
    queued: int
    def __init__(self, services: _Optional[int] = ..., instances: _Optional[int] = ..., command_types: _Optional[int] = ..., query_types: _Optional[int] = ..., in_flight: _Optional[int] = ..., queued: _Optional[int] = ...) -> None: ...
