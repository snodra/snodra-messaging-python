"""The saga model: saga classes, their handlers and the recording context they run with.

A saga is a long-running process of the service that owns it. ``@app.saga(name)`` marks the class, the decorators of
this module mark its handlers, and its fields are the state persisted as JSON after every handled message::

    @app.saga("payment")
    class PaymentSaga:
        order_id: str = ""
        payment_ref: str = ""

        @saga_start("orderId")
        async def placed(self, event: OrderPlaced, ctx: SagaContext) -> None:
            self.order_id = event.order_id
            ctx.associate_with("paymentRef", self.payment_ref)
            ctx.send(ChargeCard(self.payment_ref))

        @saga_event("paymentRef", end=True)
        async def charged(self, event: ChargeSucceeded, ctx: SagaContext) -> None: ...

        @saga_deadline("charge-timeout", end=True)
        async def timed_out(self, ctx: SagaContext) -> None: ...

        @saga_command_failed
        async def failed(self, event: SagaCommandFailed, ctx: SagaContext) -> None: ...

Handlers never send or publish directly: :meth:`SagaContext.send`, :meth:`~SagaContext.publish` and
:meth:`~SagaContext.schedule` record messages and deadlines in the saga's transaction, and the runtime's relay sends
them after the commit, in order, under the saga's lease. See
https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-4-sagas.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime as dt
import inspect
import json
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Final, get_origin, get_type_hints

from snodra._codec import Codec
from snodra._errors import ConfigurationError, RegistrationClosed
from snodra._messages import message_type, type_name
from snodra._store import (
    Association,
    DeadlineDocument,
    DeadlineStatus,
    DispatchKind,
    SagaDispatchDocument,
)

COMMAND_FAILED_TYPE: Final = "snodra.SagaCommandFailed"
"""The wire type of the local failure notice."""

SEQ_STRIDE: Final = 1000
"""A handling records at most this many messages; ``seq = version * SEQ_STRIDE + index``."""

_START_KEY_PART: Final = re.compile(r"[%|=]")


class SagaError(ConfigurationError):
    """A saga definition the library refuses at startup: the message names the saga and the handler."""


@message_type(COMMAND_FAILED_TYPE)
@dataclass(frozen=True, slots=True)
class SagaCommandFailed:
    """The local event a saga receives when a command it recorded was given up on."""

    saga_id: str
    command_type: str
    idempotency_key: str
    reason: str
    message: str

    @property
    def notice_id(self) -> str:
        """The message id: ``failed:<sagaId>:<seq>``, where the key is ``<sagaId>:<seq>``."""
        seq = self.idempotency_key.rsplit(":", 1)[-1]
        return f"failed:{self.saga_id}:{seq}"


def start_key(type_name_: str, key: str, value: str) -> str:
    """``<type>|<key>=<value>`` with ``%``, ``|`` and ``=`` escaped in each part, so two pairs never collide."""
    return f"{_escape(type_name_)}|{_escape(key)}={_escape(value)}"


def _escape(part: str) -> str:
    return part.replace("%", "%25").replace("|", "%7C").replace("=", "%3D")


@dataclass(frozen=True, slots=True)
class SagaEventBinding:
    """One handler of an event: its function, the event type, its association and its creation/ending rules."""

    fn: Callable[..., Awaitable[Any]]
    message: type
    event_type: str
    association: str | None
    key: str | None
    header: str | None
    start: bool
    force_new: bool
    end: bool
    by_id: bool
    label: str

    def association_value(self, event: object, headers: Mapping[str, str]) -> str | None:
        """The association value of ``event``: the header's value when configured, else the event field's."""
        if self.header is not None:
            return headers.get(self.header)
        if self.association is None:
            return None
        value = getattr(event, self.association, None)
        return None if value is None else str(value)


@dataclass(frozen=True, slots=True)
class SagaDeadlineBinding:
    """One deadline handler of a saga."""

    fn: Callable[..., Awaitable[Any]]
    name: str
    end: bool
    payload_type: type | None
    label: str


@dataclass(frozen=True, slots=True)
class SagaDefinition:
    """A discovered saga class and its handlers."""

    name: str
    cls: type
    events: tuple[SagaEventBinding, ...]
    deadlines: tuple[SagaDeadlineBinding, ...]

    @property
    def event_types(self) -> tuple[str, ...]:
        """The event type names some handler reacts to, in registration order."""
        seen: dict[str, None] = {}
        for binding in self.events:
            seen.setdefault(binding.event_type, None)
        return tuple(seen)

    @property
    def starts(self) -> tuple[SagaEventBinding, ...]:
        """The start handlers, in registration order."""
        return tuple(binding for binding in self.events if binding.start)

    @property
    def notice(self) -> SagaEventBinding | None:
        """The command-failed handler, when the saga has one."""
        return next((binding for binding in self.events if binding.by_id), None)


class SagaModel:
    """The saga definitions of one app: open until ``start``, then read-only."""

    def __init__(self, codec: Codec | None = None) -> None:
        from snodra._codec import JsonCodec

        self._codec: Codec = JsonCodec() if codec is None else codec
        self._by_name: dict[str, SagaDefinition] = {}
        self._by_event: dict[str, list[SagaDefinition]] = {}
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def add(self, cls: type, name: str | None = None) -> type:
        """Registers ``cls`` as a saga; raises :class:`SagaError` for a definition the library refuses."""
        if self._closed:
            raise RegistrationClosed(f"saga {cls.__qualname__}: sagas are registered before the app starts")
        definition = _definition(cls, name)
        if definition.name in self._by_name:
            other = self._by_name[definition.name].cls
            raise SagaError(
                f"saga {cls.__qualname__}: the type name {definition.name!r} is already used by {other.__qualname__}"
            )
        self._by_name[definition.name] = definition
        for event_type in definition.event_types:
            self._by_event.setdefault(event_type, []).append(definition)
        return cls

    def definitions(self) -> tuple[SagaDefinition, ...]:
        return tuple(self._by_name.values())

    def definition(self, name: str) -> SagaDefinition | None:
        return self._by_name.get(name)

    def interested(self, event_type: str) -> bool:
        return event_type in self._by_event

    def for_event(self, event_type: str) -> tuple[SagaDefinition, ...]:
        return tuple(self._by_event.get(event_type, ()))

    def event_types(self) -> set[str]:
        return set(self._by_event)

    def encode_state(self, definition: SagaDefinition, instance: object) -> str:
        """The JSON text of a saga instance; the codec writes a dataclass, plain fields use JSON."""
        if dataclasses.is_dataclass(instance):
            return self._codec.encode(instance).decode()
        return json.dumps(_plain_state(instance), separators=(",", ":"), ensure_ascii=False, default=_json_default)

    def decode_state(self, definition: SagaDefinition, state: str) -> object:
        """A fresh instance with the stored state."""
        if dataclasses.is_dataclass(definition.cls):
            return self._codec.decode(state.encode(), definition.cls)
        instance = definition.cls()
        values = json.loads(state)
        if not isinstance(values, dict):
            raise SagaError(f"saga {definition.name}: its stored state is not a JSON object")
        for name, value in values.items():
            setattr(instance, name, value)
        return instance


def _plain_state(instance: object) -> dict[str, object]:
    values: dict[str, object] = {}
    for cls in reversed(type(instance).__mro__):
        for name, annotation in getattr(cls, "__annotations__", {}).items():
            if get_origin(annotation) is ClassVar:
                continue
            if hasattr(instance, name):
                values[name] = getattr(instance, name)
    values.update(vars(instance))
    return values


def _json_default(value: object) -> object:
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.total_seconds()
    if isinstance(value, uuid.UUID):
        return str(value)
    raise TypeError(f"a saga state field of type {type(value).__name__} has no JSON form")


# ---- registration decorators ---------------------------------------------------------------------


def _mark(role: str, fn: Callable[..., Awaitable[Any]], **values: object) -> Callable[..., Awaitable[Any]]:
    setattr(fn, "_snodra_saga", {"role": role, **values})  # noqa: B010
    return fn


def saga_start(
    association: str | None = None,
    *,
    key: str | None = None,
    header: str | None = None,
    force_new: bool = False,
    end: bool = False,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Marks an event handler that may create an instance. ``force_new`` always creates one, without a start key."""

    def mark(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        return _mark(
            "event",
            fn,
            association=association,
            key=key or association or header,
            header=header,
            start=True,
            force_new=force_new,
            end=end,
            by_id=False,
        )

    return mark


def saga_event(
    association: str | None = None,
    *,
    key: str | None = None,
    header: str | None = None,
    end: bool = False,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Marks an event handler that runs on the instances associated with the event's value."""

    def mark(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        return _mark(
            "event",
            fn,
            association=association,
            key=key or association or header,
            header=header,
            start=False,
            force_new=False,
            end=end,
            by_id=False,
        )

    return mark


def saga_deadline(
    name: str, *, end: bool = False
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Marks a deadline handler of the named deadline."""

    def mark(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        return _mark("deadline", fn, name=name, end=end)

    return mark


def saga_command_failed(
    fn: Callable[..., Awaitable[Any]] | None = None,
    *,
    end: bool = False,
) -> Callable[..., Awaitable[Any]] | Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Marks the handler of :class:`SagaCommandFailed`; it is routed to the sending instance by its id."""

    def mark(handler: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        return _mark(
            "event",
            handler,
            association=None,
            key=None,
            header=None,
            start=False,
            force_new=False,
            end=end,
            by_id=True,
        )

    return mark if fn is None else mark(fn)


# ---- discovery and validation --------------------------------------------------------------------


def _definition(cls: type, name: str | None) -> SagaDefinition:
    if not isinstance(cls, type):
        raise SagaError(f"a saga is a class, not {cls!r}")
    type_name_ = f"{cls.__module__}.{cls.__qualname__}" if name is None else name
    if not isinstance(type_name_, str) or not type_name_.strip():
        raise SagaError(f"saga {cls.__qualname__}: its type name must be non-empty text")
    try:
        instance = cls()
    except Exception as error:
        raise SagaError(f"saga {cls.__qualname__}: it needs a no-argument constructor: {error}") from error
    _check_state(cls, type_name_, instance)
    events: list[SagaEventBinding] = []
    deadlines: list[SagaDeadlineBinding] = []
    for method_name, member in vars(cls).items():
        metadata = getattr(member, "_snodra_saga", None)
        if not isinstance(metadata, dict):
            continue
        if not inspect.iscoroutinefunction(member):
            raise SagaError(f"saga {cls.__qualname__} handler {method_name}: it must be an async def")
        label = f"{cls.__qualname__}.{method_name}"
        if metadata["role"] == "deadline":
            deadlines.append(_deadline_binding(label, member, metadata))
        else:
            events.append(_event_binding(label, member, metadata))
    if not any(binding.start for binding in events):
        raise SagaError(f"saga {cls.__qualname__}: it needs at least one @saga_start handler")
    names = [binding.name for binding in deadlines]
    if len(set(names)) != len(names):
        raise SagaError(f"saga {cls.__qualname__}: a deadline name has one handler at most")
    return SagaDefinition(type_name_, cls, tuple(events), tuple(deadlines))


def _event_binding(label: str, fn: Callable[..., Awaitable[Any]], metadata: dict[str, object]) -> SagaEventBinding:
    hints = _hints(label, fn)
    parameters = list(inspect.signature(fn).parameters.values())[1:]
    if not parameters:
        raise SagaError(f"saga handler {label}: it needs the event as its first parameter")
    event_type = hints.get(parameters[0].name, parameters[0].annotation)
    if not isinstance(event_type, type):
        raise SagaError(f"saga handler {label}: its first parameter must be annotated with the event class")
    try:
        wire = type_name(event_type)
    except TypeError as error:
        raise SagaError(f"saga handler {label}: {error}") from error
    by_id = bool(metadata.get("by_id"))
    if by_id and not issubclass(event_type, SagaCommandFailed):
        raise SagaError(f"saga handler {label}: a @saga_command_failed handler takes SagaCommandFailed")
    association = metadata.get("association")
    header = metadata.get("header")
    key = metadata.get("key")
    if not by_id and association is None and header is None:
        raise SagaError(f"saga handler {label}: give an association property or header")
    association_name = None if association is None else str(association)
    if association_name is not None and not _has_field(event_type, association_name):
        raise SagaError(f"saga handler {label}: the event {event_type.__qualname__} has no field {association_name!r}")
    if header is not None and not isinstance(header, str):
        raise SagaError(f"saga handler {label}: a header association is text")
    _check_context_parameters(label, parameters[1:])
    return SagaEventBinding(
        fn=fn,
        message=event_type,
        event_type=COMMAND_FAILED_TYPE if by_id else wire,
        association=association_name,
        key=None if key is None else str(key),
        header=None if header is None else header,
        start=bool(metadata.get("start")),
        force_new=bool(metadata.get("force_new")),
        end=bool(metadata.get("end")),
        by_id=by_id,
        label=label,
    )


def _deadline_binding(
    label: str, fn: Callable[..., Awaitable[Any]], metadata: dict[str, object]
) -> SagaDeadlineBinding:
    hints = _hints(label, fn)
    parameters = list(inspect.signature(fn).parameters.values())[1:]
    payload_type: type | None = None
    if parameters:
        first = parameters[0]
        annotation = hints.get(first.name, first.annotation)
        if annotation is not SagaContext and annotation != "SagaContext":
            if not isinstance(annotation, type) and get_origin(annotation) is None:
                raise SagaError(f"saga handler {label}: its first parameter must be a payload class or SagaContext")
            payload_type = annotation
        _check_context_parameters(label, parameters[1:] if payload_type is not None else parameters)
    return SagaDeadlineBinding(
        fn=fn, name=str(metadata["name"]), end=bool(metadata.get("end")), payload_type=payload_type, label=label
    )


def _has_field(cls: type, name: str) -> bool:
    if dataclasses.is_dataclass(cls):
        return any(field.name == name for field in dataclasses.fields(cls))
    return hasattr(cls, name)


def _hints(label: str, fn: Callable[..., Awaitable[Any]]) -> dict[str, Any]:
    try:
        return get_type_hints(fn)
    except Exception as error:
        raise SagaError(f"saga handler {label}: its annotations cannot be read: {error}") from error


def _check_context_parameters(label: str, parameters: Sequence[inspect.Parameter]) -> None:
    for parameter in parameters:
        if parameter.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            raise SagaError(f"saga handler {label}: it takes no *args or **kwargs")
        annotation = parameter.annotation
        if annotation not in (SagaContext, "SagaContext", Any):
            raise SagaError(f"saga handler {label}: parameter {parameter.name!r} must be a SagaContext")


def _check_state(cls: type, name: str, instance: object) -> None:
    try:
        if dataclasses.is_dataclass(instance):
            from snodra._codec import JsonCodec

            codec = JsonCodec()
            dataclass_text = codec.encode(instance)
            codec.decode(dataclass_text, cls)
        else:
            plain_text = json.dumps(_plain_state(instance), default=_json_default)
            if not isinstance(json.loads(plain_text), dict):
                raise ValueError("not a JSON object")
    except Exception as error:
        raise SagaError(f"saga {name}: a fresh instance does not round-trip through the codec: {error}") from error


# ---- the recording context -----------------------------------------------------------------------


class SagaContext:
    """What a saga handler uses to change its instance and record what it sends.

    ``send`` and ``publish`` record dispatch entries, ``schedule``/``cancel_deadline`` record deadline changes, and
    ``associate_with``/``remove_association_with``/``end`` change the instance. Everything is written in the saga's
    transaction after the handler returns; a handler never sends directly.
    """

    def __init__(
        self,
        *,
        saga_id: str,
        version: int,
        now_ms: int,
        correlation_id: str,
        traceparent: str | None,
        codec: Codec,
        associations: Sequence[Association],
        type_name_: str,
        message_id: str = "",
        event_type: str = "",
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self.saga_id = saga_id
        self.version = version
        self.correlation_id = correlation_id
        self.traceparent = traceparent
        self.message_id = message_id
        self.event_type = event_type
        self.headers = dict(headers or {})
        self._codec = codec
        self._now_ms = now_ms
        self._associations = list(associations)
        self._type_name = type_name_
        self._dispatch: list[SagaDispatchDocument] = []
        self._deadline_ops: list[tuple[str, object]] = []
        self._ended = False

    @property
    def associations(self) -> tuple[Association, ...]:
        return tuple(self._associations)

    @property
    def ended(self) -> bool:
        return self._ended

    @property
    def dispatch(self) -> tuple[SagaDispatchDocument, ...]:
        return tuple(self._dispatch)

    @property
    def deadline_ops(self) -> tuple[tuple[str, object], ...]:
        return tuple(self._deadline_ops)

    def associate_with(self, key: str, value: str) -> None:
        association = Association(key, value)
        if association not in self._associations:
            self._associations.append(association)

    def remove_association_with(self, key: str, value: str) -> None:
        with contextlib.suppress(ValueError):
            self._associations.remove(Association(key, value))

    def end(self) -> None:
        self._ended = True

    def send(self, command: object, routing_key: str | None = None) -> None:
        """Records a command for the relay; ``routing_key`` is the hub routing key."""
        self._record(DispatchKind.COMMAND, command, routing_key)

    def publish(self, event: object, key: str | None = None) -> None:
        """Records an event for the outbox; ``key`` is its key."""
        self._record(DispatchKind.EVENT, event, key)

    def _record(self, kind: DispatchKind, message: object, routing_key: str | None) -> None:
        if len(self._dispatch) >= SEQ_STRIDE - 1:
            raise SagaError(
                f"saga handler of {self.saga_id}: at most {SEQ_STRIDE - 1} messages may be recorded per handling"
            )
        wire = type_name(message)
        payload = self._codec.encode(message).decode()
        headers = {"correlation-id": self.correlation_id or self.saga_id}
        if self.traceparent:
            headers["traceparent"] = self.traceparent
        seq = self.version * SEQ_STRIDE + len(self._dispatch)
        self._dispatch.append(
            SagaDispatchDocument(
                id=f"{self.saga_id}:{seq}",
                saga_id=self.saga_id,
                seq=seq,
                kind=kind,
                type_name=wire,
                payload=payload,
                headers=headers,
                routing_key=routing_key,
                created_at_ms=self._now_ms,
            )
        )

    def schedule(
        self,
        after: float | dt.timedelta | dt.datetime | None = None,
        name: str = "",
        payload: object | None = None,
        *,
        at: dt.datetime | int | None = None,
    ) -> str:
        """Records a deadline relative to now (seconds or timedelta) or at an instant; returns its id."""
        if not name:
            raise ValueError("a deadline needs a name")
        if at is not None:
            due = _at_ms(at)
        elif isinstance(after, dt.datetime):
            due = _at_ms(after)
        elif isinstance(after, dt.timedelta):
            due = self._now_ms + int(after.total_seconds() * 1000)
        elif isinstance(after, int | float) and not isinstance(after, bool):
            due = self._now_ms + int(float(after) * 1000)
        else:
            raise TypeError("schedule needs a number of seconds, a timedelta or an instant")
        deadline_id = str(uuid.uuid4())
        encoded = None if payload is None else self._codec.encode(payload).decode()
        payload_type = None if payload is None else _payload_type(payload)
        self._deadline_ops.append(
            (
                "schedule",
                DeadlineDocument(
                    id=deadline_id,
                    saga_type=self._type_name,
                    saga_id=self.saga_id,
                    name=name,
                    payload=encoded,
                    payload_type=payload_type,
                    due_at_ms=due,
                    status=DeadlineStatus.SCHEDULED,
                    claimed_by=None,
                    claimed_until_ms=None,
                    finished_at_ms=None,
                    correlation_id=self.correlation_id or None,
                ),
            )
        )
        return deadline_id

    def cancel_deadline(self, deadline_id: str) -> None:
        self._deadline_ops.append(("cancel", deadline_id))

    def cancel(self, deadline_id: str) -> None:
        """Alias of :meth:`cancel_deadline`, as the reference libraries spell it."""
        self.cancel_deadline(deadline_id)

    def cancel_deadlines(self, name: str) -> None:
        self._deadline_ops.append(("cancel_name", name))

    cancel_all = cancel_deadlines


def _payload_type(payload: object) -> str:
    try:
        return type_name(payload)
    except TypeError:
        return f"{type(payload).__module__}.{type(payload).__qualname__}"


def _at_ms(value: dt.datetime | int) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if not isinstance(value, dt.datetime):
        raise TypeError("a deadline instant is a datetime or epoch milliseconds")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("a deadline instant needs a time zone")
    return int(value.timestamp() * 1000)


__all__ = [
    "COMMAND_FAILED_TYPE",
    "SEQ_STRIDE",
    "SagaCommandFailed",
    "SagaContext",
    "SagaDeadlineBinding",
    "SagaDefinition",
    "SagaError",
    "SagaEventBinding",
    "SagaModel",
    "saga_command_failed",
    "saga_deadline",
    "saga_event",
    "saga_start",
    "start_key",
]
