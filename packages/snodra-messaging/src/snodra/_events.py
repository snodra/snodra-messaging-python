"""Events: registering handlers, building the event, and publishing it through the outbox.

An event handler is registered on the app (``@app.event_handler``) and runs in the ``default`` processor unless another
is named. :meth:`snodra.App.publish` builds the event (its type, a fresh id, the key, the creation time, the correlation
id, the source, the user id and the trace context), appends it to the message store's outbox in the caller's
transaction, and every instance's outbox relay publishes it to the event store.

The seven headers every event carries are :class:`snodra._eventstore.EventHeaders`: ``type``, ``id``,
``correlationId``, ``timestamp``, ``source``, ``userId`` and ``traceparent``. The relay adds ``type``, ``id``,
``timestamp`` and ``source`` from the outbox entry, so a store never sees a stale copy of them; the entry keeps the
other three.

See https://snodra.damoreira.com/docs/library-authors/messages/ (Events, Event headers) and
https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/ (The rule).
"""

from __future__ import annotations

import datetime as dt
import inspect
import json
import re
from collections.abc import Awaitable, Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from snodra import _traceparent
from snodra._codec import Codec
from snodra._context import _CALLER, _CURRENT, MessageInfo, current_caller, current_message
from snodra._errors import RegistrationClosed
from snodra._eventstore import EventHeaders as EventHeaders
from snodra._eventstore import OutgoingEvent as OutgoingEvent
from snodra._ids import new_id
from snodra._messages import type_name
from snodra._registry import _message_class, label_of
from snodra._store import OutboxEntry, check_outbox_payload, now_ms

if TYPE_CHECKING:
    from snodra._telemetry import Telemetry

DEFAULT_PROCESSOR: Final = "default"
"""The processor of the event handlers registered without one."""

_PROCESSOR_NAME: Final = re.compile(r"[a-z0-9][a-z0-9-]{0,47}")
"""The shape of a named processor."""

PUBLISH_NEEDS_STORE: Final = (
    "publishing an event needs a message store: pass store= to App "
    "(snodra.MemoryStore runs the in-memory one, not durable: tests and local runs)"
)
"""What publishing without a message store fails with."""

PUBLISH_NEEDS_EVENT_STORE: Final = (
    "publishing an event needs an event store: pass event_store= to App "
    "(snodra.MemoryEventStore runs the in-memory one, not durable: tests and local runs)"
)
"""What publishing without an event store fails with."""


def processor_subscription(service: str, processor: str) -> str:
    """The name a processor's subscription has on the event store: ``<service>.<processor>``."""
    return f"{service}.{processor}"


def processor_marker(processor: str, event_id: str) -> str:
    """The id of the stage-1 marker a processor records for the event ``event_id``: ``processor:<processor>:<eventId>``.

    The marker is recorded in the handler's transaction; that is what makes a redelivered event a duplicate.
    """
    return f"processor:{processor}:{event_id}"


def event_timestamp(created_at_ms: int) -> str:
    """When an event was created, as its ``timestamp`` header: RFC 3339 in UTC, always with milliseconds."""
    seconds, millis = divmod(created_at_ms, 1000)
    at = dt.datetime.fromtimestamp(seconds, tz=dt.UTC)
    return at.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


def event_message(event_id: str, type_name: str, headers: Mapping[str, str]) -> MessageInfo:
    """What an event handler reads as :func:`snodra.current_message`."""
    correlation_id = headers.get(EventHeaders.CORRELATION_ID)
    return MessageInfo(
        event_id,
        type_name,
        event_id if correlation_id is None else correlation_id,
        # A fresh mapping: the delivery's headers are never exposed for writing.
        MappingProxyType(dict(headers)),
    )


@dataclass(frozen=True, slots=True)
class EventHandler:
    """One registered event handler: its function, the event class it takes, and where it runs."""

    fn: Callable[[Any], Awaitable[Any]]
    message: type
    type: str
    label: str
    processor: str
    replay: bool


class EventRegistry:
    """The event handlers by processor and event type; closed when the app starts.

    Handlers of one type in one processor all run, in registration order; one event type may be handled in several
    processors, and each processor gets every record.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, dict[str, list[EventHandler]]] = {}
        self._closed = False

    def close(self) -> None:
        """No handler is registered from now on."""
        self._closed = True

    def add(self, fn: object, *, processor: str = DEFAULT_PROCESSOR, replay: bool = True) -> EventHandler:
        """Registers fn as a handler of the event type its first parameter is annotated with, in ``processor``.

        Raises :class:`RegistrationClosed` once closed, ``TypeError`` for a function that cannot be an event handler
        and ``ValueError`` for a processor name outside ``[a-z0-9][a-z0-9-]{0,47}``.
        """
        label = label_of(fn)
        if self._closed:
            raise RegistrationClosed(f"event handler {label}: handlers are registered before the app starts")
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"event handler {label} must be an async def taking the event")
        if not isinstance(processor, str) or processor == "":
            raise ValueError(f"event handler {label}: a processor name is a non-empty text")
        if _PROCESSOR_NAME.fullmatch(processor) is None:
            raise ValueError(
                f"event handler {label}: the processor name {processor!r} matches [a-z0-9][a-z0-9-]{{0,47}} at most"
            )
        if not isinstance(replay, bool):
            raise TypeError(f"event handler {label}: replay must be True or False, got {replay!r}")
        message = _message_class(fn, label)
        handler = EventHandler(
            fn=fn,
            message=message,
            type=type_name(message),
            label=label,
            processor=processor,
            replay=replay,
        )
        self._handlers.setdefault(processor, {}).setdefault(handler.type, []).append(handler)
        return handler

    def handlers(self, processor: str, type_: str) -> tuple[EventHandler, ...]:
        """The handlers of ``type_`` in ``processor``, in registration order."""
        return tuple(self._handlers.get(processor, {}).get(type_, ()))

    def processors(self) -> list[str]:
        """Every processor with a handler, ``default`` first, then the others by name."""
        names = sorted(self._handlers)
        if DEFAULT_PROCESSOR in names:
            names.remove(DEFAULT_PROCESSOR)
            names.insert(0, DEFAULT_PROCESSOR)
        return names

    def types(self, processor: str) -> list[str]:
        """The event types with a handler in ``processor``, sorted."""
        return sorted(self._handlers.get(processor, {}))


_CURRENT_EVENT_USER: Final[ContextVar[str | None]] = ContextVar("snodra.current_event_user", default=None)


def current_event_user() -> str | None:
    """The ``userId`` of the event being handled, ``""`` when it carries none; ``None`` outside an event handler."""
    return _CURRENT_EVENT_USER.get()


@contextmanager
def event_handling(info: MessageInfo, user_id: str) -> Iterator[None]:
    """Binds what an event handler (and everything it publishes) reads as the message being handled."""
    current = _CURRENT.set(info)
    calling = _CALLER.set(None)
    user = _CURRENT_EVENT_USER.set(user_id)
    try:
        yield
    finally:
        _CURRENT_EVENT_USER.reset(user)
        _CALLER.reset(calling)
        _CURRENT.reset(current)


def event_headers(*, telemetry: Telemetry | None = None) -> dict[str, str]:
    """The headers of an event published now: its correlation id, the acting user id and, when the work that publishes
    carries one, its valid trace context.

    The correlation id is the handled message's, or a fresh UUID when there is none or it is empty; a published event
    never carries an empty ``correlationId`` (SG-9). The user id is the verified caller's subject for a command or a
    query and the handled event's ``userId`` inside an event handler; a token never crosses into an event.
    """
    info = current_message()
    # TODO(spec-gap): SG-23
    headers = {
        EventHeaders.CORRELATION_ID: new_id() if info is None or not info.correlation_id else info.correlation_id,
        EventHeaders.USER_ID: _acting_user(),
    }
    if info is not None:
        trace = info.headers.get(EventHeaders.TRACEPARENT)
        if isinstance(trace, str) and _traceparent.valid(trace):
            headers[EventHeaders.TRACEPARENT] = trace
    if telemetry is not None:
        scratch = dict(headers)
        telemetry.inject(scratch)
        trace = scratch.get(EventHeaders.TRACEPARENT)
        if isinstance(trace, str) and _traceparent.valid(trace):
            headers[EventHeaders.TRACEPARENT] = trace
    return headers


def _acting_user() -> str:
    event_user = _CURRENT_EVENT_USER.get()
    if event_user is not None:
        return event_user
    caller = current_caller()
    return "" if caller is None else caller.subject


def outbox_entry(
    event: object,
    *,
    key: str | None,
    seq: int,
    codec: Codec,
    telemetry: Telemetry | None = None,
    created_at_ms: int | None = None,
) -> OutboxEntry:
    """The outbox entry of an event published now, with the headers of :func:`event_headers`.

    The payload is the event encoded by ``codec`` and checked by :func:`snodra._store.check_outbox_payload` before an
    add-on sees it: what every store refuses is refused here, naming the event. ``created_at_ms`` is the application's
    clock (the wall clock when none is given); it is the event's ``timestamp`` header.
    """
    if key is not None and not isinstance(key, str):
        raise TypeError(f"an event key is text or None, got {key!r}")
    type_ = type_name(event)
    payload = codec.encode(event)
    check_outbox_payload(type_, payload)
    return OutboxEntry(
        id=new_id(),
        type_name=type_,
        key=key,
        headers=event_headers(telemetry=telemetry),
        payload=payload,
        created_at_ms=now_ms() if created_at_ms is None else created_at_ms,
        seq=seq,
    )


def outgoing_event(entry: OutboxEntry, *, source: str, service: str) -> OutgoingEvent:
    """The event an outbox entry becomes on the event store's ``source``: its stored headers, plus ``type``, ``id``,
    ``timestamp`` (its creation) and ``source`` (the publishing service)."""
    headers = dict(entry.headers)
    headers[EventHeaders.TYPE] = entry.type_name
    headers[EventHeaders.ID] = entry.id
    headers[EventHeaders.TIMESTAMP] = event_timestamp(entry.created_at_ms)
    headers[EventHeaders.SOURCE] = service
    return OutgoingEvent(
        source=source,
        id=entry.id,
        type_name=entry.type_name,
        key=entry.key,
        headers=headers,
        payload=entry.payload,
    )


def is_json_object(payload: bytes) -> bool:
    """Whether ``payload`` is a JSON object, the only event payload the suite holds.

    ``NaN``, ``Infinity`` and ``-Infinity`` are not JSON and are refused, as the outbox contract refuses them.
    """
    if not payload.lstrip().startswith(b"{"):
        return False
    try:
        value = json.loads(payload.decode("utf-8"), parse_constant=_no_constant)
    except (UnicodeDecodeError, ValueError, RecursionError):
        return False
    return isinstance(value, dict)


def _no_constant(token: str) -> None:
    raise ValueError(f"{token} is not JSON")
