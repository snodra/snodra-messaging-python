"""Stage 2 of local event dispatch: applying an event to every interested saga type.

For each (event, saga type) one transaction records the marker ``saga:<type>:<eventId>``, runs the handlers of the
matching instances, and writes the new state, the dispatch entries and the deadline changes together. An optimistic
version makes one instance apply one event at a time; a conflict retries the event, and an event whose handler keeps
failing is parked in ``saga_failures`` with its marker. The consumer acknowledges the record only after this stage.

See https://snodra.damoreira.com/docs/library-authors/writing-a-library/#level-4-sagas.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from snodra._backoff import jitter
from snodra._codec import Codec
from snodra._events import EventHeaders
from snodra._messages import type_name
from snodra._saga import (
    COMMAND_FAILED_TYPE,
    SagaCommandFailed,
    SagaContext,
    SagaDefinition,
    SagaError,
    SagaEventBinding,
    SagaModel,
    start_key,
)
from snodra._store import (
    Association,
    Conflict,
    DeadlineDocument,
    MessageStore,
    SagaDocument,
    SagaFailureDocument,
    now_ms,
)
from snodra._timings import Timings

log = logging.getLogger(__name__)

MAX_ERROR_CHARS: Final = 2000
"""A parked failure's error text is cut to this many characters."""


class _VersionConflict(Exception):
    """The stored version or start key changed after this attempt read it: the attempt is retried."""


class _GaveUp(Exception):
    """Every attempt failed; the event is parked."""

    def __init__(self, last: BaseException, attempts: int) -> None:
        super().__init__(str(last))
        self.last = last
        self.attempts = attempts


@dataclass(frozen=True, slots=True)
class _Planned:
    """One instance's new document and the effects to write with it."""

    document: SagaDocument
    expected_version: int | None
    entries: tuple[object, ...]
    deadline_ops: tuple[tuple[str, object], ...]
    created: bool


class SagaRuntime:
    """The saga stage of one app: its definitions, transactions and retries."""

    def __init__(
        self,
        model: SagaModel,
        store: MessageStore,
        codec: Codec,
        *,
        clock: Callable[[], int] = now_ms,
        timings: Timings,
    ) -> None:
        self._model = model
        self._store = store
        self._codec = codec
        self._clock = clock
        self._timings = timings
        self._lock = asyncio.Lock()
        self._running = False

    @property
    def event_types(self) -> set[str]:
        """The event types some saga reacts to."""
        return self._model.event_types()

    def interested(self, event_type: str) -> bool:
        return self._model.interested(event_type)

    def definition(self, name: str) -> SagaDefinition | None:
        return self._model.definition(name)

    async def process(
        self,
        event_type: str,
        payload: bytes,
        headers: Mapping[str, str],
        event_id: str,
    ) -> bool:
        """Applies one event to every interested saga type; whether it reached at least one instance."""
        applied = False
        for definition in self._model.for_event(event_type):
            kind = await self._deliver(definition, event_type, payload, headers, event_id)
            if kind != "no_match":
                applied = True
        return applied

    async def process_notice(
        self,
        notice: SagaCommandFailed,
        headers: Mapping[str, str] | None = None,
    ) -> bool:
        """Applies a local ``SagaCommandFailed`` to its instance by id; whether a handler took it."""
        payload = self._codec.encode(notice)
        merged = dict(headers or {})
        merged.setdefault(EventHeaders.ID, notice.notice_id)
        merged.setdefault(EventHeaders.CORRELATION_ID, merged.get("correlation-id", notice.notice_id))
        return await self.process(COMMAND_FAILED_TYPE, payload, merged, notice.notice_id)

    async def apply_deadline(self, deadline: DeadlineDocument) -> bool:
        """Fires one claimed deadline; a conflict retries, a handler failure is left for the next claim."""
        definition = self._model.definition(deadline.saga_type)
        if definition is None:
            raise SagaError(
                f"deadline {deadline.id} belongs to saga type {deadline.saga_type!r}, which this service does not have"
            )
        try:
            return await self._with_retries(
                f"deadline {deadline.id} of saga {deadline.saga_id}",
                lambda: self._fire(definition, deadline),
                park=False,
            )
        except _GaveUp as error:
            raise error.last from error

    # ---- delivery -------------------------------------------------------------------------------

    async def _deliver(
        self,
        definition: SagaDefinition,
        event_type: str,
        payload: bytes,
        headers: Mapping[str, str],
        event_id: str,
    ) -> str:
        try:
            return await self._with_retries(
                f"saga type {definition.name} event {event_id}",
                lambda: self._attempt(definition, event_type, payload, headers, event_id),
                park=True,
            )
        except _GaveUp as failure:
            await self._park(definition, event_type, payload, headers, event_id, failure)
            return "parked"

    async def _with_retries[T](
        self,
        what: str,
        attempt: Callable[[], Awaitable[T]],
        *,
        park: bool,
    ) -> T:
        conflicts = 0
        failures = 0
        max_attempts = self._timings.saga_stage_max_attempts
        while True:
            try:
                return await attempt()
            except asyncio.CancelledError:
                raise
            except (Conflict, _VersionConflict) as error:
                conflicts += 1
                if conflicts >= max_attempts:
                    raise _GaveUp(error, conflicts + failures) from error
                log.debug("%s conflicted on attempt %d: %s", what, conflicts + failures, error)
                await asyncio.sleep(self._stage_wait(conflicts + 1))
            except Exception as error:
                failures += 1
                if failures >= min(2, max_attempts):
                    raise _GaveUp(error, conflicts + failures) from error
                log.debug("%s failed on attempt %d: %s", what, conflicts + failures, error)
                await asyncio.sleep(min(self._timings.saga_stage_initial_backoff, 0.05))

    def _stage_wait(self, attempt: int) -> float:
        base = min(
            self._timings.saga_stage_initial_backoff * 2 ** max(0, attempt - 2),
            self._timings.saga_stage_max_backoff,
        )
        return jitter(base, self._timings.saga_stage_max_backoff, random.random())

    async def _attempt(
        self,
        definition: SagaDefinition,
        event_type: str,
        payload: bytes,
        headers: Mapping[str, str],
        event_id: str,
    ) -> str:
        now = self._clock()
        marker_id = f"saga:{definition.name}:{event_id}"
        async with self._store.transaction():
            if await self._store.sagas.markers.exists(marker_id):
                return "duplicate"
            await self._store.sagas.markers.insert(marker_id, event_type, now)
            handlers = [binding for binding in definition.events if binding.event_type == event_type]
            if not handlers:
                return "no_match"
            event = self._decode_event(handlers[0], payload)
            values = {binding: binding.association_value(event, headers) for binding in handlers}
            matched: dict[str, SagaDocument] = {}
            handled: dict[str, set[SagaEventBinding]] = {}
            for binding, value in values.items():
                if binding.by_id:
                    saga_id = getattr(event, "saga_id", None)
                    if saga_id is None:
                        continue
                    document = await self._store.sagas.sagas.find_by_id(saga_id)
                    if document is not None and document.type_name == definition.name and not document.ended:
                        matched.setdefault(document.id, document)
                        handled.setdefault(document.id, set()).update(handlers)
                    continue
                if value is None or binding.key is None:
                    continue
                for document in await self._store.sagas.sagas.find_live(definition.name, binding.key, value):
                    matched.setdefault(document.id, document)
                    handled.setdefault(document.id, set()).add(binding)
            planned: list[_Planned] = []
            for document in matched.values():
                planned.append(
                    await self._invoke(
                        definition,
                        document.id,
                        event,
                        values,
                        headers,
                        event_id,
                        now,
                        document=document,
                        handlers=[binding for binding in handlers if binding in handled[document.id]],
                        associations=document.associations,
                        start_key_value=document.start_key,
                    )
                )
            for binding, value in values.items():
                if not binding.start or not binding.force_new or value is None or binding.key is None:
                    continue
                planned.append(
                    await self._invoke(
                        definition,
                        _new_id(),
                        event,
                        values,
                        headers,
                        event_id,
                        now,
                        document=None,
                        handlers=[h for h in handlers if h.key == binding.key and values.get(h) == value],
                        associations=(Association(binding.key, value),),
                        start_key_value=None,
                    )
                )
            if not matched:
                start = next(
                    (
                        binding
                        for binding, value in values.items()
                        if binding.start and not binding.force_new and value is not None
                    ),
                    None,
                )
                if start is not None and start.key is not None:
                    value = values[start]
                    assert value is not None
                    planned.append(
                        await self._invoke(
                            definition,
                            _new_id(),
                            event,
                            values,
                            headers,
                            event_id,
                            now,
                            document=None,
                            handlers=[h for h in handlers if h.key == start.key and values.get(h) == value],
                            associations=(Association(start.key, value),),
                            start_key_value=start_key(definition.name, start.key, value),
                        )
                    )
            if not planned:
                return "no_match"
            for item in planned:
                await self._write(item, now)
            return "handled"

    def _decode_event(self, binding: SagaEventBinding, payload: bytes) -> object:
        try:
            return self._codec.decode(payload, binding.message)
        except Exception as error:
            raise SagaError(f"saga handler {binding.label}: cannot read the event: {error}") from error

    async def _invoke(
        self,
        definition: SagaDefinition,
        saga_id: str,
        event: object,
        values: Mapping[SagaEventBinding, str | None],
        headers: Mapping[str, str],
        event_id: str,
        now: int,
        *,
        document: SagaDocument | None,
        handlers: Sequence[SagaEventBinding],
        associations: Sequence[Association],
        start_key_value: str | None,
    ) -> _Planned:
        instance = definition.cls() if document is None else self._model.decode_state(definition, document.state)
        version = 0 if document is None else document.version + 1
        correlation = headers.get(EventHeaders.CORRELATION_ID) or event_id
        traceparent = headers.get(EventHeaders.TRACEPARENT)
        context = SagaContext(
            saga_id=saga_id,
            version=version,
            now_ms=now,
            correlation_id=correlation,
            traceparent=traceparent,
            codec=self._codec,
            associations=associations,
            type_name_=definition.name,
            message_id=event_id,
            event_type=type_name(event),
            headers=headers,
        )
        for binding in handlers:
            await _call_handler(binding.fn, instance, event, context)
            if binding.end:
                context.end()
            if context.ended:
                break
        state = self._model.encode_state(definition, instance)
        kept_start = start_key_value
        if kept_start is not None and not _keeps_start_key(definition, kept_start, context.associations):
            kept_start = None
        if context.ended:
            kept_start = None
        new_document = SagaDocument(
            id=saga_id,
            type_name=definition.name,
            state=state,
            associations=context.associations,
            start_key=kept_start,
            ended=context.ended,
            ended_at_ms=now if context.ended else None,
            version=version,
            created_at_ms=now if document is None else document.created_at_ms,
            updated_at_ms=now,
        )
        return _Planned(
            document=new_document,
            expected_version=None if document is None else document.version,
            entries=context.dispatch,
            deadline_ops=context.deadline_ops,
            created=document is None,
        )

    async def _write(self, planned: _Planned, now: int) -> None:
        document = planned.document
        if planned.expected_version is None:
            await self._store.sagas.sagas.insert(document)
        elif not await self._store.sagas.sagas.update(document, planned.expected_version):
            raise _VersionConflict(f"saga {document.id} is no longer at version {planned.expected_version}")
        for operation, value in planned.deadline_ops:
            if operation == "schedule":
                assert isinstance(value, DeadlineDocument)
                await self._store.sagas.deadlines.insert(value)
            elif operation == "cancel":
                await self._store.sagas.deadlines.cancel(document.id, str(value), now)
            else:
                await self._store.sagas.deadlines.cancel_by_name(document.id, str(value), now)
        if planned.entries:
            await self._store.sagas.dispatch.record(planned.entries)  # type: ignore[arg-type]
        if document.ended:
            await self._store.sagas.deadlines.cancel_all(document.id, now)

    async def _park(
        self,
        definition: SagaDefinition,
        event_type: str,
        payload: bytes,
        headers: Mapping[str, str],
        event_id: str,
        failure: _GaveUp,
    ) -> None:
        marker_id = f"saga:{definition.name}:{event_id}"
        now = self._clock()
        conservation = dict(headers)
        conservation.setdefault(EventHeaders.CORRELATION_ID, event_id)
        error = _describe(failure.last)
        try:
            async with self._store.transaction():
                if await self._store.sagas.markers.exists(marker_id):
                    return
                await self._store.sagas.markers.insert(marker_id, event_type, now)
                await self._store.sagas.failures.save(
                    SagaFailureDocument(
                        id=f"{definition.name}:{event_id}",
                        saga_type=definition.name,
                        event_id=event_id,
                        event_type=event_type,
                        payload=payload.decode("utf-8", "replace"),
                        headers=conservation,
                        error=error,
                        attempts=failure.attempts,
                        first_failed_at_ms=now,
                        last_failed_at_ms=now,
                    )
                )
        except Conflict:
            return
        log.error(
            "Saga type %s parked event %s of type %s after %d attempts: %s",
            definition.name,
            event_id,
            event_type,
            failure.attempts,
            error,
        )

    # ---- deadlines ------------------------------------------------------------------------------

    async def _fire(self, definition: SagaDefinition, claimed: DeadlineDocument) -> bool:
        now = self._clock()
        async with self._store.transaction():
            if await self._store.sagas.deadlines.find_scheduled(claimed.id) is None:
                return False
            document = await self._store.sagas.sagas.find_by_id(claimed.saga_id)
            if document is None or document.ended:
                await self._store.sagas.deadlines.cancel(claimed.saga_id, claimed.id, now)
                return False
            handler = next((binding for binding in definition.deadlines if binding.name == claimed.name), None)
            if handler is None:
                log.error(
                    "Saga type %s has no deadline handler %r: cancelling %s",
                    definition.name,
                    claimed.name,
                    claimed.id,
                )
                await self._store.sagas.deadlines.cancel(claimed.saga_id, claimed.id, now)
                return False
            instance = self._model.decode_state(definition, document.state)
            correlation = claimed.correlation_id or claimed.id
            context = SagaContext(
                saga_id=document.id,
                version=document.version + 1,
                now_ms=now,
                correlation_id=correlation,
                traceparent=None,
                codec=self._codec,
                associations=document.associations,
                type_name_=definition.name,
                message_id=claimed.id,
                event_type=f"deadline:{claimed.name}",
            )
            payload: object | None = None
            if handler.payload_type is not None and claimed.payload is not None:
                payload = self._codec.decode(claimed.payload.encode(), handler.payload_type)
            await _call_deadline(handler.fn, instance, payload, context)
            if handler.end:
                context.end()
            planned = await self._plan(definition, document, instance, context, now)
            if not await self._store.sagas.deadlines.mark_fired(claimed.id, now):
                raise _VersionConflict(f"deadline {claimed.id} changed while firing")
            await self._write(planned, now)
            return True

    async def _plan(
        self,
        definition: SagaDefinition,
        document: SagaDocument,
        instance: object,
        context: SagaContext,
        now: int,
    ) -> _Planned:
        kept_start = document.start_key if not context.ended else None
        if kept_start is not None and not _keeps_start_key(definition, kept_start, context.associations):
            kept_start = None
        return _Planned(
            document=SagaDocument(
                id=document.id,
                type_name=definition.name,
                state=self._model.encode_state(definition, instance),
                associations=context.associations,
                start_key=kept_start,
                ended=context.ended,
                ended_at_ms=now if context.ended else None,
                version=document.version + 1,
                created_at_ms=document.created_at_ms,
                updated_at_ms=now,
            ),
            expected_version=document.version,
            entries=context.dispatch,
            deadline_ops=context.deadline_ops,
            created=False,
        )


def _keeps_start_key(definition: SagaDefinition, key: str, associations: Sequence[Association]) -> bool:
    return any(start_key(definition.name, association.key, association.value) == key for association in associations)


def _new_id() -> str:
    from snodra._ids import new_id

    return new_id()


async def _call_handler(
    fn: Callable[..., Awaitable[object]],
    instance: object,
    event: object,
    context: SagaContext,
) -> None:
    import inspect

    parameters = list(inspect.signature(fn).parameters.values())[1:]
    if len(parameters) >= 2:
        await fn(instance, event, context)
    else:
        await fn(instance, event)


async def _call_deadline(
    fn: Callable[..., Awaitable[object]],
    instance: object,
    payload: object | None,
    context: SagaContext,
) -> None:
    import inspect

    parameters = list(inspect.signature(fn).parameters.values())[1:]
    if len(parameters) >= 2:
        await fn(instance, payload, context)
    elif len(parameters) == 1:
        # One parameter: either the payload or the context; the annotation decided which at registration.
        annotation = parameters[0].annotation
        if annotation is SagaContext or annotation == "SagaContext":
            await fn(instance, context)
        else:
            await fn(instance, payload)
    else:
        await fn(instance)


def _describe(error: BaseException) -> str:
    root: BaseException = error
    seen: set[int] = set()
    while root.__cause__ is not None and id(root.__cause__) not in seen:
        seen.add(id(root))
        root = root.__cause__
    text = f"{type(root).__name__}: {root}"
    return text[:MAX_ERROR_CHARS]


__all__ = ["SagaRuntime"]
