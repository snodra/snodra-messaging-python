"""The probe process: one app per store family, the commands the driver calls, and the life cycle."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import itertools
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import snodra
from snodra import EventHeaders, OutboxEntry, OutgoingEvent, SagaCommandFailed, SagaContext
from snodra_kafka import KafkaConfig, KafkaEventStore
from snodra_mongo import MongoConfig, MongoStore
from snodra_rabbitmq import RabbitMQConfig, RabbitMQEventStore
from snodra_sql import DRIVER_MYSQL, DRIVER_POSTGRES, SqlConfig, SqlStore

from store_interop.ledger import Ledger
from store_interop.messages import (
    SAGA_ADVANCED_TYPE,
    SAGA_ASSOCIATION_KEY,
    SAGA_COMMAND_FAILED_TYPE,
    SAGA_DEADLINE_NAME,
    SAGA_FAIL_TYPE,
    SAGA_STARTED_TYPE,
    SAGA_TYPE,
    Allow,
    AllowReply,
    ControlReply,
    ControlSet,
    DltEvent,
    LetterBrief,
    OrderPlaced,
    Poison,
    PoisonReply,
    Publish,
    PublishRaw,
    PublishRawReply,
    PublishReply,
    SagaAdvanced,
    SagaAssociationView,
    SagaDeadlinePayload,
    SagaDispatchReply,
    SagaDispatchRequest,
    SagaFail,
    SagaFailReply,
    SagaLeaseReply,
    SagaLeaseRequest,
    SagaNoticeReply,
    SagaNoticeRequest,
    SagaPublish,
    SagaPublishReply,
    SagaReleaseReply,
    SagaReleaseRequest,
    SagaSlow,
    SagaSlowReply,
    SagaSlowStarted,
    SagaStarted,
    SagaStateReply,
    SagaStateRequest,
    State,
    StateReply,
    Who,
    WhoReply,
)
from store_interop.messages import (
    ReplayRange as _ReplayRange,
)
from store_interop.observe import ObservedStore
from store_interop.settings import (
    EVENT_TYPE,
    RABBITMQ,
    SERVICE,
    Family,
    Settings,
    instance_name,
    marker_id,
    processor_name,
    source_name,
    subscription_name,
)

log = logging.getLogger("store_interop")

SHUTDOWN_BUDGET = 10.0
"""Seconds one App may take to shut down; five sequential apps fit the compose stop_grace_period (60 s)."""


class Probe:
    """One family's app in this process, its stores and the ledger of what its consumer did."""

    def __init__(self, settings: Settings, family: Family) -> None:
        self.settings = settings
        self.family = family
        self.ledger = Ledger()
        self._inner, self.event_store, self.database = _build_stores(settings, family)
        self.store = ObservedStore(self._inner, family.name, self.ledger)
        self.app = snodra.App(
            snodra.Settings(
                service=SERVICE,
                instance=instance_name(settings.language, family.name),
                hubs=(settings.hub,),
                events_source=source_name(settings.run, family.name),
                outbox_poll_interval=0.1,
                status_interval=settings.status_interval,
                dead_letter_mode="event-store" if family.name == RABBITMQ else "collection",
            ),
            store=self.store,
            event_store=self.event_store,
        )
        self._seq = itertools.count(1)
        self._opted_out = 0
        self._allowed: set[str] = set()
        self._notices: dict[str, int] = {}
        self._dlt: list[DltEvent] = []
        self._dlt_sub: snodra.EventStoreSubscription | None = None
        self._dlt_task: asyncio.Task[None] | None = None
        self._notice_watch: dict[str, int] = {}
        self._notice_sub: snodra.EventStoreSubscription | None = None
        self._notice_task: asyncio.Task[None] | None = None
        self._register()

    @property
    def instance(self) -> str:
        return instance_name(self.settings.language, self.family.name)

    def _register(self) -> None:
        app = self.app

        @app.command_handler
        async def who(command: Who) -> WhoReply:
            return WhoReply(
                instance=self.instance,
                language=self.settings.language,
                family=self.family.name,
            )

        @app.command_handler
        async def publish(command: Publish) -> PublishReply:
            self._check(command.family, command.run)
            if not command.publisher:
                raise snodra.InvalidRequest("publisher is required")
            if command.count < 1:
                raise snodra.InvalidRequest("count must be positive")
            if not command.keys:
                raise snodra.InvalidRequest("at least one key is required")
            ids: list[str] = []
            for index in range(command.count):
                key = command.keys[index % len(command.keys)]
                event_id = self._new_id(command.publisher)
                seq = next(self._seq)
                event = OrderPlaced(
                    id=event_id,
                    run=self.settings.run,
                    publisher=command.publisher,
                    family=self.family.name,
                    key=key,
                    seq=seq,
                )
                await self.store.outbox.append_now(
                    OutboxEntry(
                        id=event_id,
                        type_name=EVENT_TYPE,
                        key=key,
                        headers={EventHeaders.CORRELATION_ID: event_id},
                        payload=json.dumps(dataclasses.asdict(event)).encode(),
                        created_at_ms=int(time.time() * 1000),
                        seq=seq,
                    )
                )
                ids.append(event_id)
            return PublishReply(instance=self.instance, ids=tuple(ids))

        @app.command_handler
        async def publish_raw(command: PublishRaw) -> PublishRawReply:
            self._check(command.family, command.run)
            if not command.ids:
                raise snodra.InvalidRequest("at least one id is required")
            if not command.keys:
                raise snodra.InvalidRequest("at least one key is required")
            published: list[str] = []
            for index, event_id in enumerate(command.ids):
                if not event_id:
                    raise snodra.InvalidRequest("an event id must not be empty")
                key = command.keys[index % len(command.keys)]
                event = OrderPlaced(
                    id=event_id,
                    run=self.settings.run,
                    publisher=command.publisher,
                    family=self.family.name,
                    key=key,
                    seq=index + 1,
                )
                headers = {
                    EventHeaders.TYPE: EVENT_TYPE,
                    EventHeaders.ID: event_id,
                    EventHeaders.CORRELATION_ID: event_id,
                    EventHeaders.TIMESTAMP: _now_text(),
                    EventHeaders.SOURCE: SERVICE,
                    EventHeaders.USER_ID: "",
                }
                await self.event_store.publish(
                    OutgoingEvent(
                        source=source_name(self.settings.run, self.family.name),
                        id=event_id,
                        type_name=EVENT_TYPE,
                        key=key,
                        headers=headers,
                        payload=json.dumps(dataclasses.asdict(event)).encode(),
                    )
                )
                published.append(event_id)
            return PublishRawReply(instance=self.instance, published=tuple(published))

        @app.command_handler
        async def state(command: State) -> StateReply:
            deliveries, handled, duplicates = self.ledger.snapshot()
            markers = {
                event_id: await self.store.processed.exists(marker_id(self.family.name, event_id))
                for event_id in command.markers
            }
            control = await self.store.control.get(processor_name(self.family.name))
            letters = await self.store.dead_letters.list(processor_name(self.family.name), limit=100)
            dead_letter_total = await self.store.dead_letters.count(processor_name(self.family.name))
            operation = None if control.operation is None else control.operation.id
            dead_letters = tuple(
                LetterBrief(
                    id=letter.id,
                    reason=str(letter.reason),
                    event_id=letter.event_id,
                    attempts=letter.attempts,
                )
                for letter in letters
            )
            replay = tuple(
                _ReplayRange(segment=open_range.segment, from_=open_range.start, until=open_range.until)
                for open_range in control.replay
            )
            dlt = tuple(self._dlt)
            return StateReply(
                instance=self.instance,
                language=self.settings.language,
                family=self.family.name,
                run=self.settings.run,
                store=self.store.name,
                event_store=self.event_store.name,
                source=source_name(self.settings.run, self.family.name),
                database=self.database,
                subscription=subscription_name(self.family.name),
                outbox_pending=await self.store.outbox.pending(),
                handled=handled,
                duplicates=duplicates,
                opted_out=self._opted_out,
                paused=control.paused,
                paused_by=control.paused_by,
                operation_id=operation,
                replay=replay,
                dead_letters=dead_letters,
                dead_letter_total=dead_letter_total,
                dlt=dlt,
                dlt_total=len(dlt),
                deliveries=tuple(deliveries),
                markers=markers,
            )

        @app.command_handler
        async def control_set(command: ControlSet) -> ControlReply:
            self._check(command.family, command.run)
            if not command.by:
                raise snodra.InvalidRequest("by is required")
            await self.store.control.set_paused(
                processor_name(self.family.name), command.paused, command.by, int(time.time() * 1000)
            )
            control = await self.store.control.get(processor_name(self.family.name))
            return ControlReply(instance=self.instance, paused=control.paused, paused_by=control.paused_by)

        @app.command_handler
        async def poison(command: Poison) -> PoisonReply:
            self._check(command.family, command.run)
            if not command.id:
                raise snodra.InvalidRequest("id is required")
            if not command.key:
                raise snodra.InvalidRequest("key is required")
            event = OrderPlaced(
                id=command.id,
                run=self.settings.run,
                publisher=command.publisher,
                family=self.family.name,
                key=command.key,
                seq=1,
                poison=True,
            )
            headers = {
                EventHeaders.TYPE: EVENT_TYPE,
                EventHeaders.ID: command.id,
                EventHeaders.CORRELATION_ID: command.id,
                EventHeaders.TIMESTAMP: _now_text(),
                EventHeaders.SOURCE: SERVICE,
                EventHeaders.USER_ID: "",
            }
            await self.event_store.publish(
                OutgoingEvent(
                    source=source_name(self.settings.run, self.family.name),
                    id=command.id,
                    type_name=EVENT_TYPE,
                    key=command.key,
                    headers=headers,
                    payload=json.dumps(dataclasses.asdict(event)).encode(),
                )
            )
            return PoisonReply(instance=self.instance, published=command.id)

        @app.command_handler
        async def allow(command: Allow) -> AllowReply:
            self._check(command.family, command.run)
            self._allowed.update(command.ids)
            return AllowReply(instance=self.instance, allowed=len(self._allowed))

        @app.event_handler(processor=processor_name(self.family.name))
        async def on_placed(event: OrderPlaced) -> None:
            self._fail_if_poison(event)
            self.ledger.observe_handled(event)

        @app.event_handler(processor=processor_name(self.family.name), replay=False)
        async def on_placed_opt_out(event: OrderPlaced) -> None:
            self._fail_if_poison(event)
            self._opted_out += 1

        @app.reset_handler(processor_name(self.family.name))
        async def on_reset(reset: snodra.ResetContext) -> None:
            log.info(
                "the probe ran its reset handler (family %s, operation %s, %d ranges)",
                self.family.name,
                reset.operation_id,
                len(reset.ranges),
            )

        probe = self

        @app.saga(SAGA_TYPE)
        @dataclass
        class InteropSaga:
            """The shared saga: both languages register the same type and state fields."""

            order_id: str = ""
            run: str = ""
            family: str = ""
            mode: str = ""
            started_by: str = ""
            advanced_by: str = ""
            advances: int = 0
            deadline_scheduled_by: str = ""
            deadline_fired: bool = False
            deadline_fired_by: str = ""
            slow_recorded_by: str = ""
            notice_by: str = ""
            notice_reason: str = ""
            notice_command_type: str = ""
            notice_key: str = ""
            notice_message: str = ""
            notices: int = 0

            @snodra.saga_start("order_id", key=SAGA_ASSOCIATION_KEY)
            async def started(self, event: SagaStarted, ctx: SagaContext) -> None:
                self.order_id = event.order_id
                self.run = event.run
                self.family = event.family
                self.mode = event.mode
                self.started_by = probe.settings.language
                self.deadline_scheduled_by = probe.settings.language
                ctx.associate_with(SAGA_ASSOCIATION_KEY, event.order_id)
                if event.mode == "deadline":
                    ctx.schedule(
                        2.0,
                        SAGA_DEADLINE_NAME,
                        SagaDeadlinePayload(order_id=event.order_id, run=event.run, family=event.family),
                    )
                log.info(
                    "the probe started a saga %s (order %s, mode %s, language %s)",
                    ctx.saga_id,
                    event.order_id,
                    event.mode,
                    probe.settings.language,
                )

            @snodra.saga_event("order_id", key=SAGA_ASSOCIATION_KEY)
            async def advanced(self, event: SagaAdvanced, ctx: SagaContext) -> None:
                self.advanced_by = probe.settings.language
                self.advances += 1
                log.info(
                    "the probe continued a saga %s (order %s, mode %s, language %s)",
                    ctx.saga_id,
                    event.order_id,
                    event.mode,
                    probe.settings.language,
                )
                if event.mode == "lease":
                    self.slow_recorded_by = probe.settings.language
                    ctx.send(SagaSlow(saga_id=ctx.saga_id, order_id=event.order_id))
                elif event.mode == "notice":
                    self.notice_command_type = SAGA_FAIL_TYPE
                    ctx.send(SagaFail(saga_id=ctx.saga_id, order_id=event.order_id))

            @snodra.saga_deadline(SAGA_DEADLINE_NAME)
            async def timed_out(self, payload: SagaDeadlinePayload, ctx: SagaContext) -> None:
                self.deadline_fired = True
                self.deadline_fired_by = probe.settings.language
                log.info(
                    "the probe fired a saga deadline %s (order %s, language %s)",
                    ctx.saga_id,
                    payload.order_id,
                    probe.settings.language,
                )

            @snodra.saga_command_failed
            async def failed(self, event: SagaCommandFailed, ctx: SagaContext) -> None:
                self.notice_by = probe.settings.language
                self.notice_reason = event.reason
                self.notice_command_type = event.command_type
                self.notice_key = event.idempotency_key
                self.notice_message = event.message
                self.notices += 1
                log.info(
                    "the probe's saga received a command failure %s (%s, language %s)",
                    ctx.saga_id,
                    event.reason,
                    probe.settings.language,
                )

        @app.event_handler
        async def on_notice(event: SagaCommandFailed) -> None:
            self._notices[event.saga_id] = self._notices.get(event.saga_id, 0) + 1
            log.info(
                "the probe's ordinary notice handler ran (saga %s, reason %s, language %s)",
                event.saga_id,
                event.reason,
                probe.settings.language,
            )

        @app.command_handler
        async def saga_fail(command: SagaFail) -> SagaFailReply:
            raise snodra.InvalidRequest("the saga fail command fails on purpose")

        @app.command_handler
        async def saga_slow(command: SagaSlow) -> SagaSlowReply:
            # The driver answers this send only after it probed the lease from the other language:
            # the command stays in flight, and the relay holds the saga's lease, until then.
            await self.app.send(
                SagaSlowStarted(saga_id=command.saga_id, order_id=command.order_id, language=probe.settings.language)
            )
            return SagaSlowReply()

        @app.command_handler
        async def saga_publish(command: SagaPublish) -> SagaPublishReply:
            self._check(command.family, command.run)
            if not command.id or not command.key or not command.order_id:
                raise snodra.InvalidRequest("id, key and orderId are required")
            if command.kind == "started":
                type_name = SAGA_STARTED_TYPE
                payload = {
                    "orderId": command.order_id,
                    "run": self.settings.run,
                    "family": self.family.name,
                    "mode": command.mode,
                }
            elif command.kind == "advanced":
                type_name = SAGA_ADVANCED_TYPE
                payload = {
                    "orderId": command.order_id,
                    "run": self.settings.run,
                    "family": self.family.name,
                    "mode": command.mode,
                }
            else:
                raise snodra.InvalidRequest("kind is started or advanced")
            headers = {
                EventHeaders.TYPE: type_name,
                EventHeaders.ID: command.id,
                EventHeaders.CORRELATION_ID: command.id,
                EventHeaders.TIMESTAMP: _now_text(),
                EventHeaders.SOURCE: SERVICE,
                EventHeaders.USER_ID: "",
            }
            await self.event_store.publish(
                OutgoingEvent(
                    source=source_name(self.settings.run, self.family.name),
                    id=command.id,
                    type_name=type_name,
                    key=command.key,
                    headers=headers,
                    payload=json.dumps(payload).encode(),
                )
            )
            return SagaPublishReply(instance=self.instance, published=command.id)

        @app.command_handler
        async def saga_state(request: SagaStateRequest) -> SagaStateReply:
            self._check(request.family, request.run)
            if not request.type:
                raise snodra.InvalidRequest("type is required")
            live = 0
            if request.saga_id:
                saga = await self.store.sagas.sagas.find_by_id(request.saga_id)
                live = 1 if saga is not None and not saga.ended else 0
            else:
                found = await self.store.sagas.sagas.find_live(request.type, request.key, request.value)
                live = len(found)
                saga = found[0] if found else None
            if saga is None:
                return SagaStateReply(instance=self.instance, found=False, live=live)
            return SagaStateReply(
                instance=self.instance,
                found=True,
                saga_id=saga.id,
                type=saga.type_name,
                state=json.loads(saga.state),
                version=saga.version,
                ended=saga.ended,
                live=live,
                associations=tuple(
                    SagaAssociationView(key=association.key, value=association.value)
                    for association in saga.associations
                ),
            )

        @app.command_handler
        async def saga_dispatch(request: SagaDispatchRequest) -> SagaDispatchReply:
            self._check(request.family, request.run)
            if not request.saga_id:
                raise snodra.InvalidRequest("sagaId is required")
            dispatch = self.store.sagas.dispatch
            entry = await dispatch.next_pending(request.saga_id)
            pending = await dispatch.pending()
            if entry is None:
                return SagaDispatchReply(instance=self.instance, pending=False, pending_count=pending)
            return SagaDispatchReply(
                instance=self.instance,
                pending=True,
                entry_id=entry.id,
                seq=entry.seq,
                kind=str(entry.kind),
                type=entry.type_name,
                attempts=entry.attempts,
                pending_count=pending,
            )

        @app.command_handler
        async def saga_lease(request: SagaLeaseRequest) -> SagaLeaseReply:
            self._check(request.family, request.run)
            if not request.saga_id or not request.owner or request.until_ms <= 0:
                raise snodra.InvalidRequest("sagaId, owner and untilMs are required")
            taken = await self.store.sagas.dispatch.claim_lease(
                request.saga_id, request.owner, int(time.time() * 1000), request.until_ms
            )
            return SagaLeaseReply(instance=self.instance, taken=taken)

        @app.command_handler
        async def saga_release(request: SagaReleaseRequest) -> SagaReleaseReply:
            self._check(request.family, request.run)
            if not request.saga_id or not request.owner:
                raise snodra.InvalidRequest("sagaId and owner are required")
            dispatch = self.store.sagas.dispatch
            await dispatch.release_lease(request.saga_id, request.owner)
            # release_lease has no result: claim with a witness, which succeeds only when the
            # release actually freed the lease, and release the witness again.
            witness = f"{request.owner}-witness"
            now_ms = int(time.time() * 1000)
            taken = await dispatch.claim_lease(request.saga_id, witness, now_ms, now_ms + 5_000)
            if taken:
                await dispatch.release_lease(request.saga_id, witness)
            return SagaReleaseReply(instance=self.instance, released=taken)

        @app.command_handler
        async def saga_notice(request: SagaNoticeRequest) -> SagaNoticeReply:
            self._check(request.family, request.run)
            return SagaNoticeReply(
                instance=self.instance,
                language=self.settings.language,
                ordinary=self._notices.get(request.saga_id, 0),
                published=self._notice_watch.get(request.saga_id, 0),
            )

    def _fail_if_poison(self, event: OrderPlaced) -> None:
        """A poison event fails until the driver allows its id on this instance."""
        if event.poison and event.id not in self._allowed:
            raise RuntimeError(f"the poison event {event.id} is not allowed yet")

    def _check(self, family: str, run: str) -> None:
        if family and family != self.family.name:
            raise snodra.InvalidRequest(f"this instance is the {self.family.name} family, not {family}")
        if run and run != self.settings.run:
            raise snodra.InvalidRequest(f"this instance belongs to run {self.settings.run}, not {run}")

    def _new_id(self, publisher: str) -> str:
        return f"{self.settings.run}-{publisher}-{self.family.name}-{uuid.uuid4().hex}"

    async def start(self) -> None:
        await self.app.start()
        await self._start_dlt()
        await self._start_notice_watch()
        log.info("the %s probe (instance %s) started", self.family.name, self.instance)

    async def stop(self) -> None:
        try:
            await self.app.shutdown(timeout=SHUTDOWN_BUDGET)
        finally:
            await self._stop_dlt()
            await self._stop_notice_watch()
            await self.close()

    async def close(self) -> None:
        """Release the add-ons' own clients, after the app stopped using them."""
        for owned in (self.event_store, self._inner):
            close = getattr(owned, "close", None)
            if close is not None:
                await close()

    async def _start_dlt(self) -> None:
        """Subscribe to the family's dead-letter source when its processor uses the event-store mode."""
        if self.family.name != RABBITMQ:
            return
        self._dlt_sub = await self.event_store.subscribe(
            snodra.SubscriptionSpec(
                name=f"{SERVICE}.interop-{self.family.name}-dlt-{self.settings.language}",
                source=source_name(self.settings.run, self.family.name) + ".dlt",
            )
        )
        self._dlt_task = asyncio.create_task(self._drain_dlt(), name=f"snodra:store-interop-dlt:{self.family.name}")

    async def _drain_dlt(self) -> None:
        sub = self._dlt_sub
        if sub is None:  # pragma: no cover - started before the task
            return
        while True:
            event = await sub.next()
            if not isinstance(event, snodra.Delivery):
                continue
            self._dlt.append(
                DltEvent(
                    id=event.headers.get(EventHeaders.ID, ""),
                    type=event.headers.get(EventHeaders.TYPE, ""),
                    key=event.key,
                    segment=event.segment,
                    position=event.position,
                    headers=dict(event.headers),
                    payload=json.loads(event.payload) if event.payload else None,
                )
            )
            await sub.ack(event.segment, event.position)

    async def _start_notice_watch(self) -> None:
        """Read the family source as a raw subscription and record a published SagaCommandFailed."""
        self._notice_sub = await self.event_store.subscribe(
            snodra.SubscriptionSpec(
                name=f"{SERVICE}.interop-{self.family.name}-notice-{self.settings.language}",
                source=source_name(self.settings.run, self.family.name),
            )
        )
        self._notice_task = asyncio.create_task(
            self._drain_notice(), name=f"snodra:store-interop-notice:{self.family.name}"
        )

    async def _drain_notice(self) -> None:
        sub = self._notice_sub
        if sub is None:  # pragma: no cover - started before the task
            return
        while True:
            event = await sub.next()
            if not isinstance(event, snodra.Delivery):
                continue
            if event.headers.get(EventHeaders.TYPE) == SAGA_COMMAND_FAILED_TYPE:
                try:
                    notice = json.loads(event.payload)
                except ValueError:
                    notice = {}
                saga_id = notice.get("sagaId") if isinstance(notice, dict) else None
                if isinstance(saga_id, str) and saga_id:
                    self._notice_watch[saga_id] = self._notice_watch.get(saga_id, 0) + 1
            await sub.ack(event.segment, event.position)

    async def _stop_notice_watch(self) -> None:
        task = self._notice_task
        sub = self._notice_sub
        self._notice_task = None
        self._notice_sub = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if sub is not None:
            await sub.close()

    async def _stop_dlt(self) -> None:
        task = self._dlt_task
        sub = self._dlt_sub
        self._dlt_task = None
        self._dlt_sub = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if sub is not None:
            await sub.close()


def _build_stores(settings: Settings, family: Family) -> tuple[snodra.MessageStore, snodra.EventStore, str | None]:
    """The family's message store and event store, and the database the driver checks."""
    database: str | None = None
    if family.message_store == "memory":
        store: snodra.MessageStore = snodra.MemoryStore()
    elif family.message_store == "postgres":
        database = settings.sql_database
        store = SqlStore(SqlConfig(url=settings.postgres_url, driver=DRIVER_POSTGRES))
    elif family.message_store == "mysql":
        database = settings.sql_database
        store = SqlStore(SqlConfig(url=settings.mysql_url, driver=DRIVER_MYSQL))
    elif family.message_store == "mongo":
        database = settings.mongo_database
        store = MongoStore(MongoConfig(url=settings.mongo_uri, database=database))
    else:  # pragma: no cover - FAMILIES is fixed
        raise ValueError(f"the unknown message store {family.message_store!r}")
    if family.event_store == "kafka":
        event_store: snodra.EventStore = KafkaEventStore(KafkaConfig(bootstrap_servers=settings.kafka_bootstrap))
    elif family.event_store == "rabbitmq-streams":
        event_store = RabbitMQEventStore(
            RabbitMQConfig(
                hosts=(settings.rabbitmq_host,),
                port=settings.rabbitmq_port,
                username=settings.rabbitmq_username,
                password=settings.rabbitmq_password,
            )
        )
    else:  # pragma: no cover - FAMILIES is fixed
        raise ValueError(f"the unknown event store {family.event_store!r}")
    return store, event_store, database


def _now_text() -> str:
    """RFC 3339 UTC with milliseconds, the timestamp header's shape."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
