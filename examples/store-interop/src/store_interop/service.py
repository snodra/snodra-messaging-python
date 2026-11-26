"""The probe process: one app per store family, the commands the driver calls, and the life cycle."""

from __future__ import annotations

import dataclasses
import itertools
import json
import logging
import time
import uuid
from datetime import UTC, datetime

import snodra
from snodra import EventHeaders, OutboxEntry, OutgoingEvent
from snodra_kafka import KafkaConfig, KafkaEventStore
from snodra_mongo import MongoConfig, MongoStore
from snodra_rabbitmq import RabbitMQConfig, RabbitMQEventStore
from snodra_sql import DRIVER_MYSQL, DRIVER_POSTGRES, SqlConfig, SqlStore

from store_interop.ledger import Ledger
from store_interop.messages import (
    OrderPlaced,
    Publish,
    PublishRaw,
    PublishRawReply,
    PublishReply,
    State,
    StateReply,
    Who,
    WhoReply,
)
from store_interop.observe import ObservedStore
from store_interop.settings import (
    EVENT_TYPE,
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
            ),
            store=self.store,
            event_store=self.event_store,
        )
        self._seq = itertools.count(1)
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
                deliveries=tuple(deliveries),
                markers=markers,
            )

        @app.event_handler(processor=processor_name(self.family.name))
        async def on_placed(event: OrderPlaced) -> None:
            self.ledger.observe_handled(event)

    def _check(self, family: str, run: str) -> None:
        if family and family != self.family.name:
            raise snodra.InvalidRequest(f"this instance is the {self.family.name} family, not {family}")
        if run and run != self.settings.run:
            raise snodra.InvalidRequest(f"this instance belongs to run {self.settings.run}, not {run}")

    def _new_id(self, publisher: str) -> str:
        return f"{self.settings.run}-{publisher}-{self.family.name}-{uuid.uuid4().hex}"

    async def start(self) -> None:
        await self.app.start()
        log.info("the %s probe (instance %s) started", self.family.name, self.instance)

    async def stop(self) -> None:
        try:
            await self.app.shutdown(timeout=SHUTDOWN_BUDGET)
        finally:
            await self.close()

    async def close(self) -> None:
        """Release the add-ons' own clients, after the app stopped using them."""
        for owned in (self.event_store, self._inner):
            close = getattr(owned, "close", None)
            if close is not None:
                await close()


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
