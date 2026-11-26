"""The message store the app is built with: the add-on's own, with its processed port observed.

Every port is the add-on's; only ``processed.record`` is wrapped so the probe can tell which
stage-1 marker the library's consumer inserted and which it found (a duplicate). The transaction,
the consumer and the relay stay the library's.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from contextlib import AbstractAsyncContextManager

from snodra import (
    DeadLetterStore,
    ExternalCallStore,
    MessageStore,
    OutboxStore,
    ProcessedMarker,
    ProcessedStore,
    ProcessorControlStore,
    ReplayRange,
    SagaStores,
    StoreLifecycle,
    Tx,
)

from store_interop.ledger import Ledger
from store_interop.settings import processor_name


class ObservedProcessed:
    """The add-on's processed store, observed: the record's answer is what the consumer skips on."""

    def __init__(self, inner: ProcessedStore, family: str, ledger: Ledger) -> None:
        self._inner = inner
        self._family = family
        self._ledger = ledger

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        recorded = await self._inner.record(marker, tx=tx)
        self._ledger.observe_marker(self._family, marker, recorded)
        return recorded

    async def exists(self, marker_id: str) -> bool:
        return await self._inner.exists(marker_id)

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        return await self._inner.delete_markers(processor, ranges)

    async def purge(self, before_ms: int) -> int:
        return await self._inner.purge(before_ms)


class ObservedStore:
    """The add-on's message store, with the processed port observed."""

    def __init__(self, inner: MessageStore, family: str, ledger: Ledger) -> None:
        self._inner = inner
        self._family = family
        self._ledger = ledger

    @property
    def name(self) -> str:
        return self._inner.name

    def transaction(self) -> AbstractAsyncContextManager[Tx]:
        return self._inner.transaction()

    async def begin(self) -> Tx:
        return await self._inner.begin()

    def plain(self) -> Tx:
        return self._inner.plain()

    async def commit(self, tx: Tx) -> None:
        await self._inner.commit(tx)

    async def rollback(self, tx: Tx) -> None:
        await self._inner.rollback(tx)

    def use_clock(self, clock: Callable[[], int]) -> None:
        if isinstance(self._inner, StoreLifecycle):
            self._inner.use_clock(clock)

    async def init_schema(self) -> None:
        if isinstance(self._inner, StoreLifecycle):
            await self._inner.init_schema()

    async def check_schema(self) -> None:
        if isinstance(self._inner, StoreLifecycle):
            await self._inner.check_schema()

    async def background(self) -> None:
        if isinstance(self._inner, StoreLifecycle):
            await self._inner.background()
            return
        await asyncio.Event().wait()

    @property
    def outbox(self) -> OutboxStore:
        return self._inner.outbox

    @property
    def processed(self) -> ProcessedStore:
        return ObservedProcessed(self._inner.processed, self._family, self._ledger)

    @property
    def dead_letters(self) -> DeadLetterStore:
        return self._inner.dead_letters

    @property
    def control(self) -> ProcessorControlStore:
        return self._inner.control

    @property
    def external(self) -> ExternalCallStore:
        return self._inner.external

    @property
    def sagas(self) -> SagaStores:
        return self._inner.sagas


__all__ = ["ObservedProcessed", "ObservedStore", "processor_name"]
