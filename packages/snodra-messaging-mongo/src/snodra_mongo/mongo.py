"""The MongoDB message store: the core's message-store ports over the service's own MongoDB database.

The store keeps the shared collections and document fields the published
`MongoDB layout <https://snodra.damoreira.com/docs/library-authors/mongodb-layout/>`_ fixes, so documents written by
another Snodra library stay readable without a migration:

- Ten collections, fixed names: ``outbox``, ``processed_events``, ``dead_letters``, ``processor_control``,
  ``external_calls``, ``sagas``, ``saga_dispatch``, ``saga_dispatch_leases``, ``deadlines`` and ``saga_failures``.
- A replica set: the store's transactions are MongoDB multi-document transactions on a client session, which MongoDB
  runs only on a replica set (a single-node one is enough).
- The schema is the indexes. :meth:`MongoStore.init_schema` creates every index and TTL index idempotently, which also
  creates the collections a transaction writes to before the first transaction does.

The outbox and the processed-event markers are implemented. The dead-letter, processor-control, external-call and
saga ports are part of the interface frozen at level 2 and report
:class:`snodra.MessageStoreUnsupported` until this add-on implements them by level 4.

Every operation joins the transaction bound to the current context (``async with store.transaction()``) or runs in a
write of its own, never in another operation's transaction. A service that already runs a MongoDB transaction binds
its session with :meth:`MongoStore.transaction` (``session=``), and a handler asks for it with
:meth:`MongoStore.session`. A claim, a marker's existence check, the marks and a purge are writes of their own that
commit on their own.

Times the store sets itself (an entry's sending and failure, a claim's lease) are the application's clock
(:meth:`MongoStore.use_clock`), never the database's. MongoDB has no notification the add-on listens on, so the outbox
relay polls and :meth:`OutboxStore.on_append` registers nothing.

See https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/.
"""

from __future__ import annotations

import asyncio
import builtins
import json
import logging
import math
import os
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, suppress
from contextvars import Token
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Protocol

from bson import Int64
from pymongo import AsyncMongoClient, IndexModel, ReturnDocument
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import DuplicateKeyError, OperationFailure, PyMongoError
from snodra import (
    AppendListener,
    Conflict,
    DeadLetter,
    DeadLetterError,
    DeadLetterStore,
    DeadLetterSummary,
    DeadlineDocument,
    DispatchFailure,
    ExternalCall,
    ExternalCallStore,
    InvalidMessage,
    Locked,
    MessageStoreFailed,
    MessageStoreUnsupported,
    OutboxEntry,
    OutboxStore,
    Parked,
    ProcessedMarker,
    ProcessedStore,
    ProcessorControlState,
    ProcessorControlStore,
    ReplayRange,
    SagaDispatchDocument,
    SagaDocument,
    SagaFailureDocument,
    SagaStores,
    Tx,
    bind_transaction,
    check_outbox_payload,
    current_transaction,
    now_ms,
    unbind_transaction,
)

log = logging.getLogger(__name__)

NAME: Final = "mongo"
"""The store's name, as every Snodra MongoDB add-on reports it."""

OUTBOX_COLLECTION: Final = "outbox"
PROCESSED_COLLECTION: Final = "processed_events"
DEAD_LETTERS_COLLECTION: Final = "dead_letters"
CONTROL_COLLECTION: Final = "processor_control"
EXTERNAL_COLLECTION: Final = "external_calls"
SAGAS_COLLECTION: Final = "sagas"
SAGA_DISPATCH_COLLECTION: Final = "saga_dispatch"
SAGA_LEASES_COLLECTION: Final = "saga_dispatch_leases"
DEADLINES_COLLECTION: Final = "deadlines"
SAGA_FAILURES_COLLECTION: Final = "saga_failures"

COLLECTIONS: Final = (
    OUTBOX_COLLECTION,
    PROCESSED_COLLECTION,
    DEAD_LETTERS_COLLECTION,
    CONTROL_COLLECTION,
    EXTERNAL_COLLECTION,
    SAGAS_COLLECTION,
    SAGA_DISPATCH_COLLECTION,
    SAGA_LEASES_COLLECTION,
    DEADLINES_COLLECTION,
    SAGA_FAILURES_COLLECTION,
)
"""The ten collections of the layout, in the layout's order."""

DEFAULT_OUTBOX_RETENTION: Final = 7 * 86_400.0
DEFAULT_PROCESSED_RETENTION: Final = 30 * 86_400.0
DEFAULT_EXTERNAL_CALL_RETENTION: Final = 30 * 86_400.0
DEFAULT_SAGA_ENDED_RETENTION: Final = 30 * 86_400.0
DEFAULT_SAGA_DISPATCH_RETENTION: Final = 7 * 86_400.0
DEFAULT_DEADLINE_RETENTION: Final = 7 * 86_400.0
DEFAULT_FAILED_RETENTION: Final = 30 * 86_400.0
"""The layout's retention defaults, in seconds."""

MAX_TTL_SECONDS: Final = 2**31 - 1
"""The largest TTL a MongoDB index takes (an ``int``)."""

WRITE_CONFLICT: Final = 112
DUPLICATE_KEY: Final = 11_000
TRANSIENT_TRANSACTION: Final = "TransientTransactionError"

_EPOCH: Final = datetime(1970, 1, 1)


class StoreLifecycle(Protocol):
    # TODO(spec-gap): SG-30
    """What a message store may implement besides :class:`snodra.MessageStore`: the clock, the schema and the
    background work. A service drives it around the store's life; the six stores stay as they are, so a third-party
    add-on compiles against ``MessageStore`` alone.

    The Python core has no application binding for it yet, so a service calls the methods itself
    (``await store.init_schema()`` before ``app.start()``, and ``store.background()`` in a task).
    """

    def use_clock(self, clock: Callable[[], int]) -> None:
        """The application's clock, epoch milliseconds: what the store stamps its own times with."""
        ...

    async def init_schema(self) -> None:
        """Prepares what the store needs in its database, idempotently."""
        ...

    async def check_schema(self) -> None:
        """Fails unless the database has what the store needs, for a service that applies its own schema."""
        ...

    async def background(self) -> None:
        """Runs what the store does on its own until it is cancelled."""
        ...


@dataclass(frozen=True, slots=True)
class MongoConfig:
    """How the store reaches its database and how long it keeps what the retentions let go.

    ``url`` is the MongoDB connection string (``mongodb://...`` or ``mongodb+srv://...``) and ``database`` the
    service's own database, which holds the shared collections; both are required. The retentions are in seconds; a
    TTL index takes whole seconds, so a fraction of one is floored.
    """

    url: str = field(repr=False)
    database: str
    outbox_retention: float = DEFAULT_OUTBOX_RETENTION
    processed_retention: float = DEFAULT_PROCESSED_RETENTION
    external_call_retention: float = DEFAULT_EXTERNAL_CALL_RETENTION
    saga_ended_retention: float = DEFAULT_SAGA_ENDED_RETENTION
    saga_dispatch_retention: float = DEFAULT_SAGA_DISPATCH_RETENTION
    deadline_retention: float = DEFAULT_DEADLINE_RETENTION
    failed_retention: float = DEFAULT_FAILED_RETENTION

    def __post_init__(self) -> None:
        url = self.url.strip()
        database = self.database.strip()
        if not url:
            raise ValueError(
                "the MongoDB message store needs a connection string (MongoConfig.url, SNODRA_STORE_MONGO_URI)"
            )
        if not database:
            raise ValueError(
                "the MongoDB message store needs a database (MongoConfig.database, SNODRA_STORE_MONGO_DATABASE)"
            )
        for name in (
            "outbox_retention",
            "processed_retention",
            "external_call_retention",
            "saga_ended_retention",
            "saga_dispatch_retention",
            "deadline_retention",
            "failed_retention",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"the MongoDB message store needs a finite, non-negative {name}, has {value!r}")
            if math.floor(value) > MAX_TTL_SECONDS:
                raise ValueError(
                    f"the MongoDB message store needs a {name} of at most {MAX_TTL_SECONDS} s, has {value!r}"
                )
        object.__setattr__(self, "url", url)
        object.__setattr__(self, "database", database)

    @classmethod
    def from_env(cls) -> MongoConfig:
        """The config from the environment: ``SNODRA_STORE_MONGO_URI`` and ``SNODRA_STORE_MONGO_DATABASE``, both
        required. The retentions keep their defaults; set them on the returned value."""
        return cls(
            url=os.environ.get("SNODRA_STORE_MONGO_URI", "").strip(),
            database=os.environ.get("SNODRA_STORE_MONGO_DATABASE", "").strip(),
        )


@dataclass(slots=True)
class _MongoTx:
    """One transaction: the session it runs on and whether the store began it."""

    store: MongoStore
    session: AsyncClientSession
    owned: bool = False
    ended: bool = False


class _MongoBoundScope(AbstractAsyncContextManager[Tx]):
    """Binds a service's own session, already in a transaction, to the current context; ends nothing."""

    def __init__(self, store: MongoStore, session: AsyncClientSession) -> None:
        self._store = store
        self._session = session
        self._token: Token[Tx | None] | None = None
        self._joined = False

    async def __aenter__(self) -> Tx:
        current = current_transaction()
        if (
            current is not None
            and isinstance(current.inner, _MongoTx)
            and current.inner.store is self._store
            and current.inner.session is self._session
            and not current.inner.ended
        ):
            self._joined = True
            return current
        tx = Tx(_MongoTx(self._store, self._session, owned=False))
        self._token = bind_transaction(tx)
        return tx

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._joined:
            return
        token, self._token = self._token, None
        if token is not None:
            unbind_transaction(token)


class _MongoTransactionScope(AbstractAsyncContextManager[Tx]):
    """The core's transaction context manager over a MongoDB session, refusing a transaction bound by another store."""

    def __init__(self, store: MongoStore) -> None:
        self._store = store
        self._tx: Tx | None = None
        self._token: Token[Tx | None] | None = None
        self._joined = False

    async def __aenter__(self) -> Tx:
        current = current_transaction()
        if current is not None and self._store._inner(current) is not None:
            self._joined = True
            return current
        tx = await self._store.begin()
        self._tx = tx
        self._token = bind_transaction(tx)
        return tx

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._joined:
            return
        tx, self._tx = self._tx, None
        if tx is None:
            return
        try:
            if exc_type is None:
                await self._store.commit(tx)
            else:
                await self._store.rollback(tx)
        finally:
            token, self._token = self._token, None
            if token is not None:
                unbind_transaction(token)


class MongoStore:
    """The MongoDB message store: see the module docstring.

    ``client`` is a PyMongo async client the service already owns and configures; the store does not close it. Without
    one the store creates its own client from ``config.url`` and closes it. ``clock`` is the application's clock in
    epoch milliseconds (the wall clock by default), for tests that need known times; :meth:`use_clock` replaces it.
    """

    def __init__(
        self,
        config: MongoConfig | None = None,
        *,
        client: AsyncMongoClient[Any] | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._config = MongoConfig.from_env() if config is None else config
        self._client: AsyncMongoClient[Any] = AsyncMongoClient(self._config.url) if client is None else client
        self._owns_client = client is None
        self._database: AsyncDatabase[Any] = self._client[self._config.database]
        self._clock: Callable[[], int] = now_ms if clock is None else clock
        self._closed = False
        self._outbox_collection: AsyncCollection[Any] = self._database[OUTBOX_COLLECTION]
        self._processed_collection: AsyncCollection[Any] = self._database[PROCESSED_COLLECTION]
        self.outbox: OutboxStore = _MongoOutbox(self)
        self.processed: ProcessedStore = _MongoProcessed(self)
        self.dead_letters: DeadLetterStore = _UnsupportedDeadLetters()
        self.control: ProcessorControlStore = _UnsupportedControl()
        self.external: ExternalCallStore = _UnsupportedExternal()
        self.sagas: SagaStores = SagaStores(
            sagas=_UnsupportedSagas(),
            dispatch=_UnsupportedDispatch(),
            deadlines=_UnsupportedDeadlines(),
            failures=_UnsupportedFailures(),
            markers=_UnsupportedMarkers(),
        )

    @property
    def name(self) -> str:
        """The add-on's name: ``mongo``."""
        return NAME

    @property
    def config(self) -> MongoConfig:
        """The config the store was built with."""
        return self._config

    @property
    def client(self) -> AsyncMongoClient[Any]:
        """The PyMongo async client the store runs on: the service's own, or the one it created."""
        return self._client

    @property
    def database(self) -> AsyncDatabase[Any]:
        """The service's database, which holds the shared collections."""
        return self._database

    def use_clock(self, clock: Callable[[], int] | None) -> None:
        """The application's clock, epoch milliseconds: what the store stamps its own times with."""
        self._clock = now_ms if clock is None else clock

    async def close(self) -> None:
        """Closes a client the store created; a client the service owns stays open. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._owns_client:
            with suppress(Exception):
                await self._client.close()

    # ---- transactions -------------------------------------------------------------------------

    def transaction(self, session: AsyncClientSession | None = None) -> AbstractAsyncContextManager[Tx]:
        """A transaction bound to the current context, or a new one.

        Without ``session`` this is the core's transaction context manager: it joins a transaction already bound (or
        refuses one bound by another message store), begins one of its own otherwise, and commits or rolls it back.
        With ``session`` it binds a session the service already runs a transaction on: the store writes on that
        session and never starts, commits or aborts anything on it.
        """
        if session is None:
            return _MongoTransactionScope(self)
        return _MongoBoundScope(self, session)

    def plain(self) -> Tx:
        """No transaction: what reads and writes outside one use."""
        return Tx.none()

    def session(self, tx: Tx | None = None) -> AsyncClientSession:
        """The session of ``tx`` (or of the bound transaction), for the service's own writes in its transaction.

        Raises :class:`snodra.MessageStoreFailed` when there is no live MongoDB transaction there.
        """
        chosen = current_transaction() if tx is None else tx
        if (
            chosen is None
            or not isinstance(chosen.inner, _MongoTx)
            or chosen.inner.store is not self
            or chosen.inner.ended
        ):
            raise MessageStoreFailed(
                "the MongoDB message store has no live session in that transaction: it is none, another message "
                "store's, or has ended"
            )
        return chosen.inner.session

    async def begin(self) -> Tx:
        """A transaction of the store's own, on a client session."""
        session: AsyncClientSession | None = None
        try:
            session = self._client.start_session()
            await session.start_transaction()
        except BaseException as error:
            if session is not None:
                with suppress(Exception):
                    await session.end_session()
            if isinstance(error, asyncio.CancelledError):
                raise
            raise _failure(error) from error
        return Tx(_MongoTx(self, session, owned=True))

    async def commit(self, tx: Tx) -> None:
        """Commits what ``tx`` wrote; a session the service owns is left alone."""
        inner = self._inner(tx)
        if inner is None or not inner.owned or inner.ended:
            return
        inner.ended = True
        try:
            await inner.session.commit_transaction()
        except Exception as error:
            raise _failure(error) from error
        finally:
            with suppress(Exception):
                await inner.session.end_session()

    async def rollback(self, tx: Tx) -> None:
        """Abandons ``tx``'s writes; never fails."""
        inner = self._inner(tx)
        if inner is None or not inner.owned or inner.ended:
            return
        inner.ended = True
        try:
            await inner.session.abort_transaction()
        except Exception:
            pass
        finally:
            with suppress(Exception):
                await inner.session.end_session()

    def _inner(self, tx: Tx | None) -> _MongoTx | None:
        """This store's live transaction in ``tx`` (or the bound one when ``tx`` is None); None outside one."""
        chosen = current_transaction() if tx is None else tx
        if chosen is None or chosen.inner is None:
            return None
        inner = chosen.inner
        if not isinstance(inner, _MongoTx) or inner.store is not self:
            raise MessageStoreFailed("the MongoDB message store was given another message store's transaction")
        return None if inner.ended else inner

    def _session(self, tx: Tx | None) -> AsyncClientSession | None:
        """The session an append or a marker joins, or None for a write that commits on its own."""
        inner = self._inner(tx)
        return None if inner is None else inner.session

    def _now(self) -> int:
        return int(self._clock())

    # ---- lifecycle ----------------------------------------------------------------------------

    async def init_schema(self) -> None:
        """Creates every index and TTL index of the layout, idempotently.

        Creating them creates the collections a transaction writes to before the first transaction does. An index that
        exists is left as it is and its options are not changed, so every instance sharing a database needs the same
        retentions.
        """
        for collection, indexes in self._layout():
            models = [_index_model(index) for index in indexes]
            try:
                await self._database[collection].create_indexes(models)
            except Exception as error:
                raise _failure(error) from error

    async def check_schema(self) -> None:
        """Fails unless the database has every collection and index of the layout, with the same keys, TTLs,
        uniqueness and partial filter; it creates nothing.

        TODO(spec-gap): SG-34
        """
        for collection, indexes in self._layout():
            if collection not in await self._database.list_collection_names():
                raise MessageStoreFailed(
                    f"the MongoDB message store's schema has no {collection!r} collection: call init_schema(), or "
                    "prepare the layout's indexes yourself"
                )
            try:
                present = await self._database[collection].index_information()
            except Exception as error:
                raise _failure(error) from error
            for index in indexes:
                actual = present.get(index.name)
                if actual is None:
                    raise MessageStoreFailed(
                        f"the MongoDB message store's schema has no {index.name!r} index on {collection!r}: call "
                        "init_schema(), or prepare the layout's indexes yourself"
                    )
                keys = tuple((str(name), int(direction)) for name, direction in actual.get("key", ()))
                if keys != index.keys:
                    raise MessageStoreFailed(
                        f"the MongoDB message store's index {index.name!r} on {collection!r} has the keys {keys}, "
                        f"not {index.keys}: drop it and call init_schema()"
                    )
                _check_index_options(collection, index, actual)

    async def background(self) -> None:
        """Runs nothing: MongoDB's TTL monitor deletes expired documents on the server, so the store has no purge of
        its own. It returns at once."""
        return

    def _layout(self) -> tuple[tuple[str, tuple[_Index, ...]], ...]:
        """Every collection and index :meth:`init_schema` creates, with the retentions of the config."""
        config = self._config
        return (
            (
                OUTBOX_COLLECTION,
                (
                    _Index("sentAt_1_createdAt_1_seq_1", (("sentAt", 1), ("createdAt", 1), ("seq", 1))),
                    _Index("sentAt_ttl", (("sentAt", 1),), ttl=_ttl_seconds(config.outbox_retention)),
                ),
            ),
            (
                PROCESSED_COLLECTION,
                (_Index("processedAt_ttl", (("processedAt", 1),), ttl=_ttl_seconds(config.processed_retention)),),
            ),
            (
                EXTERNAL_COLLECTION,
                (
                    _Index(
                        "updatedAt_ttl",
                        (("updatedAt", 1),),
                        ttl=_ttl_seconds(config.external_call_retention),
                    ),
                ),
            ),
            (
                DEAD_LETTERS_COLLECTION,
                (
                    _Index(
                        "dead_letters_processor_key_offset",
                        (("processor", 1), ("key", 1), ("offset", 1)),
                    ),
                    _Index(
                        "dead_letters_processor_parked",
                        (("processor", 1), ("parkedAt", 1)),
                    ),
                    _Index("dead_letters_expire", (("expireAt", 1),), ttl=0),
                ),
            ),
            (
                SAGAS_COLLECTION,
                (
                    _Index(
                        "type_associations_live",
                        (("type", 1), ("associations.k", 1), ("associations.v", 1)),
                        partial={"ended": False},
                    ),
                    _Index(
                        "startKey_live_unique",
                        (("startKey", 1),),
                        unique=True,
                        partial={"ended": False, "startKey": {"$exists": True}},
                    ),
                    _Index("endedAt_ttl", (("endedAt", 1),), ttl=_ttl_seconds(config.saga_ended_retention)),
                ),
            ),
            (
                SAGA_DISPATCH_COLLECTION,
                (
                    _Index("sagaId_1_seq_1", (("sagaId", 1), ("seq", 1))),
                    _Index(
                        "dispatchedAt_1_failedAt_1_sagaId_1_seq_1",
                        (("dispatchedAt", 1), ("failedAt", 1), ("sagaId", 1), ("seq", 1)),
                    ),
                    _Index(
                        "dispatchedAt_ttl",
                        (("dispatchedAt", 1),),
                        ttl=_ttl_seconds(config.saga_dispatch_retention),
                    ),
                    _Index("failedAt_ttl", (("failedAt", 1),), ttl=_ttl_seconds(config.failed_retention)),
                    _Index(
                        "noticePending_noticeNextAt",
                        (("noticeNextAt", 1),),
                        partial={"noticePending": True},
                    ),
                ),
            ),
            (
                SAGA_LEASES_COLLECTION,
                (_Index("until_ttl", (("until", 1),), ttl=0),),
            ),
            (
                DEADLINES_COLLECTION,
                (
                    _Index("status_1_dueAt_1", (("status", 1), ("dueAt", 1))),
                    _Index("sagaId_1_status_1", (("sagaId", 1), ("status", 1))),
                    _Index(
                        "finishedAt_ttl",
                        (("finishedAt", 1),),
                        ttl=_ttl_seconds(config.deadline_retention),
                    ),
                ),
            ),
            (
                SAGA_FAILURES_COLLECTION,
                (
                    _Index("sagaType_1_lastFailedAt_1", (("sagaType", 1), ("lastFailedAt", 1))),
                    _Index("lastFailedAt_ttl", (("lastFailedAt", 1),), ttl=_ttl_seconds(config.failed_retention)),
                ),
            ),
        )


@dataclass(frozen=True, slots=True)
class _Index:
    """One index of the layout: its name, ascending keys and options."""

    name: str
    keys: tuple[tuple[str, int], ...]
    ttl: int | None = None
    unique: bool = False
    partial: Mapping[str, object] | None = None


def _ttl_seconds(retention: float) -> int:
    """``retention`` in whole seconds, floored as the published retentions are stored: a positive sub-second retention
    becomes zero, and MongoDB's TTL monitor may then expire the document at once.

    TODO(spec-gap): SG-35
    """
    if retention <= 0:
        return 0
    return min(math.floor(retention), MAX_TTL_SECONDS)


def _index_model(index: _Index) -> IndexModel:
    options: dict[str, object] = {"name": index.name}
    if index.ttl is not None:
        options["expireAfterSeconds"] = index.ttl
    if index.unique:
        options["unique"] = True
    if index.partial is not None:
        options["partialFilterExpression"] = dict(index.partial)
    return IndexModel(list(index.keys), **options)


def _check_index_options(collection: str, index: _Index, actual: Mapping[str, Any]) -> None:
    """Fails unless one listed index carries the layout's TTL, uniqueness and partial filter."""
    where = f"the MongoDB message store's index {index.name!r} on {collection!r}"
    present_ttl = actual.get("expireAfterSeconds")
    if index.ttl is None and present_ttl is not None:
        raise MessageStoreFailed(f"{where} has a TTL, not none: drop it and call init_schema()")
    if index.ttl is not None and (present_ttl is None or int(present_ttl) != index.ttl):
        raise MessageStoreFailed(
            f"{where} has the TTL {present_ttl}, not {index.ttl}: every instance sharing a database needs the same "
            "retentions"
        )
    if bool(actual.get("unique", False)) != index.unique:
        raise MessageStoreFailed(
            f"{where} is unique={bool(actual.get('unique', False))}, not {index.unique}: drop it and call init_schema()"
        )
    present_partial = actual.get("partialFilterExpression") or {}
    if _normalized(present_partial) != _normalized(index.partial or {}):
        raise MessageStoreFailed(
            f"{where} filters {present_partial}, not {dict(index.partial or {})}: drop it and call init_schema()"
        )


def _normalized(value: object) -> object:
    """A partial filter without its fields' order, which carries no meaning in MongoDB; an array keeps its order."""
    if isinstance(value, Mapping):
        return tuple(sorted((str(name), _normalized(child)) for name, child in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_normalized(child) for child in value)
    return value


# ---- the outbox -----------------------------------------------------------------------------------


class _MongoOutbox:
    """The outbox on ``outbox``."""

    def __init__(self, store: MongoStore) -> None:
        self._store = store

    async def append(self, entry: OutboxEntry, *, tx: Tx | None = None) -> None:
        await self._insert(entry, self._store._session(tx))

    async def append_now(self, entry: OutboxEntry) -> None:
        await self._insert(entry, None)

    async def _insert(self, entry: OutboxEntry, session: AsyncClientSession | None) -> None:
        document = _outbox_document(entry)
        try:
            await self._store._outbox_collection.insert_one(document, session=session)
        except Exception as error:
            raise _failure(error) from error

    async def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        if not owner:
            # TODO(spec-gap): SG-31
            raise InvalidMessage("the MongoDB message store needs a non-empty claim owner")
        if limit <= 0:
            return []
        store = self._store
        collection = store._outbox_collection
        won: list[dict[str, Any]] = []
        won_ids: list[str] = []
        while len(won) < limit:
            now = store._now()
            query: dict[str, Any] = _unfinished()
            query["$or"] = _claimable(owner, now)
            if won_ids:
                query["_id"] = {"$nin": won_ids}
            try:
                candidates = await collection.find(
                    query,
                    sort=[("createdAt", 1), ("seq", 1), ("_id", 1)],
                    limit=limit - len(won),
                ).to_list(length=limit - len(won))
            except Exception as error:
                raise _failure(error) from error
            if not candidates:
                break
            for candidate in candidates:
                now = store._now()
                claimed = await self._take(candidate, owner, lease, now)
                if claimed is not None:
                    won.append(claimed)
                    won_ids.append(str(claimed["_id"]))
                    if len(won) >= limit:
                        break
        won.sort(key=lambda document: (document["createdAt"], int(document.get("seq", 0)), str(document["_id"])))
        return [_entry_from_document(document) for document in won]

    async def _take(
        self,
        candidate: Mapping[str, Any],
        owner: str,
        lease: float,
        now: int,
    ) -> dict[str, Any] | None:
        """Wins one candidate if it is still pending and claimable at ``now``; None when another relay won it."""
        query: dict[str, Any] = {**_unfinished(), "_id": candidate["_id"], "$or": _claimable(owner, now)}
        update = {"$set": {"owner": owner, "leaseUntil": _datetime(now + _milliseconds(lease))}}
        try:
            claimed = await self._store._outbox_collection.find_one_and_update(
                query,
                update,
                return_document=ReturnDocument.AFTER,
            )
        except Exception as error:
            raise _failure(error) from error
        return None if claimed is None else dict(claimed)

    async def mark_sent(self, ids: Sequence[str]) -> None:
        if not ids:
            return
        try:
            await self._store._outbox_collection.update_many(
                {"_id": {"$in": list(ids)}},
                {"$set": {"sentAt": _datetime(self._store._now())}},
            )
        except Exception as error:
            raise _failure(error) from error

    async def mark_attempt_failed(self, entry_id: str, error: str) -> None:
        try:
            await self._store._outbox_collection.update_one(
                {"_id": entry_id},
                {"$inc": {"attempts": 1}},
            )
        except Exception as cause:
            raise _failure(cause) from cause

    async def mark_failed(self, entry_id: str, error: str) -> None:
        try:
            await self._store._outbox_collection.update_one(
                {"_id": entry_id},
                {"$inc": {"attempts": 1}, "$set": {"failedAt": _datetime(self._store._now())}},
            )
        except Exception as cause:
            raise _failure(cause) from cause

    async def purge(self, sent_before_ms: int) -> int:
        try:
            result = await self._store._outbox_collection.delete_many({"sentAt": {"$lt": _datetime(sent_before_ms)}})
        except Exception as error:
            raise _failure(error) from error
        return int(result.deleted_count)

    async def pending(self) -> int:
        try:
            return int(await self._store._outbox_collection.count_documents(_unfinished()))
        except Exception as error:
            raise _failure(error) from error

    def on_append(self, wake: Callable[[], None]) -> AppendListener:
        """Registers nothing: the MongoDB store has no notification the relay could wait on, so it polls."""
        return AppendListener(lambda: None)

    async def oldest_pending(self) -> int | None:
        try:
            document = await self._store._outbox_collection.find_one(
                _unfinished(),
                sort=[("createdAt", 1), ("seq", 1)],
                projection={"createdAt": 1},
            )
        except Exception as error:
            raise _failure(error) from error
        return None if document is None else _ms_value(document.get("createdAt"))


def _unfinished() -> dict[str, Any]:
    """Neither sent nor given up on (a missing field is null)."""
    return {"sentAt": None, "failedAt": None}


def _claimable(owner: str, now: int) -> list[dict[str, Any]]:
    """Nobody holds it, ``owner`` does, or its holder's lease has passed at ``now``."""
    return [
        {"owner": None},
        {"owner": owner},
        {"leaseUntil": {"$lte": _datetime(now)}},
    ]


def _outbox_document(entry: OutboxEntry) -> dict[str, Any]:
    """The layout's outbox document for ``entry``.

    ``key``, ``sentAt`` and ``failedAt`` are written as null when they have no value; ``owner`` and ``leaseUntil`` are
    absent until a relay claims the entry, and no ``_class`` is written.
    """
    check_outbox_payload(entry.type_name, entry.payload)
    headers = _headers(entry.headers)
    payload = json.loads(entry.payload)
    return {
        "_id": entry.id,
        "type": entry.type_name,
        "key": entry.key,
        "headers": headers,
        "payload": payload,
        "createdAt": _datetime(entry.created_at_ms),
        "seq": Int64(entry.seq),
        "attempts": int(entry.attempts),
        "sentAt": None,
        "failedAt": None,
    }


def _headers(headers: Mapping[str, str]) -> dict[str, str]:
    """The headers as the layout stores them: an object whose values are all strings."""
    stored: dict[str, str] = {}
    for name, value in (headers or {}).items():
        if not isinstance(name, str) or not isinstance(value, str):
            raise InvalidMessage("an outbox entry's headers must be a map of strings")
        if _has_surrogate(name) or _has_surrogate(value):
            raise InvalidMessage("an outbox entry's headers hold a lone surrogate, which a document cannot keep")
        stored[name] = value
    return stored


def _has_surrogate(text: str) -> bool:
    return any(0xD800 <= ord(char) <= 0xDFFF for char in text)


def _entry_from_document(document: Mapping[str, Any]) -> OutboxEntry:
    """One outbox document as the core's entry; a missing optional field is none."""
    key = document.get("key")
    created = document.get("createdAt")
    if created is None:
        raise MessageStoreFailed("the MongoDB message store cannot read an outbox entry without its createdAt")
    payload = document.get("payload")
    if payload is None:
        payload = {}
    if not isinstance(payload, Mapping):
        raise MessageStoreFailed("the MongoDB message store cannot read an outbox payload that is not a document")
    try:
        text = json.dumps(_plain(payload), ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise MessageStoreFailed(f"the MongoDB message store cannot read an outbox payload: {error}") from error
    return OutboxEntry(
        id=str(document["_id"]),
        type_name=str(document.get("type", "")),
        key=None if key is None else str(key),
        headers=_read_headers(document.get("headers")),
        payload=text.encode(),
        created_at_ms=_ms_value(created),
        seq=int(document.get("seq", 0)),
        attempts=int(document.get("attempts", 0)),
    )


def _read_headers(value: object) -> dict[str, str]:
    """A stored headers object; a missing one is empty, and a non-string value reads as its JSON text."""
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise MessageStoreFailed("the MongoDB message store cannot read headers that are not a document")
    headers: dict[str, str] = {}
    for name, child in value.items():
        if isinstance(child, str):
            headers[str(name)] = child
        else:
            headers[str(name)] = json.dumps(_plain(child), ensure_ascii=False, separators=(",", ":"))
    return headers


def _plain(value: object) -> object:
    """A BSON value as the JSON value it was written from: int32 and int64 become ints, documents and arrays recurse."""
    if isinstance(value, Int64):
        return int(value)
    if isinstance(value, Mapping):
        return {str(name): _plain(child) for name, child in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(child) for child in value]
    return value


# ---- the processed events -------------------------------------------------------------------------


class _MongoProcessed:
    """The processed-event markers on ``processed_events``."""

    def __init__(self, store: MongoStore) -> None:
        self._store = store

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        session = self._store._session(tx)
        try:
            await self._store._processed_collection.insert_one(_marker_document(marker), session=session)
        except DuplicateKeyError:
            return False
        except Exception as error:
            raise _failure(error) from error
        return True

    async def exists(self, marker_id: str) -> bool:
        try:
            document = await self._store._processed_collection.find_one(
                {"_id": marker_id},
                projection={"_id": 1},
            )
        except Exception as error:
            raise _failure(error) from error
        return document is not None

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        deleted = 0
        for replay in ranges:
            query = {
                "processor": processor,
                "topic": replay.source,
                "partition": _segment_value(replay.segment),
                "offset": {"$gte": _position_value(replay.start), "$lt": _position_value(replay.until)},
            }
            try:
                result = await self._store._processed_collection.delete_many(query)
            except Exception as error:
                raise _failure(error) from error
            deleted += int(result.deleted_count)
        return deleted

    async def purge(self, before_ms: int) -> int:
        try:
            result = await self._store._processed_collection.delete_many({"processedAt": {"$lt": _datetime(before_ms)}})
        except Exception as error:
            raise _failure(error) from error
        return int(result.deleted_count)


def _marker_document(marker: ProcessedMarker) -> dict[str, Any]:
    """The layout's marker: the four position fields only when the marker carries them."""
    document: dict[str, Any] = {
        "_id": marker.id,
        "type": marker.type_name,
        "processedAt": _datetime(marker.processed_at_ms),
    }
    if marker.processor is not None:
        document["processor"] = marker.processor
    if marker.source is not None:
        document["topic"] = marker.source
    if marker.segment is not None:
        document["partition"] = _segment_value(marker.segment)
    if marker.position is not None:
        document["offset"] = _position_value(marker.position)
    return document


def _segment_value(segment: str) -> int | str:
    """A segment as stored: an int32 when it is a canonical decimal one, else the string."""
    try:
        number = int(segment, 10)
    except (TypeError, ValueError):
        return str(segment)
    if str(number) == segment and -(2**31) <= number <= 2**31 - 1:
        return number
    return str(segment)


def _position_value(position: str) -> Int64 | str:
    """A position as stored: an int64 when it is a canonical decimal one, else the string."""
    try:
        number = int(position, 10)
    except (TypeError, ValueError):
        return str(position)
    if str(number) == position and -(2**63) <= number <= 2**63 - 1:
        return Int64(number)
    return str(position)


# ---- the ports this level does not implement ------------------------------------------------------


def _unsupported(what: str) -> MessageStoreUnsupported:
    return MessageStoreUnsupported(f"{what} in the MongoDB message store (level 2)")


class _UnsupportedDeadLetters:
    async def park(self, letter: DeadLetter) -> Parked:
        raise _unsupported("dead_letters")

    async def head(self, processor: str, key: str) -> str | None:
        raise _unsupported("dead_letters")

    async def earlier(self, processor: str, key: str, position: str) -> str | None:
        raise _unsupported("dead_letters")

    async def parked_keys(self, processor: str, source: str, segments: Sequence[str]) -> set[str]:
        raise _unsupported("dead_letters")

    async def get(self, letter: str) -> DeadLetter | None:
        raise _unsupported("dead_letters")

    async def list(self, processor: str | None = None, limit: int | None = None) -> builtins.list[DeadLetter]:
        raise _unsupported("dead_letters")

    async def summaries(self, processor: str) -> builtins.list[DeadLetterSummary]:
        raise _unsupported("dead_letters")

    async def count(self, processor: str | None = None) -> int:
        raise _unsupported("dead_letters")

    async def delete(self, letter: str) -> bool:
        raise _unsupported("dead_letters")

    async def delete_in(self, letter: str, *, tx: Tx | None = None) -> bool:
        raise _unsupported("dead_letters")

    async def record_failure(self, letter: str, error: DeadLetterError, now_ms: int) -> bool:
        raise _unsupported("dead_letters")


class _UnsupportedControl:
    async def get(self, processor: str) -> ProcessorControlState:
        raise _unsupported("processor control")

    async def set_paused(self, processor: str, paused: bool, by: str, now_ms: int) -> None:
        raise _unsupported("processor control")

    async def lock(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> Locked:
        raise _unsupported("processor control")

    async def extend(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> bool:
        raise _unsupported("processor control")

    async def release(self, processor: str, operation: str, now_ms: int) -> bool:
        raise _unsupported("processor control")

    async def expire(self, processor: str, operation: str, now_ms: int) -> bool:
        raise _unsupported("processor control")

    async def set_replay(self, processor: str, ranges: Sequence[ReplayRange], now_ms: int) -> None:
        raise _unsupported("processor control")

    async def remove_range(self, processor: str, replay_range: ReplayRange, now_ms: int) -> bool:
        raise _unsupported("processor control")


class _UnsupportedExternal:
    async def insert(self, call: ExternalCall) -> bool:
        raise _unsupported("external calls")

    async def find(self, key: str) -> ExternalCall | None:
        raise _unsupported("external calls")

    async def take_over(self, seen: ExternalCall, owner: str, lease_until_ms: int, now_ms: int) -> ExternalCall | None:
        raise _unsupported("external calls")

    async def expire(self, seen: ExternalCall, now_ms: int) -> bool:
        raise _unsupported("external calls")

    async def succeed(self, key: str, owner: str, reply: str, now_ms: int) -> bool:
        raise _unsupported("external calls")

    async def fail(self, key: str, owner: str, error: str, now_ms: int) -> bool:
        raise _unsupported("external calls")

    async def mark_unknown(self, key: str, owner: str, now_ms: int) -> bool:
        raise _unsupported("external calls")

    async def reset(self, key: str, owner: str) -> bool:
        raise _unsupported("external calls")


class _UnsupportedSagas:
    async def find_by_id(self, saga_id: str, *, tx: Tx | None = None) -> SagaDocument | None:
        raise _unsupported("sagas")

    async def find_live(self, type_name: str, key: str, value: str, *, tx: Tx | None = None) -> list[SagaDocument]:
        raise _unsupported("sagas")

    async def insert(self, saga: SagaDocument, *, tx: Tx | None = None) -> None:
        raise _unsupported("sagas")

    async def update(self, saga: SagaDocument, expected_version: int, *, tx: Tx | None = None) -> bool:
        raise _unsupported("sagas")

    async def count_live_by_type(self) -> dict[str, int]:
        raise _unsupported("sagas")


class _UnsupportedDispatch:
    async def record(self, entries: Sequence[SagaDispatchDocument], *, tx: Tx | None = None) -> None:
        raise _unsupported("saga dispatch")

    async def due_sagas(self, now_ms: int, except_sagas: Sequence[str], limit: int) -> list[str]:
        raise _unsupported("saga dispatch")

    async def next_pending(self, saga_id: str) -> SagaDispatchDocument | None:
        raise _unsupported("saga dispatch")

    async def mark_dispatched(self, entry_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _unsupported("saga dispatch")

    async def schedule_retry(self, entry_id: str, attempts: int, next_attempt_at_ms: int) -> None:
        raise _unsupported("saga dispatch")

    async def mark_failed(self, entry_id: str, failure: DispatchFailure) -> None:
        raise _unsupported("saga dispatch")

    async def claim_due_notice(self, now_ms: int, reserve_until_ms: int) -> SagaDispatchDocument | None:
        raise _unsupported("saga dispatch")

    async def clear_notice(self, entry_id: str) -> None:
        raise _unsupported("saga dispatch")

    async def schedule_notice_retry(self, entry_id: str, notice_attempts: int, next_at_ms: int) -> None:
        raise _unsupported("saga dispatch")

    async def claim_lease(self, saga_id: str, owner: str, now_ms: int, until_ms: int) -> bool:
        raise _unsupported("saga dispatch")

    async def renew_lease(self, saga_id: str, owner: str, until_ms: int) -> bool:
        raise _unsupported("saga dispatch")

    async def release_lease(self, saga_id: str, owner: str) -> None:
        raise _unsupported("saga dispatch")

    async def pending(self) -> int:
        raise _unsupported("saga dispatch")


class _UnsupportedDeadlines:
    async def insert(self, deadline: DeadlineDocument, *, tx: Tx | None = None) -> None:
        raise _unsupported("deadlines")

    async def find_scheduled(self, deadline_id: str, *, tx: Tx | None = None) -> DeadlineDocument | None:
        raise _unsupported("deadlines")

    async def cancel(self, saga_id: str, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _unsupported("deadlines")

    async def cancel_by_name(self, saga_id: str, name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _unsupported("deadlines")

    async def cancel_all(self, saga_id: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _unsupported("deadlines")

    async def mark_fired(self, deadline_id: str, now_ms: int, *, tx: Tx | None = None) -> bool:
        raise _unsupported("deadlines")

    async def claim_due(self, owner: str, now_ms: int, claimed_until_ms: int) -> DeadlineDocument | None:
        raise _unsupported("deadlines")

    async def count_overdue_by_name(self, now_ms: int) -> dict[str, int]:
        raise _unsupported("deadlines")


class _UnsupportedFailures:
    async def find(self, failure_id: str, *, tx: Tx | None = None) -> SagaFailureDocument | None:
        raise _unsupported("saga failures")

    async def save(self, failure: SagaFailureDocument, *, tx: Tx | None = None) -> None:
        raise _unsupported("saga failures")

    async def delete(self, failure_id: str, *, tx: Tx | None = None) -> bool:
        raise _unsupported("saga failures")

    async def failed_again(
        self, failure_id: str, error: str, attempts: int, now_ms: int, *, tx: Tx | None = None
    ) -> bool:
        raise _unsupported("saga failures")

    async def list(self, saga_type: str | None = None, *, tx: Tx | None = None) -> builtins.list[SagaFailureDocument]:
        raise _unsupported("saga failures")


class _UnsupportedMarkers:
    async def exists(self, marker_id: str, *, tx: Tx | None = None) -> bool:
        raise _unsupported("saga markers")

    async def insert(self, marker_id: str, type_name: str, now_ms: int, *, tx: Tx | None = None) -> None:
        raise _unsupported("saga markers")


# ---- values and errors ----------------------------------------------------------------------------


def _datetime(milliseconds: int) -> datetime:
    """Epoch milliseconds as a BSON date: naive UTC, as the drivers read and write dates by default."""
    return _EPOCH + timedelta(milliseconds=milliseconds)


def _ms_value(value: object) -> int:
    """A stored date as epoch milliseconds."""
    if isinstance(value, datetime):
        moment = value if value.tzinfo is None else value.astimezone(UTC).replace(tzinfo=None)
        delta = moment - _EPOCH
        return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000
    if isinstance(value, int):
        return value
    raise MessageStoreFailed(f"the MongoDB message store cannot read a time: {value!r}")


def _milliseconds(seconds: float) -> int:
    """``seconds`` in whole milliseconds, capped at the signed 64-bit range."""
    try:
        return min(int(seconds * 1000), 2**63 - 1)
    except (OverflowError, ValueError):
        return 2**63 - 1


def _failure(error: BaseException) -> BaseException:
    """What a driver error means to the store: a duplicate key, a write conflict or a transient transaction error is a
    :class:`snodra.MessageStoreConflict` (a retry of the whole attempt resolves it); every other error is a
    :class:`snodra.MessageStoreFailed`."""
    if isinstance(error, MessageStoreUnsupported | InvalidMessage | Conflict | MessageStoreFailed):
        return error
    if isinstance(error, DuplicateKeyError):
        return Conflict(str(error))
    if isinstance(error, OperationFailure):
        if (
            error.code in (DUPLICATE_KEY, WRITE_CONFLICT)
            or error.has_error_label(TRANSIENT_TRANSACTION)
            or error.has_error_label("UnknownTransactionCommitResult")
        ):
            return Conflict(str(error))
        return MessageStoreFailed(f"the MongoDB message store: {error}")
    if isinstance(error, PyMongoError):
        return MessageStoreFailed(f"the MongoDB message store: {error}")
    return MessageStoreFailed(f"the MongoDB message store: {error}")


__all__ = [
    "COLLECTIONS",
    "DEFAULT_DEADLINE_RETENTION",
    "DEFAULT_EXTERNAL_CALL_RETENTION",
    "DEFAULT_FAILED_RETENTION",
    "DEFAULT_OUTBOX_RETENTION",
    "DEFAULT_PROCESSED_RETENTION",
    "DEFAULT_SAGA_DISPATCH_RETENTION",
    "DEFAULT_SAGA_ENDED_RETENTION",
    "NAME",
    "MongoConfig",
    "MongoStore",
    "StoreLifecycle",
]
