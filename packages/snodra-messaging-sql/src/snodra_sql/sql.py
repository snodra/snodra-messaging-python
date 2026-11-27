"""The SQL message store: PostgreSQL and MySQL on the shared layout version 1.

The store keeps the outbox, the processed-event markers, the dead letters and the processor control
state in the service's own database. Every operation joins the transaction bound to the current context
(``async with store.transaction()``) or runs in a short transaction of its own, at READ COMMITTED;
``store.transaction(conn=conn)`` binds a connection the service already runs a transaction on, and the
store never commits or rolls it back.

The layout is the published SQL version 1, byte for byte:

- ``schema/postgresql/V1__snodra.sql`` and ``schema/mysql/V1__snodra.sql``, embedded in the
  package and checked against their published SHA-256 values;
- twelve ``snodra_`` tables, UTC instants at millisecond precision stamped by the application's
  clock, hashes for values with no bound, bounds checked in code before any write;
- one schema lock for every Snodra library: PostgreSQL's session advisory lock
  ``0x534E44525343484D`` and MySQL's named lock ``snodra_schema``.

A claim takes ``SELECT ... FOR UPDATE SKIP LOCKED`` in ``(created_at, seq)`` order. On PostgreSQL an
append's transaction sends ``pg_notify('snodra_outbox', '')`` and :meth:`SqlStore.outbox`'s
``on_append`` listens on a connection of its own, so a commit wakes the relay and a rollback wakes
nobody; MySQL has no notification and the relay polls.

The dead-letter and processor-control stores use the layout's text/binary payload split and its
``replay`` array, and a letter whose expiry passed is gone for every read and write here. The
external-call and saga ports are part of the interface frozen at level 2 and report
:class:`snodra.MessageStoreUnsupported` for now. The official add-ons implement every port by level 4.

See https://snodra.damoreira.com/docs/library-authors/message-store-add-ons/.
"""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import hashlib
import inspect
import json
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from typing import Any, Final
from urllib.parse import parse_qs, unquote, urlsplit

import asyncmy
import asyncmy.cursors
import asyncpg
from asyncmy.constants import CLIENT
from snodra import (
    RESET_KIND,
    AppendListener,
    Conflict,
    ControlOperation,
    DeadLetter,
    DeadLetterError,
    DeadLetterReason,
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
    StoreLifecycle,
    Tx,
    bind_transaction,
    check_outbox_payload,
    current_transaction,
    now_ms,
    unbind_transaction,
)

log = logging.getLogger(__name__)

DRIVER_POSTGRES: Final = "postgres"
DRIVER_MYSQL: Final = "mysql"
NAME_POSTGRES: Final = "postgres"
NAME_MYSQL: Final = "mysql"

SCHEMA_VERSION: Final = 1
"""The schema version this add-on reads and writes; versions are additive."""

PUBLISHED_POSTGRES_SHA256: Final = "882bff968f8f2e13e021178f5772d124e6352e3efb3ec104f7e8e5fed254a012"
PUBLISHED_MYSQL_SHA256: Final = "ec77259da977f07ba83abe8ba9104dcf7a9e292760c87ae712639d786671557d"

SCHEMA_POSTGRES_PATH: Final = "schema/postgresql/V1__snodra.sql"
SCHEMA_MYSQL_PATH: Final = "schema/mysql/V1__snodra.sql"

SCHEMA_LOCK_POSTGRES: Final = 0x534E44525343484D  # "SNDRSCHM", the same in every library
SCHEMA_LOCK_MYSQL: Final = "snodra_schema"
SCHEMA_LOCK_WAIT: Final = 60.0
SCHEMA_LOCK_POLL: Final = 0.2
NOTIFY_CHANNEL: Final = "snodra_outbox"
NOTIFY_HEALTH_CHECK: Final = 30.0
NOTIFY_NETWORK_TIMEOUT: Final = 10.0
NOTIFY_MIN_BACKOFF: Final = 0.2
NOTIFY_MAX_BACKOFF: Final = 30.0

MAX_PROCESSOR: Final = 64
MAX_SOURCE: Final = 255
MAX_SEGMENT: Final = 128
MAX_POSITION: Final = 128
MAX_NAME: Final = 255
MAX_ID: Final = 255
FREE_TEXT_BYTES: Final = 16 * 1024
PURGE_BATCH: Final = 1000

DEFAULT_MIN_SIZE: Final = 1
DEFAULT_MAX_SIZE: Final = 10
DEFAULT_PURGE_INTERVAL: Final = 60.0
DEFAULT_OUTBOX_RETENTION: Final = 7 * 24 * 3600.0
DEFAULT_PROCESSED_RETENTION: Final = 30 * 24 * 3600.0
DEFAULT_EXTERNAL_CALL_RETENTION: Final = 30 * 24 * 3600.0
DEFAULT_SAGA_ENDED_RETENTION: Final = 30 * 24 * 3600.0
DEFAULT_SAGA_DISPATCH_RETENTION: Final = 7 * 24 * 3600.0
DEFAULT_DEADLINE_RETENTION: Final = 7 * 24 * 3600.0
DEFAULT_FAILED_RETENTION: Final = 30 * 24 * 3600.0


@dataclass(frozen=True, slots=True)
class SqlConfig:
    """How the store reaches its database and how long it keeps what the retentions let go.

    ``url`` is required and names the database: ``postgres://`` or ``postgresql://`` for PostgreSQL,
    ``mysql://`` for MySQL. ``driver`` is read from the scheme when empty. Pool sizes are in
    connections (at least two: a write that commits on its own inside the service's transaction takes
    a second one). Retentions and the purge interval are in seconds; a negative purge interval turns
    the background purge off.
    """

    url: str = field(repr=False)
    driver: str | None = None
    min_size: int = DEFAULT_MIN_SIZE
    max_size: int = DEFAULT_MAX_SIZE
    notify: bool = True
    purge_interval: float = DEFAULT_PURGE_INTERVAL
    outbox_retention: float = DEFAULT_OUTBOX_RETENTION
    processed_retention: float = DEFAULT_PROCESSED_RETENTION
    external_call_retention: float = DEFAULT_EXTERNAL_CALL_RETENTION
    saga_ended_retention: float = DEFAULT_SAGA_ENDED_RETENTION
    saga_dispatch_retention: float = DEFAULT_SAGA_DISPATCH_RETENTION
    deadline_retention: float = DEFAULT_DEADLINE_RETENTION
    failed_retention: float = DEFAULT_FAILED_RETENTION

    def __post_init__(self) -> None:
        url = self.url.strip()
        if not url:
            raise ValueError("the SQL message store needs a database URL (SqlConfig.url, SNODRA_STORE_SQL_URL)")
        driver = self.driver or _driver_from_url(url)
        if driver not in (DRIVER_POSTGRES, DRIVER_MYSQL):
            raise ValueError(
                f"the SQL message store has an unknown driver {driver!r}; it supports {DRIVER_POSTGRES!r} and "
                f"{DRIVER_MYSQL!r} (SNODRA_STORE_SQL_DRIVER)"
            )
        if self.max_size < 2:
            raise ValueError(
                "the SQL message store needs a connection pool of at least 2 (max_size has "
                f"{self.max_size}): a write that commits on its own inside the service's transaction takes a second "
                "connection"
            )
        if self.min_size < 0 or self.min_size > self.max_size:
            raise ValueError("the SQL message store needs a min_size between 0 and max_size")
        for name in (
            "outbox_retention",
            "processed_retention",
            "external_call_retention",
            "saga_ended_retention",
            "saga_dispatch_retention",
            "deadline_retention",
            "failed_retention",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"the SQL message store needs a non-negative {name}, has {getattr(self, name)}")
        object.__setattr__(self, "url", url)
        object.__setattr__(self, "driver", driver)
        if self.purge_interval == 0:
            object.__setattr__(self, "purge_interval", DEFAULT_PURGE_INTERVAL)

    @property
    def resolved_driver(self) -> str:
        """The driver the store runs on, read from the URL when the config names none."""
        return self.driver or _driver_from_url(self.url)

    @classmethod
    def from_env(cls) -> SqlConfig:
        """The config from the environment: ``SNODRA_STORE_SQL_URL`` (required), ``SNODRA_STORE_SQL_DRIVER``,
        ``SNODRA_STORE_SQL_NOTIFY`` (true or false, default true) and ``SNODRA_STORE_SQL_PURGE_INTERVAL_MS``
        (milliseconds, default 60000; 0 turns the purge off)."""
        driver = os.environ.get("SNODRA_STORE_SQL_DRIVER", "").strip().lower() or None
        if driver == "postgresql":
            driver = DRIVER_POSTGRES
        notify = True
        if os.environ.get("SNODRA_STORE_SQL_NOTIFY", "").strip().lower() in ("false", "0", "no"):
            notify = False
        purge = DEFAULT_PURGE_INTERVAL
        value = os.environ.get("SNODRA_STORE_SQL_PURGE_INTERVAL_MS", "").strip()
        if value:
            with contextlib.suppress(ValueError):
                milliseconds = int(value)
                purge = -1.0 if milliseconds <= 0 else milliseconds / 1000.0
        return cls(
            url=os.environ.get("SNODRA_STORE_SQL_URL", "").strip(),
            driver=driver,
            notify=notify,
            purge_interval=purge,
        )


def _driver_from_url(url: str) -> str:
    scheme = urlsplit(url).scheme.lower()
    if scheme in ("postgres", "postgresql"):
        return DRIVER_POSTGRES
    if scheme == "mysql":
        return DRIVER_MYSQL
    raise ValueError(
        f"the SQL message store cannot tell the database from the URL's scheme ({scheme or 'none'}); set driver to "
        f"{DRIVER_POSTGRES!r} or {DRIVER_MYSQL!r} (SNODRA_STORE_SQL_DRIVER)"
    )


@dataclass(slots=True)
class _SqlTx:
    """One transaction: the connection it runs on and whether the store began it."""

    store: SqlStore
    conn: Any
    native: Any = None
    owned: bool = False
    ended: bool = False


class _BoundScope(AbstractAsyncContextManager[Tx]):
    """Binds a service's own connection in its transaction to the current context; ends nothing."""

    def __init__(self, store: SqlStore, conn: Any) -> None:
        self._store = store
        self._conn = conn
        self._tx: Tx | None = None
        self._token: Any = None
        self._joined = False

    async def __aenter__(self) -> Tx:
        current = current_transaction()
        if (
            current is not None
            and isinstance(current.inner, _SqlTx)
            and current.inner.store is self._store
            and current.inner.conn is self._conn
            and not current.inner.ended
        ):
            self._joined = True
            return current
        self._tx = Tx(_SqlTx(self._store, self._conn, owned=False))
        self._token = bind_transaction(self._tx)
        return self._tx

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._joined:
            return
        token, self._token = self._token, None
        if token is not None:
            unbind_transaction(token)


class _SqlTransactionScope(AbstractAsyncContextManager[Tx]):
    """The core's transaction context manager, refusing a transaction bound by another message store."""

    def __init__(self, store: SqlStore) -> None:
        self._store = store
        self._tx: Tx | None = None
        self._token: Any = None
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


class SqlStore:
    """The PostgreSQL or MySQL message store: see the module docstring.

    ``clock`` is the application's clock in epoch milliseconds (the wall clock by default), for tests
    that need known times; :meth:`use_clock` replaces it.
    """

    def __init__(self, config: SqlConfig | None = None, *, clock: Callable[[], int] | None = None) -> None:
        self._config = SqlConfig.from_env() if config is None else config
        self._driver = self._config.resolved_driver
        self._dsn = self._config.url
        self._mysql = _MySQLTarget.parse(self._dsn) if self._driver == DRIVER_MYSQL else None
        self._clock: Callable[[], int] = now_ms if clock is None else clock
        self._pool: Any = None
        self._open_lock = asyncio.Lock()
        self._closed = False
        self._listeners: dict[int, Callable[[], None]] = {}
        self._next_listener = 0
        self._listener_task: asyncio.Task[None] | None = None
        self._listener_conn: Any = None
        self._listens = 0
        self._lock_step: Callable[[str], Awaitable[None]] | None = None
        """A test hook around :meth:`_SqlControl.lock`'s renew/take sequence; ``None`` in production."""
        self.outbox: OutboxStore = _SqlOutbox(self)
        self.processed: ProcessedStore = _SqlProcessed(self)
        self.dead_letters: DeadLetterStore = _SqlDeadLetters(self)
        self.control: ProcessorControlStore = _SqlControl(self)
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
        """The add-on's name: ``postgres`` or ``mysql``."""
        return self._driver

    @property
    def config(self) -> SqlConfig:
        """The config the store was built with."""
        return self._config

    @property
    def listens(self) -> int:
        """How many times the PostgreSQL listener established ``LISTEN`` (0 without one)."""
        return self._listens

    def use_clock(self, clock: Callable[[], int]) -> None:
        """The application's clock, epoch milliseconds: what the store stamps its own times with."""
        self._clock = now_ms if clock is None else clock

    async def open(self) -> None:
        """Opens the connection pool. Every operation opens it on demand; calling it more than once does nothing."""
        if self._pool is not None:
            return
        async with self._open_lock:
            if self._pool is not None:
                return
            if self._closed:
                raise MessageStoreFailed(f"the SQL message store ({self._driver}) is closed")
            try:
                if self._driver == DRIVER_POSTGRES:
                    self._pool = await asyncpg.create_pool(
                        dsn=self._dsn,
                        min_size=self._config.min_size,
                        max_size=self._config.max_size,
                    )
                else:
                    assert self._mysql is not None
                    self._pool = await asyncmy.create_pool(
                        **self._mysql.kwargs(),
                        minsize=self._config.min_size,
                        maxsize=self._config.max_size,
                        autocommit=True,
                        init_command="SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                    )
            except Exception as error:
                raise MessageStoreFailed(f"the SQL message store cannot open its pool: {error}") from error

    async def close(self) -> None:
        """Closes the pool and the PostgreSQL listener; the store is not usable afterwards. Idempotent."""
        async with self._open_lock:
            if self._closed:
                return
            self._closed = True
            await self._stop_listener()
            pool, self._pool = self._pool, None
        if pool is not None:
            with contextlib.suppress(Exception):
                if self._driver == DRIVER_POSTGRES:
                    await pool.close()
                else:
                    pool.close()
                    await pool.wait_closed()

    # ---- transactions -------------------------------------------------------------------------

    def transaction(self, conn: Any | None = None) -> AbstractAsyncContextManager[Tx]:
        """A transaction bound to the current context, or a new one.

        Without ``conn`` this is the core's transaction context manager: it joins a transaction already bound (or
        refuses one bound by another message store), begins one of its own otherwise, and commits or rolls it back.
        With ``conn`` it binds a connection the service already runs a transaction on: the store writes on
        that connection and never commits or rolls it back. ``conn`` is an ``asyncpg.Connection`` or an
        ``asyncmy`` connection.
        """
        if conn is None:
            return _SqlTransactionScope(self)
        # TODO(spec-gap): SG-32
        return _BoundScope(self, conn)

    def plain(self) -> Tx:
        """No transaction: what reads and writes outside one use."""
        return Tx.none()

    async def begin(self) -> Tx:
        """A transaction of the store's own, at READ COMMITTED."""
        await self.open()
        assert self._pool is not None
        conn = await self._pool.acquire()
        native: Any = None
        try:
            if self._driver == DRIVER_POSTGRES:
                native = conn.transaction(isolation="read_committed")
                await native.start()
            else:
                await self._exec(conn, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                await self._exec(conn, "START TRANSACTION")
        except BaseException as error:
            await self._release(conn)
            if native is not None:
                with contextlib.suppress(BaseException):
                    await native.rollback()
            if isinstance(error, asyncio.CancelledError):
                raise
            raise _failure(error) from error
        return Tx(_SqlTx(self, conn, native, owned=True))

    async def commit(self, tx: Tx) -> None:
        """Commits what ``tx`` wrote; a transaction it does not own (a service's connection) is left alone."""
        inner = self._inner(tx)
        if inner is None or not inner.owned or inner.ended:
            return
        inner.ended = True
        try:
            if self._driver == DRIVER_POSTGRES:
                await inner.native.commit()
            else:
                await inner.conn.commit()
        except Exception as error:
            raise _failure(error) from error
        finally:
            await self._release(inner.conn)

    async def rollback(self, tx: Tx) -> None:
        """Abandons ``tx``'s writes; never fails."""
        inner = self._inner(tx)
        if inner is None or not inner.owned or inner.ended:
            return
        inner.ended = True
        try:
            if self._driver == DRIVER_POSTGRES:
                await inner.native.rollback()
            else:
                await inner.conn.rollback()
        except Exception:
            pass
        finally:
            await self._release(inner.conn)

    @asynccontextmanager
    async def _scoped(self, tx: Tx | None = None) -> AsyncIterator[Any]:
        """The connection an append or a marker runs on: the bound or given transaction, or a short one of the
        store's own committed afterwards."""
        inner = self._inner(tx)
        if inner is not None:
            yield inner.conn
            return
        own = await self.begin()
        inner = self._inner(own)
        assert inner is not None
        try:
            yield inner.conn
        except BaseException:
            await self.rollback(own)
            raise
        else:
            await self.commit(own)

    @asynccontextmanager
    async def _own(self) -> AsyncIterator[Any]:
        """A connection in a transaction of the store's own at READ COMMITTED, committed afterwards, whatever
        transaction is bound to the caller's context."""
        own = await self.begin()
        inner = self._inner(own)
        assert inner is not None
        try:
            yield inner.conn
        except BaseException:
            await self.rollback(own)
            raise
        else:
            await self.commit(own)

    @asynccontextmanager
    async def _read(self) -> AsyncIterator[Any]:
        """A pooled connection for a read outside any transaction (autocommit)."""
        await self.open()
        assert self._pool is not None
        conn = await self._pool.acquire()
        try:
            yield conn
        finally:
            await self._release(conn)

    def _inner(self, tx: Tx | None) -> _SqlTx | None:
        """This store's open transaction in ``tx`` (or the bound one when ``tx`` is None); None outside one."""
        chosen = current_transaction() if tx is None else tx
        if chosen is None or chosen.inner is None:
            return None
        inner = chosen.inner
        if not isinstance(inner, _SqlTx) or inner.store is not self:
            raise MessageStoreFailed(
                f"the SQL message store ({self._driver}) was given another message store's transaction"
            )
        return None if inner.ended else inner

    async def _release(self, conn: Any) -> None:
        if self._pool is None:
            with contextlib.suppress(Exception):
                close = getattr(conn, "close", None)
                if close is not None:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
            return
        result = self._pool.release(conn)
        if inspect.isawaitable(result):
            await result

    # ---- statements ---------------------------------------------------------------------------

    def _bind(self, query: str) -> str:
        """Turns the ``?`` placeholders into the driver's own: ``$n`` for asyncpg, ``%s`` for asyncmy."""
        if self._driver == DRIVER_POSTGRES:
            pieces = query.split("?")
            if len(pieces) == 1:
                return query
            bound: list[str] = [pieces[0]]
            for number, piece in enumerate(pieces[1:], start=1):
                bound.append(f"${number}")
                bound.append(piece)
            return "".join(bound)
        return query.replace("?", "%s") if "?" in query else query

    async def _exec(self, conn: Any, query: str, args: Sequence[Any] = ()) -> int:
        """Runs a statement and returns the rows it changed (0 for a statement that changes none)."""
        bound = self._bind(query)
        try:
            if self._driver == DRIVER_POSTGRES:
                status = await conn.execute(bound, *args) if args else await conn.execute(bound)
                return _affected(status)
            async with conn.cursor() as cursor:
                if args:
                    await cursor.execute(bound, tuple(args))
                else:
                    await cursor.execute(bound)
                return max(0, int(cursor.rowcount))
        except Exception as error:
            raise _failure(error) from error

    async def _exec_many(self, conn: Any, statements: Sequence[str]) -> None:
        for statement in statements:
            await self._exec(conn, statement)

    async def _fetchall(self, conn: Any, query: str, args: Sequence[Any] = ()) -> list[dict[str, Any]]:
        bound = self._bind(query)
        try:
            if self._driver == DRIVER_POSTGRES:
                rows = await conn.fetch(bound, *args) if args else await conn.fetch(bound)
                return [dict(row) for row in rows]
            async with conn.cursor(asyncmy.cursors.DictCursor) as cursor:
                if args:
                    await cursor.execute(bound, tuple(args))
                else:
                    await cursor.execute(bound)
                rows = await cursor.fetchall()
                return list(rows)
        except Exception as error:
            raise _failure(error) from error

    async def _fetchone(self, conn: Any, query: str, args: Sequence[Any] = ()) -> dict[str, Any] | None:
        rows = await self._fetchall(conn, query, args)
        return rows[0] if rows else None

    async def _fetchval(self, conn: Any, query: str, args: Sequence[Any] = ()) -> Any:
        row = await self._fetchone(conn, query, args)
        if row is None:
            return None
        return next(iter(row.values()))

    def _json_type(self) -> str:
        return "jsonb" if self._driver == DRIVER_POSTGRES else "JSON"

    def _text_type(self) -> str:
        return "TEXT" if self._driver == DRIVER_POSTGRES else "CHAR"

    # ---- lifecycle ----------------------------------------------------------------------------

    # TODO(spec-gap): SG-30
    async def init_schema(self) -> None:
        """Checks the server and applies schema version 1 unless the database has it or later, under the shared
        database lock so instances starting together apply it once.

        :meth:`snodra.App.start` calls this (or :meth:`check_schema`) and then :meth:`background`; it is public for a
        store constructed after the app started, or used outside an app.
        """
        await self.open()
        await self._verify_server()
        if await self._schema_version() >= SCHEMA_VERSION:
            return
        await self._apply_schema()

    async def check_schema(self) -> None:
        """Fails unless the database has schema version 1 or later: the startup path of a service that applies the
        schema itself."""
        await self.open()
        await self._verify_server()
        version = await self._schema_version()
        if version < SCHEMA_VERSION:
            path = SCHEMA_POSTGRES_PATH if self._driver == DRIVER_POSTGRES else SCHEMA_MYSQL_PATH
            raise MessageStoreFailed(
                f"the SQL message store's schema is at version {version}, it needs {SCHEMA_VERSION} or later: apply "
                f"{path} (and the versions after it), or call init_schema()"
            )

    async def background(self) -> None:
        """Runs the retention purge every ``purge_interval`` seconds until cancelled. A negative interval turns it
        off and it waits for cancellation; an error is logged and the next interval runs."""
        if self._config.purge_interval < 0:
            await asyncio.Event().wait()
        while True:
            await asyncio.sleep(self._config.purge_interval)
            try:
                async with asyncio.timeout(30):
                    await self.purge_expired()
            except TimeoutError:
                log.warning("the SQL message store's purge did not finish within 30 s")
            except Exception as error:
                log.warning("the SQL message store's purge failed: %s", error)

    async def purge_expired(self) -> int:
        """Deletes what the retentions let go and the expired leases; how many rows went."""
        now = self._now()
        steps: tuple[tuple[str, str, str, int], ...] = (
            ("snodra_outbox", "id", "sent_at < ?", now - _ms(self._config.outbox_retention)),
            ("snodra_processed_events", "id_hash", "processed_at < ?", now - _ms(self._config.processed_retention)),
            ("snodra_external_calls", "id_hash", "updated_at < ?", now - _ms(self._config.external_call_retention)),
            ("snodra_sagas", "id", "ended = TRUE AND ended_at < ?", now - _ms(self._config.saga_ended_retention)),
            ("snodra_saga_dispatch", "id", "dispatched_at < ?", now - _ms(self._config.saga_dispatch_retention)),
            ("snodra_saga_dispatch", "id", "failed_at < ?", now - _ms(self._config.failed_retention)),
            ("snodra_saga_dispatch_leases", "saga_id", "lease_until < ?", now),
            ("snodra_deadlines", "id", "finished_at < ?", now - _ms(self._config.deadline_retention)),
            ("snodra_saga_failures", "id_hash", "last_failed_at < ?", now - _ms(self._config.failed_retention)),
            ("snodra_dead_letters", "id", "expire_at <= ?", now),
        )
        total = 0
        for table, key, condition, bound in steps:
            total += await self._purge_table(table, key, condition, bound)
        return total

    async def _purge_table(self, table: str, key: str, condition: str, bound: int) -> int:
        if self._driver == DRIVER_POSTGRES:
            query = (
                f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM {table} WHERE {condition} LIMIT {PURGE_BATCH})"
            )
        else:
            query = f"DELETE FROM {table} WHERE {condition} LIMIT {PURGE_BATCH}"
        total = 0
        while True:
            async with self._own() as conn:
                affected = await self._exec(conn, query, (self._time(bound),))
            total += affected
            if affected < PURGE_BATCH:
                return total

    async def _verify_server(self) -> None:
        if self._config.max_size < 2:
            raise MessageStoreFailed(
                "the SQL message store needs a connection pool of at least 2: a write that commits on its own inside "
                "the service's transaction takes a second connection"
            )
        if self._driver == DRIVER_POSTGRES:
            async with self._read() as conn:
                row = await self._fetchone(
                    conn,
                    "SELECT version() AS product, current_setting('server_version_num') AS num, "
                    "current_setting('server_version') AS version",
                )
            if row is None:
                raise MessageStoreFailed("the SQL message store cannot check the database server")
            _check_postgres_server(str(row["product"]), str(row["num"]), str(row["version"]))
            return
        async with self._read() as conn:
            row = await self._fetchone(conn, "SELECT VERSION() AS version, @@SESSION.sql_mode AS mode")
        if row is None:
            raise MessageStoreFailed("the SQL message store cannot check the database server")
        _check_mysql_server(str(row["version"]), str(row["mode"] or ""))

    async def _schema_version(self) -> int:
        async with self._read() as conn:
            return await self._schema_version_on(conn)

    async def _schema_version_on(self, conn: Any) -> int:
        cast = "BIGINT" if self._driver == DRIVER_POSTGRES else "SIGNED"
        try:
            value = await self._fetchval(conn, f"SELECT CAST(MAX(version) AS {cast}) AS v FROM snodra_schema")
        except Exception as error:
            if _missing_table(error):
                return 0
            raise MessageStoreFailed(f"the SQL message store cannot read the schema version: {error}") from error
        return 0 if value is None else int(value)

    async def _apply_schema(self) -> None:
        path, script = _schema_file(self._driver)
        statements = split_sql(script)
        if not statements:
            raise MessageStoreFailed(f"the SQL message store's embedded schema {path} holds no statement")
        conn = await self._connect_raw()
        try:
            await self._acquire_schema_lock(conn)
            try:
                if await self._schema_version_on(conn) >= SCHEMA_VERSION:
                    return
                try:
                    if self._driver == DRIVER_POSTGRES:
                        native = conn.transaction()
                        await native.start()
                        try:
                            await self._exec_many(conn, statements)
                        except BaseException:
                            await native.rollback()
                            raise
                        await native.commit()
                    else:
                        # TODO(spec-gap): SG-33
                        await self._exec_many(conn, statements)
                except Exception as error:
                    raise MessageStoreFailed(
                        f"the SQL message store could not apply its schema ({path}): apply that file yourself and "
                        "do not call init_schema(), or grant the service's database user the right to create tables. "
                        f"Cause: {error}"
                    ) from error
            finally:
                await self._release_schema_lock(conn)
        finally:
            with contextlib.suppress(Exception):
                result = conn.close()
                if inspect.isawaitable(result):
                    await result

    async def _connect_raw(self) -> Any:
        try:
            if self._driver == DRIVER_POSTGRES:
                return await asyncpg.connect(dsn=self._dsn)
            assert self._mysql is not None
            conn = await asyncmy.connect(**self._mysql.kwargs(), autocommit=True)
            await self._exec(conn, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            return conn
        except Exception as error:
            raise MessageStoreFailed(f"the SQL message store cannot open a connection: {error}") from error

    async def _acquire_schema_lock(self, conn: Any) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + SCHEMA_LOCK_WAIT
        timed_out = MessageStoreFailed(
            f"the SQL message store timed out after {SCHEMA_LOCK_WAIT:g} s waiting for another instance to apply the "
            "schema"
        )
        if self._driver == DRIVER_MYSQL:
            got = await self._fetchval(conn, "SELECT GET_LOCK(?, ?) AS got", (SCHEMA_LOCK_MYSQL, int(SCHEMA_LOCK_WAIT)))
            if got != 1:
                raise timed_out
            return
        while True:
            got = await self._fetchval(conn, "SELECT pg_try_advisory_lock(?) AS got", (SCHEMA_LOCK_POSTGRES,))
            if got:
                return
            if loop.time() >= deadline:
                raise timed_out
            await asyncio.sleep(SCHEMA_LOCK_POLL)

    async def _release_schema_lock(self, conn: Any) -> None:
        with contextlib.suppress(Exception):
            if self._driver == DRIVER_MYSQL:
                await self._fetchval(conn, "SELECT RELEASE_LOCK(?) AS released", (SCHEMA_LOCK_MYSQL,))
            else:
                await self._fetchval(conn, "SELECT pg_advisory_unlock(?) AS released", (SCHEMA_LOCK_POSTGRES,))

    # ---- the clock and the wake-up ----------------------------------------------------------------

    def _now(self) -> int:
        return int(self._clock())

    def _time(self, milliseconds: int) -> datetime:
        """A time as the driver's column takes it: aware UTC on PostgreSQL, naive UTC on MySQL."""
        moment = _datetime_from_ms(milliseconds)
        return moment if self._driver == DRIVER_POSTGRES else moment.replace(tzinfo=None)

    def on_append(self, wake: Callable[[], None]) -> AppendListener:
        """Registers ``wake`` for every append that commits. On PostgreSQL with notifications on the store listens
        on ``snodra_outbox``; on MySQL, and with notifications off, it registers nothing and the relay polls."""
        if wake is None or self._driver != DRIVER_POSTGRES or not self._config.notify or self._closed:
            return AppendListener(lambda: None)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No loop: a listener cannot run, and the relay will poll.
            return AppendListener(lambda: None)
        self._next_listener += 1
        listener = self._next_listener
        self._listeners[listener] = wake
        self._ensure_listener()

        def remove() -> None:
            self._listeners.pop(listener, None)
            if not self._listeners:
                self._stop_listener_soon()

        return AppendListener(remove)

    def _ensure_listener(self) -> None:
        if self._listener_task is not None and not self._listener_task.done():
            return
        self._listener_task = asyncio.get_running_loop().create_task(self._listen_loop(), name="snodra:sql:listener")

    def _stop_listener_soon(self) -> None:
        task = self._listener_task
        if task is not None and not task.done():
            task.cancel()

    async def _stop_listener(self) -> None:
        task, self._listener_task = self._listener_task, None
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            with contextlib.suppress(BaseException):
                await task
        conn, self._listener_conn = self._listener_conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.remove_listener(NOTIFY_CHANNEL, self._notification)
                await conn.close()

    def _notification(self, *_: Any) -> None:
        self._wake_all()

    def _wake_all(self) -> None:
        token = bind_transaction(None)
        try:
            for wake in list(self._listeners.values()):
                with contextlib.suppress(Exception):
                    wake()
        finally:
            unbind_transaction(token)

    async def _listen_loop(self) -> None:
        backoff = NOTIFY_MIN_BACKOFF
        while not self._closed:
            conn: Any = None
            try:
                conn = await asyncpg.connect(dsn=self._dsn)
                await conn.add_listener(NOTIFY_CHANNEL, self._notification)
                self._listener_conn = conn
                self._listens += 1
                # An append may have committed while nobody listened.
                self._wake_all()
                while not self._closed:
                    await asyncio.sleep(NOTIFY_HEALTH_CHECK)
                    await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=NOTIFY_NETWORK_TIMEOUT)
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if not self._closed:
                    log.warning("the SQL message store's PostgreSQL listener: %s", error)
            finally:
                self._listener_conn = None
                if conn is not None:
                    with contextlib.suppress(Exception):
                        await conn.remove_listener(NOTIFY_CHANNEL, self._notification)
                    with contextlib.suppress(Exception):
                        await conn.close()
            if self._closed:
                return
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, NOTIFY_MAX_BACKOFF)


# ---- the outbox -----------------------------------------------------------------------------------


class _SqlOutbox:
    """The outbox on ``snodra_outbox``."""

    def __init__(self, store: SqlStore) -> None:
        self._store = store

    async def append(self, entry: OutboxEntry, *, tx: Tx | None = None) -> None:
        await self._append(entry, tx)

    async def append_now(self, entry: OutboxEntry) -> None:
        await self._append(entry, None, own=True)

    async def _append(self, entry: OutboxEntry, tx: Tx | None, *, own: bool = False) -> None:
        headers = _check_outbox_entry(entry)
        store = self._store
        scope = store._own() if own else store._scoped(tx)
        async with scope as conn:
            await store._exec(
                conn,
                "INSERT INTO snodra_outbox (id, type, event_key, headers, payload, created_at, seq, attempts) "
                f"VALUES (?, ?, ?, CAST(? AS {store._json_type()}), CAST(? AS {store._json_type()}), ?, ?, ?)",
                (
                    entry.id,
                    entry.type_name,
                    entry.key,
                    headers,
                    bytes(entry.payload).decode("utf-8"),
                    store._time(entry.created_at_ms),
                    entry.seq,
                    entry.attempts,
                ),
            )
            if store._driver == DRIVER_POSTGRES and store._config.notify:
                # In the same transaction: delivered at the commit, dropped by a rollback.
                await store._fetchval(conn, "SELECT pg_notify('snodra_outbox', '') AS sent")

    async def claim(self, owner: str, lease: float, limit: int) -> list[OutboxEntry]:
        if not owner:
            # TODO(spec-gap): SG-31
            raise InvalidMessage("the SQL message store needs a non-empty claim owner")
        if limit <= 0:
            return []
        store = self._store
        now = store._now()
        until = now + _ms(lease)
        async with store._own() as conn:
            rows = await store._fetchall(
                conn,
                "SELECT id, type, event_key, "
                f"CAST(headers AS {store._text_type()}) AS headers, CAST(payload AS {store._text_type()}) AS payload, "
                "created_at, seq, attempts FROM snodra_outbox "
                "WHERE sent_at IS NULL AND failed_at IS NULL AND (owner IS NULL OR owner = ? OR lease_until <= ?) "
                "ORDER BY created_at, seq, id LIMIT ? FOR UPDATE SKIP LOCKED",
                (owner, store._time(now), int(limit)),
            )
            entries = [_entry_from_row(row) for row in rows]
            if entries:
                places = ", ".join("?" for _ in entries)
                await store._exec(
                    conn,
                    f"UPDATE snodra_outbox SET owner = ?, lease_until = ? WHERE id IN ({places})",
                    (owner, store._time(until), *(entry.id for entry in entries)),
                )
            return entries

    async def mark_sent(self, ids: Sequence[str]) -> None:
        if not ids:
            return
        store = self._store
        places = ", ".join("?" for _ in ids)
        async with store._own() as conn:
            await store._exec(
                conn,
                f"UPDATE snodra_outbox SET sent_at = ? WHERE id IN ({places})",
                (store._time(store._now()), *ids),
            )

    async def mark_attempt_failed(self, entry_id: str, error: str) -> None:
        store = self._store
        async with store._own() as conn:
            await store._exec(
                conn,
                "UPDATE snodra_outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?",
                (_cut_free_text(error), entry_id),
            )

    async def mark_failed(self, entry_id: str, error: str) -> None:
        store = self._store
        async with store._own() as conn:
            await store._exec(
                conn,
                "UPDATE snodra_outbox SET attempts = attempts + 1, last_error = ?, failed_at = ? WHERE id = ?",
                (_cut_free_text(error), store._time(store._now()), entry_id),
            )

    async def purge(self, sent_before_ms: int) -> int:
        return await self._store._purge_table("snodra_outbox", "id", "sent_at < ?", sent_before_ms)

    async def pending(self) -> int:
        store = self._store
        async with store._read() as conn:
            value = await store._fetchval(
                conn, "SELECT COUNT(*) AS n FROM snodra_outbox WHERE sent_at IS NULL AND failed_at IS NULL"
            )
        return 0 if value is None else int(value)

    def on_append(self, wake: Callable[[], None]) -> AppendListener:
        return self._store.on_append(wake)

    async def oldest_pending(self) -> int | None:
        store = self._store
        async with store._read() as conn:
            row = await store._fetchone(
                conn,
                "SELECT created_at FROM snodra_outbox WHERE sent_at IS NULL AND failed_at IS NULL "
                "ORDER BY created_at, seq LIMIT 1",
            )
        return None if row is None else _ms_value(row["created_at"])


def _entry_from_row(row: dict[str, Any]) -> OutboxEntry:
    key = row["event_key"]
    return OutboxEntry(
        id=str(row["id"]),
        type_name=str(row["type"]),
        key=None if key is None else str(key),
        headers=_headers_from_json(row["headers"]),
        payload=str(row["payload"]).encode(),
        created_at_ms=_ms_value(row["created_at"]),
        seq=int(row["seq"]),
        attempts=int(row["attempts"]),
    )


# ---- the processed events -------------------------------------------------------------------------


class _SqlProcessed:
    """The processed-event markers on ``snodra_processed_events``."""

    def __init__(self, store: SqlStore) -> None:
        self._store = store

    async def record(self, marker: ProcessedMarker, *, tx: Tx | None = None) -> bool:
        _check_marker(marker)
        store = self._store
        async with store._scoped(tx) as conn:
            query = (
                "INSERT INTO snodra_processed_events "
                "(id_hash, id, type, processed_at, processor, source, segment, position, position_num) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            if store._driver == DRIVER_POSTGRES:
                query += " ON CONFLICT (id_hash) DO NOTHING"
            try:
                affected = await store._exec(
                    conn,
                    query,
                    (
                        _sha256(marker.id),
                        marker.id,
                        marker.type_name,
                        store._time(marker.processed_at_ms),
                        marker.processor or None,
                        marker.source or None,
                        marker.segment or None,
                        marker.position or None,
                        _position_number(marker.position or ""),
                    ),
                )
            except Exception as error:
                if store._driver == DRIVER_MYSQL and _duplicate(error):
                    return False
                raise
            return affected == 1

    async def exists(self, marker_id: str) -> bool:
        store = self._store
        async with store._read() as conn:
            value = await store._fetchval(
                conn,
                "SELECT COUNT(*) AS n FROM snodra_processed_events WHERE id_hash = ?",
                (_sha256(marker_id),),
            )
        return bool(value)

    async def delete_markers(self, processor: str, ranges: Sequence[ReplayRange]) -> int:
        if not ranges:
            return 0
        _check_optional_text("a reset's processor", processor, MAX_PROCESSOR)
        store = self._store
        deleted = 0
        async with store._own() as conn:
            for replay in ranges:
                _check_optional_text("a reset range's source", replay.source, MAX_SOURCE)
                _check_optional_text("a reset range's segment", replay.segment, MAX_SEGMENT)
                _check_optional_text("a reset range's from position", replay.start, MAX_POSITION)
                _check_optional_text("a reset range's until position", replay.until, MAX_POSITION)
                from_clause, from_arg = _position_bound(">=", replay.start)
                until_clause, until_arg = _position_bound("<", replay.until)
                deleted += await store._exec(
                    conn,
                    "DELETE FROM snodra_processed_events WHERE processor = ? AND source = ? AND segment = ? "
                    f"AND {from_clause} AND {until_clause}",
                    (processor, replay.source, replay.segment, from_arg, until_arg),
                )
        return deleted

    async def purge(self, before_ms: int) -> int:
        return await self._store._purge_table("snodra_processed_events", "id_hash", "processed_at < ?", before_ms)


def _position_bound(op: str, position: str) -> tuple[str, Any]:
    number = _position_number(position)
    if number is not None:
        return f"position_num {op} ?", number
    return f"position_num IS NULL AND position {op} ?", position


# ---- the dead letters -----------------------------------------------------------------------------


class _SqlDeadLetters:
    """The dead letters on ``snodra_dead_letters``. Every write but :meth:`delete_in` commits on its own; a letter
    whose ``expire_at`` passed is gone for every read and write here (it no longer blocks its key), and the purge
    deletes it. Positions are ordered as the layout says: decimal ones by value, then the others by their bytes; a
    bound compares only positions of its own kind."""

    def __init__(self, store: SqlStore) -> None:
        self._store = store

    def _columns(self) -> str:
        return (
            "id, processor, source, segment, position, event_key, event_id, event_type, "
            f"CAST(headers AS {self._store._text_type()}) AS headers, payload, payload_binary, reason, "
            "error_type, error_message, blocked_by, attempts, parked_at, last_attempt_at, expire_at, replay"
        )

    async def park(self, letter: DeadLetter) -> Parked:
        store = self._store
        _check_text("a dead letter's id", letter.id, 578)
        _check_optional_text("a dead letter's processor", letter.processor, MAX_PROCESSOR)
        _check_optional_text("a dead letter's source", letter.source, MAX_SOURCE)
        _check_optional_text("a dead letter's segment", letter.segment, MAX_SEGMENT)
        _check_optional_text("a dead letter's position", letter.position, MAX_POSITION)
        if letter.key is not None:
            _check_text("a dead letter's key", letter.key, -1)
        if letter.event_id is not None:
            _check_text("a dead letter's event id", letter.event_id, -1)
        if letter.event_type is not None:
            _check_text("a dead letter's event type", letter.event_type, -1)
        if letter.blocked_by is not None:
            _check_text("a dead letter's blocked-by id", letter.blocked_by, -1)
        headers = _headers_json(letter.headers)
        payload, payload_binary = _dead_letter_payload(letter.payload)
        error_type = _cut_free_text(letter.error.type_name)
        error_message = _cut_free_text(letter.error.message)
        key_hash = None if letter.key is None else _sha256(letter.key)
        now = store._now()
        query = (
            "INSERT INTO snodra_dead_letters (id, processor, source, segment, position, position_num, "
            "event_key, event_key_hash, event_id, event_type, headers, payload, payload_binary, reason, "
            "error_type, error_message, blocked_by, attempts, parked_at, last_attempt_at, expire_at, replay) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS {store._json_type()}), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if store._driver == DRIVER_POSTGRES:
            query += " ON CONFLICT (id) DO NOTHING"
        async with store._own() as conn:
            # An expired letter of the same id no longer exists: its id is free again.
            await store._exec(
                conn,
                "DELETE FROM snodra_dead_letters WHERE id = ? AND expire_at <= ?",
                (letter.id, store._time(now)),
            )
            try:
                affected = await store._exec(
                    conn,
                    query,
                    (
                        letter.id,
                        letter.processor,
                        letter.source,
                        letter.segment,
                        letter.position,
                        _position_number(letter.position),
                        letter.key,
                        key_hash,
                        letter.event_id,
                        letter.event_type,
                        headers,
                        payload,
                        payload_binary,
                        letter.reason.value,
                        error_type,
                        error_message,
                        letter.blocked_by,
                        letter.attempts,
                        store._time(letter.parked_at_ms),
                        store._time(letter.last_attempt_at_ms),
                        store._time(letter.expire_at_ms),
                        letter.replay,
                    ),
                )
            except Conflict as error:
                if store._driver == DRIVER_MYSQL and _duplicate(error):
                    return Parked.ALREADY
                raise
        return Parked.NEW if affected == 1 else Parked.ALREADY

    async def head(self, processor: str, key: str) -> str | None:
        _check_optional_text("a dead letter's processor", processor, MAX_PROCESSOR)
        _check_text("a dead letter's key", key, -1)
        store = self._store
        async with store._read() as conn:
            row = await store._fetchone(
                conn,
                "SELECT id FROM snodra_dead_letters WHERE processor = ? AND event_key_hash = ? "
                "AND reason <> 'INVALID' AND expire_at > ? "
                "ORDER BY position_num IS NULL, position_num, position, id LIMIT 1",
                (processor, _sha256(key), store._time(store._now())),
            )
        return None if row is None else str(row["id"])

    async def earlier(self, processor: str, key: str, position: str) -> str | None:
        _check_optional_text("a dead letter's processor", processor, MAX_PROCESSOR)
        _check_text("a dead letter's key", key, -1)
        _check_text("a dead letter's position", position, MAX_POSITION)
        bound, argument = _position_bound("<", position)
        store = self._store
        async with store._read() as conn:
            row = await store._fetchone(
                conn,
                "SELECT id FROM snodra_dead_letters WHERE processor = ? AND event_key_hash = ? "
                "AND reason <> 'INVALID' AND expire_at > ? AND "
                f"{bound} ORDER BY position_num IS NULL, position_num, position, id LIMIT 1",
                (processor, _sha256(key), store._time(store._now()), argument),
            )
        return None if row is None else str(row["id"])

    async def parked_keys(self, processor: str, source: str, segments: Sequence[str]) -> set[str]:
        if not segments:
            return set()
        _check_optional_text("a dead letter's processor", processor, MAX_PROCESSOR)
        _check_optional_text("a dead letter's source", source, MAX_SOURCE)
        for segment in segments:
            _check_text("a dead letter's segment", segment, MAX_SEGMENT)
        store = self._store
        places = ", ".join("?" for _ in segments)
        async with store._read() as conn:
            rows = await store._fetchall(
                conn,
                "SELECT DISTINCT event_key FROM snodra_dead_letters "
                f"WHERE processor = ? AND source = ? AND segment IN ({places}) "
                "AND event_key IS NOT NULL AND reason <> 'INVALID' AND expire_at > ?",
                (processor, source, *segments, store._time(store._now())),
            )
        return {str(row["event_key"]) for row in rows}

    async def get(self, letter: str) -> DeadLetter | None:
        _check_text("a dead letter's id", letter, 578)
        store = self._store
        async with store._read() as conn:
            row = await store._fetchone(
                conn,
                f"SELECT {self._columns()} FROM snodra_dead_letters WHERE id = ? AND expire_at > ?",
                (letter, store._time(store._now())),
            )
        return None if row is None else _dead_letter_from_row(row)

    async def list(self, processor: str | None = None, limit: int | None = None) -> builtins.list[DeadLetter]:
        _check_optional_text("a dead letter's processor", processor or "", MAX_PROCESSOR)
        store = self._store
        query = f"SELECT {self._columns()} FROM snodra_dead_letters WHERE expire_at > ?"
        args: list[Any] = [store._time(store._now())]
        if processor is not None:
            query += " AND processor = ?"
            args.append(processor)
        query += " ORDER BY parked_at, id"
        if limit is not None:
            query += " LIMIT ?"
            args.append(max(0, int(limit)))
        async with store._read() as conn:
            rows = await store._fetchall(conn, query, tuple(args))
        return [_dead_letter_from_row(row) for row in rows]

    async def summaries(self, processor: str) -> builtins.list[DeadLetterSummary]:
        _check_optional_text("a dead letter's processor", processor, MAX_PROCESSOR)
        store = self._store
        async with store._read() as conn:
            rows = await store._fetchall(
                conn,
                "SELECT id, event_key, position, reason, parked_at FROM snodra_dead_letters "
                "WHERE processor = ? AND expire_at > ? "
                "ORDER BY position_num IS NULL, position_num, position, id",
                (processor, store._time(store._now())),
            )
        return [
            DeadLetterSummary(
                id=str(row["id"]),
                key=None if row["event_key"] is None else str(row["event_key"]),
                position=str(row["position"]),
                reason=DeadLetterReason(str(row["reason"])),
                parked_at_ms=_ms_value(row["parked_at"]),
            )
            for row in rows
        ]

    async def count(self, processor: str | None = None) -> int:
        _check_optional_text("a dead letter's processor", processor or "", MAX_PROCESSOR)
        store = self._store
        query = "SELECT COUNT(*) AS n FROM snodra_dead_letters WHERE expire_at > ?"
        args: list[Any] = [store._time(store._now())]
        if processor is not None:
            query += " AND processor = ?"
            args.append(processor)
        async with store._read() as conn:
            value = await store._fetchval(conn, query, tuple(args))
        return 0 if value is None else int(value)

    async def delete(self, letter: str) -> bool:
        _check_text("a dead letter's id", letter, 578)
        store = self._store
        async with store._own() as conn:
            affected = await store._exec(
                conn,
                "DELETE FROM snodra_dead_letters WHERE id = ? AND expire_at > ?",
                (letter, store._time(store._now())),
            )
        return affected > 0

    async def delete_in(self, letter: str, *, tx: Tx | None = None) -> bool:
        _check_text("a dead letter's id", letter, 578)
        store = self._store
        async with store._scoped(tx) as conn:
            affected = await store._exec(
                conn,
                "DELETE FROM snodra_dead_letters WHERE id = ? AND expire_at > ?",
                (letter, store._time(store._now())),
            )
        return affected > 0

    async def record_failure(self, letter: str, error: DeadLetterError, now_ms: int) -> bool:
        _check_text("a dead letter's id", letter, 578)
        store = self._store
        async with store._own() as conn:
            affected = await store._exec(
                conn,
                "UPDATE snodra_dead_letters SET attempts = attempts + 1, error_type = ?, error_message = ?, "
                "last_attempt_at = ?, reason = 'HANDLER_FAILED', blocked_by = NULL "
                "WHERE id = ? AND expire_at > ?",
                (
                    _cut_free_text(error.type_name),
                    _cut_free_text(error.message),
                    store._time(now_ms),
                    letter,
                    store._time(store._now()),
                ),
            )
        return affected > 0


def _dead_letter_payload(payload: bytes) -> tuple[str | None, bytes | None]:
    """The text or binary column a letter's payload needs: text only when it is UTF-8 without U+0000."""
    raw = bytes(payload)
    if not raw:
        return "", None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None, raw
    if "\x00" in text:
        return None, raw
    return text, None


def _dead_letter_from_row(row: dict[str, Any]) -> DeadLetter:
    payload = row["payload"]
    if payload is not None:
        raw = str(payload).encode()
    else:
        raw = b"" if row["payload_binary"] is None else bytes(row["payload_binary"])
    return DeadLetter(
        id=str(row["id"]),
        processor=str(row["processor"]),
        source=str(row["source"]),
        segment=str(row["segment"]),
        position=str(row["position"]),
        key=None if row["event_key"] is None else str(row["event_key"]),
        event_id=None if row["event_id"] is None else str(row["event_id"]),
        event_type=None if row["event_type"] is None else str(row["event_type"]),
        headers=_headers_from_json(row["headers"]),
        payload=raw,
        reason=DeadLetterReason(str(row["reason"])),
        error=DeadLetterError(str(row["error_type"]), str(row["error_message"])),
        blocked_by=None if row["blocked_by"] is None else str(row["blocked_by"]),
        attempts=int(row["attempts"]),
        parked_at_ms=_ms_value(row["parked_at"]),
        last_attempt_at_ms=_ms_value(row["last_attempt_at"]),
        expire_at_ms=_ms_value(row["expire_at"]),
        replay=bool(row["replay"]),
    )


# ---- the processor control ------------------------------------------------------------------------


class _SqlControl:
    """The processor control state on ``snodra_processor_control``: one row per processor, created by the first write
    that needs it. Every write commits on its own; a lock is one conditional ``UPDATE`` after a renewal attempt."""

    def __init__(self, store: SqlStore) -> None:
        self._store = store

    def _columns(self) -> str:
        return (
            "processor, paused, paused_at, paused_by, op_id, op_kind, op_lease_until, op_prepared_at, "
            f"CAST(replay AS {self._store._text_type()}) AS replay, updated_at"
        )

    async def get(self, processor: str) -> ProcessorControlState:
        _check_optional_text("a processor's name", processor, MAX_PROCESSOR)
        store = self._store
        async with store._read() as conn:
            row = await store._fetchone(
                conn,
                f"SELECT {self._columns()} FROM snodra_processor_control WHERE processor = ?",
                (processor,),
            )
        return _control_state(processor, row)

    async def _get_on(self, conn: Any, processor: str) -> ProcessorControlState:
        row = await self._store._fetchone(
            conn,
            f"SELECT {self._columns()} FROM snodra_processor_control WHERE processor = ?",
            (processor,),
        )
        return _control_state(processor, row)

    async def _ensure(self, conn: Any, processor: str) -> None:
        _check_text("a processor's name", processor, MAX_PROCESSOR)
        store = self._store
        query = (
            "INSERT INTO snodra_processor_control (processor, paused, replay) "
            f"VALUES (?, FALSE, CAST(? AS {store._json_type()}))"
        )
        if store._driver == DRIVER_POSTGRES:
            query += " ON CONFLICT (processor) DO NOTHING"
        else:
            query += " ON DUPLICATE KEY UPDATE processor = processor"
        await store._exec(conn, query, (processor, "[]"))

    async def set_paused(self, processor: str, paused: bool, by: str, now_ms: int) -> None:
        _check_text("a processor's name", processor, MAX_PROCESSOR)
        _check_text("a pause's control id", by, -1)
        store = self._store
        async with store._own() as conn:
            await self._ensure(conn, processor)
            await store._exec(
                conn,
                "UPDATE snodra_processor_control SET paused = ?, paused_at = ?, paused_by = ?, updated_at = ? "
                "WHERE processor = ?",
                (
                    paused,
                    store._time(now_ms) if paused else None,
                    by if paused else None,
                    store._time(now_ms),
                    processor,
                ),
            )

    async def lock(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> Locked:
        _check_text("a processor's name", processor, MAX_PROCESSOR)
        _check_text("a reset's operation id", operation, -1)
        store = self._store
        async with store._own() as conn:
            await self._ensure(conn, processor)
            renewed = await store._exec(
                conn,
                "UPDATE snodra_processor_control SET op_lease_until = ?, updated_at = ? "
                "WHERE processor = ? AND op_id = ?",
                (store._time(lease_until_ms), store._time(now_ms), processor, operation),
            )
            if renewed == 1:
                return Locked.was_taken()
            if store._lock_step is not None:
                await store._lock_step("renewed")
            taken = await store._exec(
                conn,
                "UPDATE snodra_processor_control SET op_id = ?, op_kind = ?, op_lease_until = ?, "
                "op_prepared_at = ?, updated_at = ? "
                "WHERE processor = ? AND (op_id IS NULL OR op_lease_until <= ?)",
                (
                    operation,
                    RESET_KIND,
                    store._time(lease_until_ms),
                    store._time(now_ms),
                    store._time(now_ms),
                    processor,
                    store._time(now_ms),
                ),
            )
            if taken == 1:
                # The take matched: hold the transaction (and its row lock) here when a test asks,
                # so another instance's take queues behind this commit.
                if store._lock_step is not None:
                    await store._lock_step("taken")
                return Locked.was_taken()
            state = await self._get_on(conn, processor)
            if state.operation is not None and state.operation.id == operation:
                # Another instance prepared the same operation concurrently: this transaction's
                # renewal ran before that prepare committed. A fresh statement sees the commit,
                # and a prepare for the operation that holds the lock is a renewal, not a refusal.
                renewed = await store._exec(
                    conn,
                    "UPDATE snodra_processor_control SET op_lease_until = ?, updated_at = ? "
                    "WHERE processor = ? AND op_id = ?",
                    (store._time(lease_until_ms), store._time(now_ms), processor, operation),
                )
                if renewed == 1:
                    return Locked.was_taken()
            return Locked(False, state.operation)

    async def extend(self, processor: str, operation: str, lease_until_ms: int, now_ms: int) -> bool:
        _check_text("a processor's name", processor, MAX_PROCESSOR)
        _check_text("a reset's operation id", operation, -1)
        store = self._store
        async with store._own() as conn:
            affected = await store._exec(
                conn,
                "UPDATE snodra_processor_control SET op_lease_until = ?, updated_at = ? "
                "WHERE processor = ? AND op_id = ?",
                (store._time(lease_until_ms), store._time(now_ms), processor, operation),
            )
        return affected == 1

    async def release(self, processor: str, operation: str, now_ms: int) -> bool:
        return await self._clear(processor, operation, now_ms, expired_only=False)

    async def expire(self, processor: str, operation: str, now_ms: int) -> bool:
        return await self._clear(processor, operation, now_ms, expired_only=True)

    async def _clear(self, processor: str, operation: str, now_ms: int, *, expired_only: bool) -> bool:
        _check_text("a processor's name", processor, MAX_PROCESSOR)
        _check_text("a reset's operation id", operation, -1)
        store = self._store
        query = (
            "UPDATE snodra_processor_control SET op_id = NULL, op_kind = NULL, op_lease_until = NULL, "
            "op_prepared_at = NULL, updated_at = ? WHERE processor = ? AND op_id = ?"
        )
        args: list[Any] = [store._time(now_ms), processor, operation]
        if expired_only:
            query += " AND op_lease_until <= ?"
            args.append(store._time(now_ms))
        async with store._own() as conn:
            affected = await store._exec(conn, query, tuple(args))
        return affected == 1

    async def set_replay(self, processor: str, ranges: Sequence[ReplayRange], now_ms: int) -> None:
        _check_text("a processor's name", processor, MAX_PROCESSOR)
        encoded = _encode_replay(ranges)
        store = self._store
        async with store._own() as conn:
            await self._ensure(conn, processor)
            await store._exec(
                conn,
                "UPDATE snodra_processor_control "
                f"SET replay = CAST(? AS {store._json_type()}), updated_at = ? WHERE processor = ?",
                (encoded, store._time(now_ms), processor),
            )

    async def remove_range(self, processor: str, replay_range: ReplayRange, now_ms: int) -> bool:
        _check_text("a processor's name", processor, MAX_PROCESSOR)
        _encode_replay((replay_range,))
        store = self._store
        async with store._own() as conn:
            row = await store._fetchone(
                conn,
                "SELECT "
                f"CAST(replay AS {store._text_type()}) AS replay FROM snodra_processor_control "
                "WHERE processor = ? FOR UPDATE",
                (processor,),
            )
            if row is None:
                return False
            ranges = list(_decode_replay(row["replay"]))
            try:
                ranges.remove(replay_range)
            except ValueError:
                return False
            await store._exec(
                conn,
                "UPDATE snodra_processor_control "
                f"SET replay = CAST(? AS {store._json_type()}), updated_at = ? WHERE processor = ?",
                (_encode_replay(ranges), store._time(now_ms), processor),
            )
        return True


def _control_state(processor: str, row: dict[str, Any] | None) -> ProcessorControlState:
    if row is None:
        return ProcessorControlState(processor)
    operation: ControlOperation | None = None
    if row["op_id"] is not None:
        lease = row["op_lease_until"]
        prepared = row["op_prepared_at"]
        operation = ControlOperation(
            id=str(row["op_id"]),
            kind=RESET_KIND if row["op_kind"] is None else str(row["op_kind"]),
            lease_until_ms=0 if lease is None else _ms_value(lease),
            prepared_at_ms=0 if prepared is None else _ms_value(prepared),
        )
    paused_at = row["paused_at"]
    updated_at = row["updated_at"]
    return ProcessorControlState(
        processor=str(row["processor"]),
        paused=bool(row["paused"]),
        paused_at_ms=None if paused_at is None else _ms_value(paused_at),
        paused_by=None if row["paused_by"] is None else str(row["paused_by"]),
        operation=operation,
        replay=_decode_replay(row["replay"]),
        updated_at_ms=None if updated_at is None else _ms_value(updated_at),
    )


def _encode_replay(ranges: Sequence[ReplayRange]) -> str:
    stored: list[dict[str, str]] = []
    for replay in ranges:
        _check_optional_text("a replay range's source", replay.source, MAX_SOURCE)
        _check_optional_text("a replay range's segment", replay.segment, MAX_SEGMENT)
        _check_optional_text("a replay range's from position", replay.start, MAX_POSITION)
        _check_optional_text("a replay range's until position", replay.until, MAX_POSITION)
        stored.append({"source": replay.source, "segment": replay.segment, "from": replay.start, "until": replay.until})
    return json.dumps(stored, ensure_ascii=False, separators=(",", ":"))


def _decode_replay(text: Any) -> tuple[ReplayRange, ...]:
    if text is None:
        return ()
    raw = str(text)
    if not raw.strip() or raw.strip() == "null":
        return ()
    try:
        stored: Any = json.loads(raw)
    except ValueError as error:
        raise MessageStoreFailed(f"the SQL message store cannot read a replay column: {error}") from error
    if not isinstance(stored, list):
        raise MessageStoreFailed("the SQL message store cannot read a replay column: not an array")
    ranges: list[ReplayRange] = []
    for item in stored:
        if not isinstance(item, dict):
            continue
        source = item.get("source")
        segment = item.get("segment")
        start = item.get("from")
        until = item.get("until")
        if not isinstance(source, str) or not isinstance(segment, str):
            continue
        if not isinstance(start, str) or not isinstance(until, str):
            continue
        ranges.append(ReplayRange(source=source, segment=segment, start=start, until=until))
    return tuple(ranges)


# ---- the ports this level does not implement ------------------------------------------------------


def _unsupported(what: str) -> MessageStoreUnsupported:
    return MessageStoreUnsupported(f"{what} in the SQL message store (level 2)")


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


# ---- values, bounds and drivers -------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _MySQLTarget:
    """The parts of a ``mysql://`` URL the driver takes."""

    host: str
    port: int
    user: str
    password: str = field(repr=False)
    database: str
    charset: str
    options: dict[str, Any]

    @classmethod
    def parse(cls, url: str) -> _MySQLTarget:
        parsed = urlsplit(url)
        if parsed.scheme.lower() != "mysql":
            raise ValueError("the SQL message store reads a MySQL URL as mysql://user:password@host:port/database")
        database = unquote(parsed.path.lstrip("/"))
        if not database:
            raise ValueError("the MySQL URL names no database")
        query = parse_qs(parsed.query, keep_blank_values=True)
        options: dict[str, Any] = {}
        for name, values in query.items():
            value = values[-1]
            if name in ("parseTime", "loc", "multiStatements"):
                continue
            if name in ("port", "connect_timeout", "read_timeout", "write_timeout"):
                with contextlib.suppress(ValueError):
                    options[name] = int(value)
                    continue
            options[name] = value
        charset = str(options.pop("charset", "utf8mb4"))
        return cls(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=unquote(parsed.username or ""),
            password=unquote(parsed.password or ""),
            database=database,
            charset=charset,
            options=options,
        )

    def kwargs(self) -> dict[str, Any]:
        """The keyword arguments ``asyncmy.connect`` and ``asyncmy.create_pool`` take. ``CLIENT_FOUND_ROWS`` makes an
        update report the rows it matched, as the Kotlin and Rust drivers report them, so a renewal that writes the
        values already there still counts."""
        return {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "db": self.database,
            "charset": self.charset,
            "client_flag": CLIENT.FOUND_ROWS,
            **self.options,
        }


def _schema_file(driver: str) -> tuple[str, str]:
    path = SCHEMA_POSTGRES_PATH if driver == DRIVER_POSTGRES else SCHEMA_MYSQL_PATH
    return path, files("snodra_sql").joinpath(path).read_text(encoding="utf-8")


def split_sql(script: str) -> list[str]:
    """Splits a schema script into its statements, dropping comments.

    Single and double quotes (with their doubled-quote escape), MySQL backticks and backslash escapes in
    quoted strings are understood, so a semicolon inside one does not end a statement.
    """
    statements: list[str] = []
    statement: list[str] = []
    index = 0
    length = len(script)
    while index < length:
        char = script[index]
        if char == "-" and index + 1 < length and script[index + 1] == "-":
            while index < length and script[index] != "\n":
                index += 1
        elif char == "/" and index + 1 < length and script[index + 1] == "*":
            index += 2
            while index + 1 < length and (script[index] != "*" or script[index + 1] != "/"):
                index += 1
            index = min(length, index + 2)
            statement.append(" ")
        elif char in ("'", '"', "`"):
            quote = char
            statement.append(char)
            index += 1
            while index < length:
                statement.append(script[index])
                if script[index] == quote:
                    if index + 1 < length and script[index + 1] == quote:
                        statement.append(script[index + 1])
                        index += 2
                        continue
                    index += 1
                    break
                if script[index] == "\\" and quote != "`":
                    index += 1
                    if index < length:
                        statement.append(script[index])
                        index += 1
                    continue
                index += 1
        elif char == ";":
            text = "".join(statement).strip()
            if text:
                statements.append(text)
            statement = []
            index += 1
        else:
            statement.append(char)
            index += 1
    text = "".join(statement).strip()
    if text:
        statements.append(text)
    return statements


def _check_text(field_name: str, value: str, maximum: int) -> None:
    if not isinstance(value, str):
        raise InvalidMessage(f"the SQL message store needs {field_name} to be text, got {type(value).__name__}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise InvalidMessage(f"the SQL message store refuses text that is not UTF-8 in {field_name}") from error
    if "\x00" in value:
        raise InvalidMessage(f"the SQL message store refuses the character U+0000 in {field_name}")
    if maximum >= 0 and len(value) > maximum:
        raise InvalidMessage(
            f"the SQL message store keeps {field_name} of at most {maximum} characters; this one has {len(value)}"
        )


def _check_optional_text(field_name: str, value: str, maximum: int) -> None:
    if value:
        _check_text(field_name, value, maximum)


def _check_outbox_entry(entry: OutboxEntry) -> str:
    check_outbox_payload(entry.type_name, entry.payload)
    _check_text("an outbox entry's id", entry.id, MAX_ID)
    _check_text("an outbox entry's type", entry.type_name, MAX_NAME)
    if entry.key is not None:
        _check_text("an outbox entry's key", entry.key, -1)
    headers = _headers_json(entry.headers)
    if _payload_has_nul(entry.payload):
        raise InvalidMessage(
            f"the SQL message store refuses the character U+0000 in the payload of event {entry.type_name!r}"
        )
    return headers


def _check_marker(marker: ProcessedMarker) -> None:
    _check_text("a marker's id", marker.id, -1)
    _check_text("a marker's type", marker.type_name, -1)
    _check_optional_text("a marker's processor", marker.processor or "", MAX_PROCESSOR)
    _check_optional_text("a marker's source", marker.source or "", MAX_SOURCE)
    _check_optional_text("a marker's segment", marker.segment or "", MAX_SEGMENT)
    _check_optional_text("a marker's position", marker.position or "", MAX_POSITION)


def _headers_json(headers: dict[str, str]) -> str:
    if headers is None:
        headers = {}
    for name, value in headers.items():
        _check_text("a header name", name, -1)
        _check_text("a header value", value, -1)
    return json.dumps(headers, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _headers_from_json(text: Any) -> dict[str, str]:
    if text is None:
        return {}
    raw = str(text)
    if not raw.strip() or raw.strip() == "null":
        return {}
    try:
        fields: Any = json.loads(raw)
    except ValueError as error:
        raise MessageStoreFailed(f"the SQL message store cannot read a headers column: {error}") from error
    if not isinstance(fields, dict):
        raise MessageStoreFailed("the SQL message store cannot read a headers column: not an object")
    headers: dict[str, str] = {}
    for name, value in fields.items():
        if value is None:
            headers[str(name)] = "null"
        elif isinstance(value, str):
            headers[str(name)] = value
        else:
            headers[str(name)] = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return headers


def _payload_has_nul(payload: bytes) -> bool:
    try:
        value: Any = json.loads(payload)
    except (ValueError, RecursionError):
        return False
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            if "\x00" in item:
                return True
        elif isinstance(item, list):
            pending.extend(item)
        elif isinstance(item, dict):
            for name, child in item.items():
                if "\x00" in name:
                    return True
                pending.append(child)
    return False


def _cut_free_text(text: str) -> str:
    encoded = text.encode("utf-8", "surrogatepass")
    if len(encoded) <= FREE_TEXT_BYTES:
        return text
    return encoded[:FREE_TEXT_BYTES].decode("utf-8", "ignore")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _position_number(position: str) -> int | None:
    if not position or (not position.isdigit() and not (position.startswith("-") and position[1:].isdigit())):
        return None
    with contextlib.suppress(ValueError):
        number = int(position, 10)
        if str(number) == position and -(2**63) <= number <= 2**63 - 1:
            return number
    return None


def _ms(seconds: float) -> int:
    try:
        return int(seconds * 1000)
    except (OverflowError, ValueError):
        return 2**63 - 1


def _datetime_from_ms(milliseconds: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=milliseconds)


def _ms_value(value: Any) -> int:
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        delta = moment - datetime(1970, 1, 1, tzinfo=UTC)
        return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000
    if isinstance(value, int):
        return value
    raise MessageStoreFailed(f"the SQL message store cannot read a time: {value!r}")


def _affected(status: str) -> int:
    try:
        return max(0, int(status.rsplit(" ", 1)[-1]))
    except (AttributeError, ValueError):
        return 0


def _failure(error: BaseException) -> BaseException:
    if isinstance(error, MessageStoreUnsupported | InvalidMessage | Conflict | MessageStoreFailed):
        return error
    if isinstance(error, asyncpg.PostgresError):
        code = getattr(error, "sqlstate", None)
        if code in ("23505", "40001", "40P01", "55P03"):
            return Conflict(str(error))
        return MessageStoreFailed(f"the PostgreSQL message store: {error}")
    args = getattr(error, "args", ())
    if args and args[0] in (1062, 1586, 1213, 1205, 3572):
        return Conflict(str(error))
    return MessageStoreFailed(f"the SQL message store: {error}")


def _duplicate(error: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, asyncpg.PostgresError):
            return getattr(current, "sqlstate", None) == "23505"
        args = getattr(current, "args", ())
        if args and args[0] in (1062, 1586):
            return True
        current = current.__cause__
    return False


def _missing_table(error: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, asyncpg.PostgresError):
            return getattr(current, "sqlstate", None) == "42P01"
        args = getattr(current, "args", ())
        if args and args[0] == 1146:
            return True
        current = current.__cause__
    return False


def _check_postgres_server(product: str, num: str, version: str) -> None:
    """Fails unless the server is PostgreSQL itself, 13 or later, naming what it is."""
    if not product.startswith("PostgreSQL"):
        raise MessageStoreFailed(
            f"the SQL message store supports PostgreSQL 13 or later, MySQL 8.0.17 or later; this server is {product}"
        )
    try:
        number = int(num.strip())
    except ValueError as error:
        raise MessageStoreFailed(f"the SQL message store cannot read the PostgreSQL version number: {num!r}") from error
    if number < 130_000:
        raise MessageStoreFailed(
            f"the SQL message store needs PostgreSQL 13.0 or later; this server is PostgreSQL {version}"
        )


def _check_mysql_server(version: str, mode: str) -> None:
    """Fails unless the server is MySQL 8.0.17 or later (not MariaDB) with a strict sql_mode."""
    if "mariadb" in version.lower():
        raise MessageStoreFailed(
            f"the SQL message store does not support MariaDB yet (MySQL {version}): it supports PostgreSQL 13 or "
            "later, MySQL 8.0.17 or later"
        )
    if _version_numbers(version) < (8, 0, 17):
        raise MessageStoreFailed(f"the SQL message store needs MySQL 8.0.17 or later; this server is MySQL {version}")
    if "STRICT_TRANS_TABLES" not in mode and "STRICT_ALL_TABLES" not in mode:
        raise MessageStoreFailed(
            "the SQL message store needs a strict sql_mode on MySQL (STRICT_TRANS_TABLES or STRICT_ALL_TABLES), so "
            f"a value too long is refused rather than truncated; this session's sql_mode is {mode!r}"
        )


def _version_numbers(version: str) -> tuple[int, int, int]:
    numbers: list[int] = []
    current: int | None = None
    for char in version:
        if char.isdigit():
            current = (current or 0) * 10 + int(char)
        elif current is not None:
            numbers.append(current)
            current = None
        if len(numbers) == 3:
            break
    if current is not None and len(numbers) < 3:
        numbers.append(current)
    while len(numbers) < 3:
        numbers.append(0)
    return numbers[0], numbers[1], numbers[2]


__all__ = [
    "DRIVER_MYSQL",
    "DRIVER_POSTGRES",
    "NAME_MYSQL",
    "NAME_POSTGRES",
    "SCHEMA_VERSION",
    "SqlConfig",
    "SqlStore",
    "StoreLifecycle",
    "split_sql",
]
