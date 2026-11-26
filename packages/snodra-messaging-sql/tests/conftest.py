"""One PostgreSQL and one MySQL server for the add-on's tests, and one fresh database per test.

The images are pinned by digest, every database name carries a fresh random suffix and is dropped
when its test ends, and Docker is required (without it the tests skip). The host port forwarding can
lead the server's own ready line, so a server is only handed out once it answers a query.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import asyncmy
import asyncpg
import pytest
from snodra_sql import SqlConfig, SqlStore
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

POSTGRES_IMAGE = "postgres:17.7@sha256:2006493727bd5277eece187319af1ef82b4cf82cf4fc1ed00da0775b646ac2a4"
"""PostgreSQL 17.7, the same digest the other Snodra SQL add-ons pin."""

MYSQL_IMAGE = "mysql:8.4.7@sha256:0426ec38c7a10aa45ba383887df7878f74ee70e2fd589c7b69207f3577901903"
"""MySQL 8.4.7, the same digest the other Snodra SQL add-ons pin."""

POSTGRES_PORT = 5432
MYSQL_PORT = 3306
ADMIN_DATABASE = "snodra_admin"


@dataclass(slots=True)
class Server:
    """One running database server: where it listens and the database it administers."""

    driver: str
    host: str
    port: int
    container: Any = None

    def url(self, database: str) -> str:
        if self.driver == "postgres":
            return f"postgresql://postgres:postgres@{self.host}:{self.port}/{database}"
        return f"mysql://root:snodra@{self.host}:{self.port}/{database}"

    def admin_url(self, database: str) -> str:
        return self.url(database)

    async def connect_admin(self) -> Any:
        return await self.connect(ADMIN_DATABASE)

    async def connect(self, database: str) -> Any:
        """A raw connection to one database on this server (the caller closes it)."""
        last: Exception | None = None
        for _ in range(120):
            try:
                if self.driver == "postgres":
                    conn: Any = await asyncpg.connect(dsn=self.url(database))
                    await conn.fetchval("SELECT 1")
                    return conn
                conn = await asyncmy.connect(
                    host=self.host,
                    port=self.port,
                    user="root",
                    password="snodra",
                    db=database,
                    charset="utf8mb4",
                    autocommit=True,
                )
                await conn.query("SELECT 1")
                return conn
            except Exception as error:
                last = error
                await asyncio.sleep(1)
        raise AssertionError(f"the test database does not answer: {last}")

    async def close(self, conn: Any) -> None:
        if self.driver == "postgres":
            await conn.close()
        else:
            await conn.ensure_closed()

    async def create_database(self) -> str:
        name = f"snodra_test_{uuid.uuid4().hex}"
        conn = await self.connect_admin()
        try:
            if self.driver == "postgres":
                await conn.execute(f'CREATE DATABASE "{name}"')
            else:
                await conn.query(f"CREATE DATABASE `{name}`")
        finally:
            await self.close(conn)
        return name

    async def drop_database(self, name: str) -> None:
        conn = await self.connect_admin()
        try:
            if self.driver == "postgres":
                await conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            else:
                await conn.query(f"DROP DATABASE IF EXISTS `{name}`")
        finally:
            await self.close(conn)

    async def answers(self) -> bool:
        try:
            conn = await self.connect_admin()
        except Exception:
            return False
        try:
            if self.driver == "postgres":
                await conn.fetchval("SELECT 1")
            else:
                await conn.query("SELECT 1")
            return True
        except Exception:
            return False
        finally:
            with contextlib.suppress(Exception):
                await self.close(conn)


def _start(image: str, port: int, env: dict[str, str], ready_line: str) -> Server:
    last: Exception | None = None
    for _ in range(3):
        container = (
            DockerContainer(image)
            .with_exposed_ports(port)
            .with_envs(**env)
            .waiting_for(LogMessageWaitStrategy(ready_line).with_startup_timeout(180))
        )
        try:
            container.start()
        except Exception as error:  # the image or Docker itself is missing
            last = error
            with contextlib.suppress(Exception):
                container.stop()
            continue
        server = Server(
            driver="postgres" if port == POSTGRES_PORT else "mysql",
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(port)),
            container=container,
        )
        return server
    pytest.skip(f"the SQL tests need Docker: {last}")


@pytest.fixture(scope="session")
def postgres_server() -> Iterator[Server]:
    """The run's PostgreSQL container, started once."""
    server = _start(
        POSTGRES_IMAGE,
        POSTGRES_PORT,
        {"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": ADMIN_DATABASE},
        "database system is ready to accept connections",
    )
    try:
        yield server
    finally:
        if server.container is not None:
            server.container.stop()


@pytest.fixture(scope="session")
def mysql_server() -> Iterator[Server]:
    """The run's MySQL container, started once."""
    server = _start(
        MYSQL_IMAGE,
        MYSQL_PORT,
        {"MYSQL_ROOT_PASSWORD": "snodra", "MYSQL_DATABASE": ADMIN_DATABASE},
        "ready for connections",
    )
    try:
        yield server
    finally:
        if server.container is not None:
            server.container.stop()


@pytest.fixture(params=["postgres", "mysql"])
def server(request: pytest.FixtureRequest) -> Server:
    """The test runs once on PostgreSQL and once on MySQL."""
    return request.getfixturevalue(f"{request.param}_server")  # type: ignore[no-any-return]


@pytest.fixture
async def database(server: Server) -> AsyncIterator[str]:
    """A fresh, uniquely named database, dropped when the test ends."""
    name = await server.create_database()
    try:
        yield name
    finally:
        await server.drop_database(name)


@pytest.fixture
def make_config(server: Server, database: str) -> Any:
    """A config factory over the test's database; the purge is off unless a test turns it on."""

    def make(**overrides: Any) -> SqlConfig:
        settings: dict[str, Any] = {
            "url": server.url(database),
            "max_size": 5,
            "purge_interval": -1.0,
        }
        settings.update(overrides)
        return SqlConfig(**settings)

    return make


@pytest.fixture
async def store(make_config: Any) -> AsyncIterator[SqlStore]:
    """A store over a fresh database, its schema applied."""
    store = SqlStore(make_config())
    try:
        await store.open()
        await store.init_schema()
        yield store
    finally:
        await store.close()
