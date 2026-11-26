"""One MongoDB replica set for the add-on's tests, and one fresh database per test.

The image is pinned by digest, every database name carries a fresh random suffix and is dropped when its test ends,
and Docker is required (without it the tests skip). A standalone MongoDB has no transactions, so the container runs a
single-node replica set; the client connects to the mapped port directly, and the replica set's own member address
stays the container's ``localhost``.
"""

from __future__ import annotations

import contextlib
import re
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from snodra_mongo import MongoConfig, MongoStore
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

MONGO_IMAGE = "mongo:8.0.15@sha256:f4d54619262ae3bc6a0a8efbebcef970b87b8ad70697479a75ce308a6f400158"
"""MongoDB 8.0.15, the same digest the other Snodra MongoDB add-ons pin."""

MONGO_PORT = 27017
REPLICA_SET = "rs0"
START_ATTEMPTS = 3
PRIMARY_WAIT = 60.0


@dataclass(slots=True)
class Server:
    """One running single-node replica set: where it listens and how to reach it."""

    host: str
    port: int
    container: Any = None

    def uri(self) -> str:
        return f"mongodb://{self.host}:{self.port}/?directConnection=true"

    def client(self) -> MongoClient[Any]:
        """A synchronous client for the test harness (the caller closes it)."""
        return MongoClient(self.uri(), serverSelectionTimeoutMS=5_000, directConnection=True)

    def database(self) -> str:
        return f"snodra_test_{uuid.uuid4().hex}"

    def drop_database(self, name: str) -> None:
        client = self.client()
        try:
            client.drop_database(name)
        finally:
            client.close()


def _start() -> Server:
    last: Exception | None = None
    for _ in range(START_ATTEMPTS):
        container = (
            DockerContainer(MONGO_IMAGE)
            .with_exposed_ports(MONGO_PORT)
            .with_command(["mongod", "--replSet", REPLICA_SET, "--bind_ip_all"])
            .waiting_for(
                LogMessageWaitStrategy(re.compile("waiting for connections", re.IGNORECASE)).with_startup_timeout(180)
            )
        )
        try:
            container.start()
        except Exception as error:  # the image or Docker itself is missing
            last = error
            with contextlib.suppress(Exception):
                container.stop()
            continue
        server = Server(
            host=container.get_container_host_ip(),
            port=int(container.get_exposed_port(MONGO_PORT)),
            container=container,
        )
        try:
            _initiate(server)
        except Exception as error:
            last = error
            with contextlib.suppress(Exception):
                container.stop()
            continue
        return server
    pytest.skip(f"the MongoDB tests need Docker: {last}")


def _initiate(server: Server) -> None:
    """Starts the replica set (idempotently) and waits until it is the writable primary."""
    client = server.client()
    try:
        with contextlib.suppress(PyMongoError):
            client.admin.command(
                "replSetInitiate",
                {"_id": REPLICA_SET, "members": [{"_id": 0, "host": f"localhost:{MONGO_PORT}"}]},
            )
        deadline = time.monotonic() + PRIMARY_WAIT
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                hello = client.admin.command("hello")
                if hello.get("isWritablePrimary"):
                    return
            except PyMongoError as error:
                last = error
            time.sleep(0.2)
        raise AssertionError(f"the replica set never became the writable primary: {last}")
    finally:
        client.close()


@pytest.fixture(scope="session")
def server() -> Iterator[Server]:
    """The run's MongoDB container, started once."""
    started = _start()
    try:
        yield started
    finally:
        if started.container is not None:
            started.container.stop()


@pytest.fixture
def database(server: Server) -> Iterator[str]:
    """A fresh, uniquely named database, dropped when the test ends (after the store is closed)."""
    name = server.database()
    try:
        yield name
    finally:
        server.drop_database(name)


@pytest.fixture
def make_config(server: Server, database: str) -> Any:
    """A config factory over the test's database; the layout's retention defaults unless a test sets others."""

    def make(**overrides: Any) -> MongoConfig:
        settings: dict[str, Any] = {"url": server.uri(), "database": database}
        settings.update(overrides)
        return MongoConfig(**settings)

    return make


@pytest.fixture
async def store(make_config: Any) -> AsyncIterator[MongoStore]:
    """A store over a fresh database, its indexes created."""
    opened = MongoStore(make_config())
    try:
        await opened.init_schema()
        yield opened
    finally:
        await opened.close()
