"""The shared SQL layout: the published bytes, the schema lock, the server checks and the splitter."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from importlib.resources import files
from pathlib import Path
from typing import Any

import pytest
from snodra import MessageStoreFailed, MessageStoreUnsupported
from snodra_sql import (
    SCHEMA_VERSION,
    SqlConfig,
    SqlStore,
    split_sql,
)
from snodra_sql.sql import (
    PUBLISHED_MYSQL_SHA256,
    PUBLISHED_POSTGRES_SHA256,
    SCHEMA_LOCK_MYSQL,
    SCHEMA_LOCK_POSTGRES,
    SCHEMA_MYSQL_PATH,
    SCHEMA_POSTGRES_PATH,
    _check_mysql_server,
    _check_postgres_server,
)

SOURCE = Path(__file__).resolve().parents[1] / "src" / "snodra_sql"
MakeConfig = Callable[..., SqlConfig]


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_the_schema_files_are_the_published_ones() -> None:
    """The files are the shared layout's reference files, byte for byte; a change is a specification change."""
    for path, published in (
        (SCHEMA_POSTGRES_PATH, PUBLISHED_POSTGRES_SHA256),
        (SCHEMA_MYSQL_PATH, PUBLISHED_MYSQL_SHA256),
    ):
        on_disk = (SOURCE / path).read_bytes()
        assert _sha256(on_disk) == published, f"{path}: SHA-256 differs from the published one"
        assert files("snodra_sql").joinpath(path).read_bytes() == on_disk, f"{path}: the embedded bytes differ"
        assert not on_disk.startswith(b"\xef\xbb\xbf"), f"{path} starts with a byte order mark"
        assert b"\r\n" not in on_disk, f"{path} holds CRLF line endings"
        assert on_disk.endswith(b"\n"), f"{path} does not end with a newline"


def test_the_schema_lock_is_the_shared_one() -> None:
    assert SCHEMA_VERSION == 1
    assert SCHEMA_LOCK_POSTGRES == 6_002_810_473_706_375_245
    assert SCHEMA_LOCK_MYSQL == "snodra_schema"


def test_every_statement_is_found_and_the_version_is_recorded_last() -> None:
    for path in (SCHEMA_POSTGRES_PATH, SCHEMA_MYSQL_PATH):
        script = (SOURCE / path).read_text(encoding="utf-8")
        statements = split_sql(script)
        assert len(statements) >= 13, f"{path}: only {len(statements)} statements"
        for index, statement in enumerate(statements):
            assert statement.strip(), f"{path}: statement {index} is empty"
            assert "--" not in statement, f"{path}: statement {index} keeps a comment"
            assert "/*" not in statement, f"{path}: statement {index} keeps a comment"
            assert ";" not in statement, f"{path}: statement {index} keeps a semicolon"
        assert statements[-1].startswith("INSERT INTO snodra_schema (version) VALUES (1)"), path


def test_split_sql_keeps_quoted_semicolons_and_drops_comments() -> None:
    script = (
        "-- a comment; with a semicolon\n"
        "CREATE TABLE \"a;b\" (x TEXT DEFAULT 'y;z'); /* another ; comment */\n"
        "INSERT INTO t VALUES ('it''s; fine');\n"
        "INSERT INTO t VALUES ('a\\'b; still quoted');"
    )
    statements = split_sql(script)
    assert len(statements) == 3
    assert '"a;b"' in statements[0]
    assert "'y;z'" in statements[0]
    assert "'it''s; fine'" in statements[1]
    assert "'a\\'b; still quoted'" in statements[2]
    assert len(split_sql("INSERT INTO t VALUES ('-- x; y'); SELECT 1;")) == 2


def test_the_driver_is_read_from_the_url() -> None:
    assert SqlConfig(url="postgres://u@h/db").resolved_driver == "postgres"
    assert SqlConfig(url="postgresql://u@h/db").resolved_driver == "postgres"
    assert SqlConfig(url="mysql://u@h/db").resolved_driver == "mysql"
    assert SqlConfig(url="postgres://u@h/db", driver="mysql").resolved_driver == "mysql"


def test_a_config_needs_a_url_a_pool_of_two_and_a_known_driver() -> None:
    with pytest.raises(ValueError, match="database URL"):
        SqlConfig(url="")
    with pytest.raises(ValueError, match="pool of at least 2"):
        SqlConfig(url="postgres://u@h/db", max_size=1)
    with pytest.raises(ValueError, match="unknown driver"):
        SqlConfig(url="postgres://u@h/db", driver="sqlite")
    with pytest.raises(ValueError, match="cannot tell the database"):
        SqlConfig(url="sqlite:///tmp/db")
    with pytest.raises(ValueError, match="non-negative outbox_retention"):
        SqlConfig(url="postgres://u@h/db", outbox_retention=-1)


def test_from_env_reads_the_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNODRA_STORE_SQL_URL", "mysql://u:p@h:3306/db")
    monkeypatch.setenv("SNODRA_STORE_SQL_DRIVER", "mysql")
    monkeypatch.setenv("SNODRA_STORE_SQL_NOTIFY", "false")
    monkeypatch.setenv("SNODRA_STORE_SQL_PURGE_INTERVAL_MS", "0")
    config = SqlConfig.from_env()
    assert config.url == "mysql://u:p@h:3306/db"
    assert config.resolved_driver == "mysql"
    assert not config.notify
    assert config.purge_interval == -1.0


def test_the_store_offers_the_lifecycle() -> None:
    store = SqlStore(SqlConfig(url="postgres://u@h/db"))
    for name in ("use_clock", "init_schema", "check_schema", "background"):
        assert callable(getattr(store, name)), name
    store.use_clock(lambda: 1_800_000_000_000)
    assert store._now() == 1_800_000_000_000


async def test_the_unimplemented_ports_name_the_level() -> None:
    store = SqlStore(SqlConfig(url="postgres://u@h/db"))
    with pytest.raises(MessageStoreUnsupported, match="level 2"):
        await store.dead_letters.get("letter")
    with pytest.raises(MessageStoreUnsupported, match="level 2"):
        await store.control.get("default")
    with pytest.raises(MessageStoreUnsupported, match="level 2"):
        await store.external.find("key")
    with pytest.raises(MessageStoreUnsupported, match="level 2"):
        await store.sagas.sagas.find_by_id("id")


@pytest.mark.timeout(300)
async def test_an_empty_database_fails_the_schema_check(database: str, make_config: MakeConfig) -> None:
    store = SqlStore(make_config())
    try:
        with pytest.raises(MessageStoreFailed, match="schema is at version 0") as refused:
            await store.check_schema()
        assert "V1__snodra.sql" in str(refused.value)
    finally:
        await store.close()


@pytest.mark.timeout(300)
async def test_two_startups_apply_the_schema_once(server: Any, database: str, make_config: MakeConfig) -> None:
    first = SqlStore(make_config())
    second = SqlStore(make_config())
    try:
        await asyncio.gather(first.init_schema(), second.init_schema())
        async with first._read() as conn:
            rows = await first._fetchval(conn, "SELECT COUNT(*) AS n FROM snodra_schema")
        assert rows == 1, "the schema's version row was written once"
        await first.check_schema()
        await second.check_schema()
        await second.init_schema()
    finally:
        await first.close()
        await second.close()


@pytest.mark.timeout(300)
async def test_a_startup_waits_for_the_schema_lock(server: Any, database: str, make_config: MakeConfig) -> None:
    store = SqlStore(make_config())
    holder = await server.connect(database)
    try:
        await store.open()
        if store.name == "postgres":
            await holder.fetchval("SELECT pg_advisory_lock($1)", SCHEMA_LOCK_POSTGRES)
        else:
            async with holder.cursor() as cursor:
                await cursor.execute("SELECT GET_LOCK(%s, 0)", (SCHEMA_LOCK_MYSQL,))
                got = (await cursor.fetchone())[0]
            assert got == 1
        task = asyncio.create_task(store.init_schema())
        await asyncio.sleep(0.5)
        assert not task.done(), "the startup did not wait for the lock"
        if store.name == "postgres":
            await holder.fetchval("SELECT pg_advisory_unlock($1)", SCHEMA_LOCK_POSTGRES)
        else:
            async with holder.cursor() as cursor:
                await cursor.execute("SELECT RELEASE_LOCK(%s)", (SCHEMA_LOCK_MYSQL,))
                await cursor.fetchone()
        async with asyncio.timeout(30):
            await task
        await store.check_schema()
    finally:
        await server.close(holder)
        await store.close()


def test_the_server_checks_refuse_unsupported_servers() -> None:
    """PostgreSQL 13 or later, MySQL 8.0.17 or later (not MariaDB) with a strict sql_mode."""
    _check_postgres_server("PostgreSQL 17.7 (Debian 17.7-3)", "170007", "17.7")
    _check_postgres_server("PostgreSQL 13.0 on x86_64", "130000", "13.0")
    with pytest.raises(MessageStoreFailed, match=r"needs PostgreSQL 13\.0"):
        _check_postgres_server("PostgreSQL 12.18 on x86_64", "120018", "12.18")
    with pytest.raises(MessageStoreFailed, match="this server is CockroachDB"):
        _check_postgres_server("CockroachDB CCL v24.1.0", "130000", "13.0")
    with pytest.raises(MessageStoreFailed, match="cannot read the PostgreSQL version"):
        _check_postgres_server("PostgreSQL 17.7", "not-a-number", "17.7")

    strict = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
    _check_mysql_server("8.4.7", strict)
    _check_mysql_server("8.0.17", "STRICT_ALL_TABLES")
    _check_mysql_server("9.1.0-commercial", strict)
    with pytest.raises(MessageStoreFailed, match=r"needs MySQL 8\.0\.17"):
        _check_mysql_server("8.0.16", strict)
    with pytest.raises(MessageStoreFailed, match=r"needs MySQL 8\.0\.17"):
        _check_mysql_server("5.7.44-log", strict)
    with pytest.raises(MessageStoreFailed, match="does not support MariaDB"):
        _check_mysql_server("10.11.6-MariaDB", strict)
    with pytest.raises(MessageStoreFailed, match="strict sql_mode"):
        _check_mysql_server("8.4.7", "NO_ENGINE_SUBSTITUTION")


@pytest.mark.timeout(300)
async def test_a_partial_mysql_schema_is_completed_by_a_retry(
    server: Any, database: str, make_config: MakeConfig
) -> None:
    """SG-33: MySQL commits DDL statement by statement; a retry of init_schema() finishes the idempotent file."""
    store = SqlStore(make_config())
    if store.name != "mysql":
        await store.close()
        pytest.skip("SG-33 is MySQL's statement-by-statement DDL")
    try:
        await store.open()
        statements = split_sql((SOURCE / SCHEMA_MYSQL_PATH).read_text(encoding="utf-8"))
        async with store._read() as conn:
            for statement in statements[:2]:
                await store._exec(conn, statement)
        async with store._read() as conn:
            assert await store._fetchval(conn, "SELECT COUNT(*) AS n FROM snodra_schema") == 0
        await store.init_schema()
        await store.check_schema()
        async with store._read() as conn:
            assert await store._fetchval(conn, "SELECT COUNT(*) AS n FROM snodra_schema") == 1
    finally:
        await store.close()
