"""The settings and the names a run shares with the driver."""

from __future__ import annotations

import pytest
from store_interop.settings import (
    FAMILIES,
    MYSQL,
    POSTGRES,
    Settings,
    database_name,
    instance_name,
    marker_id,
    source_name,
    subscription_name,
)


def base_env() -> dict[str, str]:
    return {
        "INTEROP_RUN": "run42",
        "INTEROP_LANGUAGE": "python",
        "INTEROP_KAFKA_BOOTSTRAP": "kafka:9092",
        "INTEROP_RABBITMQ_HOST": "rabbitmq",
        "INTEROP_RABBITMQ_PORT": "5552",
        "INTEROP_POSTGRES_URL": f"postgresql://snodra:snodra@postgres:5432/{database_name('run42')}",
        "INTEROP_MYSQL_URL": f"mysql://snodra:snodra@mysql:3306/{database_name('run42')}",
        "INTEROP_MONGO_URI": "mongodb://mongo:27017/?replicaSet=rs0",
        "INTEROP_MONGO_DATABASE": database_name("run42"),
    }


def test_settings_from_env() -> None:
    settings = Settings.from_env(base_env())
    assert settings.run == "run42"
    assert settings.language == "python"
    assert settings.hub == "hub:7000"
    assert settings.families == tuple(FAMILIES)
    assert settings.kafka_bootstrap == "kafka:9092"
    assert settings.rabbitmq_port == 5552
    assert settings.sql_database == "snodra_interop_run42"
    assert settings.mongo_database == "snodra_interop_run42"
    assert settings.status_interval == 5.0


def test_the_status_interval_is_read_from_the_library_setting() -> None:
    values = base_env()
    values["SNODRA_SERVER_STATUS_INTERVAL_MS"] = "4000"
    assert Settings.from_env(values).status_interval == 4.0
    values["SNODRA_SERVER_STATUS_INTERVAL_MS"] = "0"
    with pytest.raises(ValueError, match="SNODRA_SERVER_STATUS_INTERVAL_MS"):
        Settings.from_env(values)


def test_names_are_per_run() -> None:
    assert source_name("run42", POSTGRES) == "snodra.interop.run42.postgres"
    assert instance_name("python", POSTGRES) == "python-postgres"
    assert subscription_name("kafka") == "store-interop.interop-kafka"
    assert marker_id(MYSQL, "e-1") == "processor:interop-mysql:e-1"


def test_shared_family_endpoints_are_required() -> None:
    values = base_env()
    del values["INTEROP_MYSQL_URL"]
    with pytest.raises(ValueError, match="INTEROP_MYSQL_URL"):
        Settings.from_env(values)


def test_an_unknown_family_is_refused() -> None:
    values = base_env()
    values["INTEROP_FAMILIES"] = "kafka,firestore"
    with pytest.raises(ValueError, match="firestore"):
        Settings.from_env(values)


def test_an_unsafe_run_is_refused() -> None:
    for run in ("", "Run-42", "run.42", "run 42"):
        values = base_env()
        values["INTEROP_RUN"] = run
        with pytest.raises(ValueError, match="INTEROP_RUN"):
            Settings.from_env(values)


def test_families_are_unique_and_ordered() -> None:
    values = base_env()
    values["INTEROP_FAMILIES"] = "mongo,kafka,mongo"
    assert Settings.from_env(values).families == ("mongo", "kafka")


def test_the_sql_url_must_name_the_runs_database() -> None:
    values = base_env()
    values["INTEROP_POSTGRES_URL"] = "postgresql://snodra:snodra@postgres:5432/another_database"
    with pytest.raises(ValueError, match="another_database"):
        Settings.from_env(values)
