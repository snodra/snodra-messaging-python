"""What the probe reads from the environment, and the names a run shares with the driver."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

SERVICE = "store-interop"
"""The service name every probe registers: one logical service, several instances."""

EVENT_TYPE = "orders.OrderPlaced"
"""The event type the outbox and the raw publish commands carry."""

KAFKA = "kafka"
RABBITMQ = "rabbitmq"
POSTGRES = "postgres"
MYSQL = "mysql"
MONGO = "mongo"


@dataclass(frozen=True, slots=True)
class Family:
    """One store family: which add-ons a probe of the family runs, and whether both languages share
    the message store."""

    name: str
    message_store: str
    event_store: str
    shared: bool


FAMILIES: dict[str, Family] = {
    KAFKA: Family(name=KAFKA, message_store="memory", event_store="kafka", shared=False),
    RABBITMQ: Family(name=RABBITMQ, message_store="memory", event_store="rabbitmq-streams", shared=False),
    POSTGRES: Family(name=POSTGRES, message_store="postgres", event_store="kafka", shared=True),
    MYSQL: Family(name=MYSQL, message_store="mysql", event_store="kafka", shared=True),
    MONGO: Family(name=MONGO, message_store="mongo", event_store="kafka", shared=True),
}
"""The families the profile runs."""

FAMILY_ORDER: tuple[str, ...] = (KAFKA, RABBITMQ, POSTGRES, MYSQL, MONGO)
"""The order the families run in, and the default when ``INTEROP_FAMILIES`` is unset."""

_RUN = re.compile(r"^[a-z0-9]+$")
_LANGUAGE = re.compile(r"^[a-z][a-z0-9]*$")


def database_name(run: str) -> str:
    """The one database both languages share for a run."""
    return f"snodra_interop_{run}"


def source_name(run: str, family: str) -> str:
    """The event store source (a Kafka topic, a RabbitMQ super stream) of one family."""
    return f"snodra.interop.{run}.{family}"


def instance_name(language: str, family: str) -> str:
    """The hub instance id of one language in one family."""
    return f"{language}-{family}"


def processor_name(family: str) -> str:
    """The processor (and so the subscription) of one family: both languages use it, so their
    consumers compete in the same group or under the same stream consumer name."""
    return f"interop-{family}"


def subscription_name(family: str) -> str:
    """The event store subscription (``<service>.<processor>``) of one family."""
    return f"{SERVICE}.{processor_name(family)}"


def marker_id(family: str, event_id: str) -> str:
    """The stage-1 processed-event marker the library records for an event."""
    return f"processor:{processor_name(family)}:{event_id}"


def _database_from_url(raw: str) -> str:
    path = urlsplit(raw).path.lstrip("/")
    if not path:
        raise ValueError(f"the database URL {raw!r} names no database")
    return path


@dataclass(frozen=True, slots=True)
class Settings:
    """What the probe reads from the environment."""

    language: str
    run: str
    hub: str
    families: tuple[str, ...]
    kafka_bootstrap: str
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_username: str
    rabbitmq_password: str
    postgres_url: str
    mysql_url: str
    mongo_uri: str
    mongo_database: str
    sql_database: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str]) -> Settings:
        """The settings of this process. ``INTEROP_RUN``, ``INTEROP_LANGUAGE`` and the endpoints of
        the families it runs are required; a blank value is not configured."""

        def get(name: str, fallback: str = "") -> str:
            value = environ.get(name, "").strip()
            return value or fallback

        run = get("INTEROP_RUN")
        if not run:
            raise ValueError("INTEROP_RUN is required (the unique token of the run)")
        if not _RUN.match(run):
            raise ValueError(f"INTEROP_RUN {run!r} must be lowercase letters and digits only")
        language = get("INTEROP_LANGUAGE")
        if not language:
            raise ValueError("INTEROP_LANGUAGE is required")
        if not _LANGUAGE.match(language):
            raise ValueError(
                f"INTEROP_LANGUAGE {language!r} must be lowercase letters and digits, starting with a letter"
            )
        names = get("INTEROP_FAMILIES", ",".join(FAMILY_ORDER))
        families: list[str] = []
        for name in names.split(","):
            name = name.strip()
            if not name or name in families:
                continue
            if name not in FAMILIES:
                raise ValueError(
                    f"INTEROP_FAMILIES names the unknown family {name!r} (the families are {', '.join(FAMILY_ORDER)})"
                )
            families.append(name)
        if not families:
            raise ValueError("INTEROP_FAMILIES names no family")
        port = int(get("INTEROP_RABBITMQ_PORT", "5552"))
        if not 1 <= port <= 65535:
            raise ValueError(f"INTEROP_RABBITMQ_PORT {port!r} is not a port")
        postgres_url = get("INTEROP_POSTGRES_URL")
        mysql_url = get("INTEROP_MYSQL_URL")
        mongo_uri = get("INTEROP_MONGO_URI")
        if POSTGRES in families and not postgres_url:
            raise ValueError("INTEROP_POSTGRES_URL is required")
        if MYSQL in families and not mysql_url:
            raise ValueError("INTEROP_MYSQL_URL is required")
        if MONGO in families and not mongo_uri:
            raise ValueError("INTEROP_MONGO_URI is required")
        sql_database = database_name(run)
        for family, url in ((POSTGRES, postgres_url), (MYSQL, mysql_url)):
            if family in families and _database_from_url(url) != sql_database:
                raise ValueError(
                    f"the {family} URL names {_database_from_url(url)!r}, not the run's database {sql_database!r}"
                )
        return cls(
            language=language,
            run=run,
            hub=get("INTEROP_HUB", "hub:7000"),
            families=tuple(families),
            kafka_bootstrap=get("INTEROP_KAFKA_BOOTSTRAP", "kafka:9092"),
            rabbitmq_host=get("INTEROP_RABBITMQ_HOST", "rabbitmq"),
            rabbitmq_port=port,
            rabbitmq_username=get("INTEROP_RABBITMQ_USERNAME", "guest"),
            rabbitmq_password=get("INTEROP_RABBITMQ_PASSWORD", "guest"),
            postgres_url=postgres_url,
            mysql_url=mysql_url,
            mongo_uri=mongo_uri,
            mongo_database=get("INTEROP_MONGO_DATABASE", database_name(run)),
            sql_database=sql_database,
        )
