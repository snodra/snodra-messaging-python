"""The SQL message-store add-on: the core's message-store ports on PostgreSQL and MySQL.

Hand :class:`SqlStore` to :class:`snodra.App` as its ``store``; a service that only sends commands and
queries needs none. The store keeps every message-store port in the service's own database: the outbox,
the processed-event markers, the dead letters, the processor control state, the external-call records and
the saga records, on the shared SQL layout version 1 that every Snodra library reads and writes.

See :mod:`snodra_sql.sql` for the layout and the guarantees, ``DRIVER-SPIKE.md`` for the driver
behaviour the add-on relies on, and ``SPEC-GAPS.md`` for the choices the public specification leaves
open.
"""

from snodra import StoreLifecycle

from snodra_sql.sql import (
    DRIVER_MYSQL,
    DRIVER_POSTGRES,
    NAME_MYSQL,
    NAME_POSTGRES,
    SCHEMA_VERSION,
    SqlConfig,
    SqlStore,
    split_sql,
)

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
