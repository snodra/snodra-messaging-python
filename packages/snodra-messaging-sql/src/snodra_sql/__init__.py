"""The SQL message-store add-on: the core's message-store ports on PostgreSQL and MySQL.

Hand :class:`SqlStore` to :class:`snodra.App` as its ``store``; a service that only sends commands and
queries needs none. The store keeps the outbox and the processed-event markers in the service's own
database, on the shared SQL layout version 1 that every Snodra library reads and writes; the
dead-letter, processor-control, external-call and saga ports belong to the interface frozen at level 2
and report ``snodra.MessageStoreUnsupported`` until this add-on implements them.

See :mod:`snodra_sql.sql` for the layout and the guarantees, ``DRIVER-SPIKE.md`` for the driver
behaviour the add-on relies on, and ``SPEC-GAPS.md`` for the choices the public specification leaves
open.
"""

from snodra_sql.sql import (
    DRIVER_MYSQL,
    DRIVER_POSTGRES,
    NAME_MYSQL,
    NAME_POSTGRES,
    SCHEMA_VERSION,
    SqlConfig,
    SqlStore,
    StoreLifecycle,
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
