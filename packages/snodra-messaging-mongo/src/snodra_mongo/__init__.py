"""The MongoDB message-store add-on: the core's message-store ports on a replica set."""

from snodra import StoreLifecycle

from snodra_mongo.mongo import (
    COLLECTIONS,
    DEFAULT_DEADLINE_RETENTION,
    DEFAULT_EXTERNAL_CALL_RETENTION,
    DEFAULT_FAILED_RETENTION,
    DEFAULT_OUTBOX_RETENTION,
    DEFAULT_PROCESSED_RETENTION,
    DEFAULT_SAGA_DISPATCH_RETENTION,
    DEFAULT_SAGA_ENDED_RETENTION,
    NAME,
    MongoConfig,
    MongoStore,
)

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
