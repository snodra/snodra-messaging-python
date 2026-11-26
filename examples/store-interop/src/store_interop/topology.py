"""Creates the RabbitMQ Streams super stream a stores run reads.

The libraries never create streams: the infrastructure does. The harness's compose file runs this
module as a one-shot init container between the broker's health check and the probes, using the
stream client the RabbitMQ add-on already depends on. It is idempotent for one run token.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import suppress

from rstream.client import Client
from rstream.exceptions import StreamAlreadyExists, StreamDoesNotExist

from store_interop.settings import RABBITMQ, source_name

PARTITIONS = 2
"""The partitions of a stores run: one per language, so a single-active consumer of each language
is active."""

log = logging.getLogger("store_interop.topology")


async def _partitions(client: Client, source: str) -> list[str]:
    try:
        return list(await client.partitions(source))
    except StreamDoesNotExist:
        return []


async def create_super_stream(
    source: str,
    *,
    host: str,
    port: int,
    username: str,
    password: str,
) -> None:
    """Create the two-partition super stream ``source``, unless it is there already."""
    client = Client(host, port, connection_name="store-interop-topology", frame_max=1 << 20, heartbeat=30)
    await client.start()
    try:
        await client.authenticate("/", username, password)
        existing = await _partitions(client, source)
        if len(existing) == PARTITIONS:
            log.info("the super stream %s is there with %d partitions", source, len(existing))
            return
        if existing:
            log.warning("the super stream %s has %d partitions; recreating it", source, len(existing))
            with suppress(StreamDoesNotExist):
                await client.delete_super_stream(source)
        try:
            await client.create_super_stream(
                source,
                [f"{source}-{index}" for index in range(PARTITIONS)],
                [str(index) for index in range(PARTITIONS)],
            )
        except StreamAlreadyExists:
            log.info("the super stream %s was created by another instance", source)
        log.info("created the super stream %s with %d partitions", source, PARTITIONS)
    finally:
        await client.close()


async def main() -> int:
    run = os.environ.get("INTEROP_RUN", "").strip()
    if not run:
        log.error("INTEROP_RUN is required")
        return 2
    source = os.environ.get("INTEROP_RABBITMQ_SOURCE", "").strip() or source_name(run, RABBITMQ)
    host = os.environ.get("INTEROP_RABBITMQ_HOST", "rabbitmq").strip() or "rabbitmq"
    port = int(os.environ.get("INTEROP_RABBITMQ_PORT", "5552"))
    username = os.environ.get("INTEROP_RABBITMQ_USERNAME", "guest")
    password = os.environ.get("INTEROP_RABBITMQ_PASSWORD", "guest")
    last: Exception | None = None
    for _ in range(60):
        try:
            await create_super_stream(source, host=host, port=port, username=username, password=password)
            return 0
        except Exception as error:  # the broker's stream listener may still be starting
            last = error
            await asyncio.sleep(1.0)
    log.error("the super stream %s could not be created: %s", source, last)
    return 1


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    raise SystemExit(asyncio.run(main()))
