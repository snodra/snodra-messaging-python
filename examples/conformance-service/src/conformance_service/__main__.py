"""Runs the conformance service: registers on every hub, stops on SIGTERM or SIGINT within the runner's 5 s."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from snodra import App

from conformance_service.env import settings_from_env
from conformance_service.handlers import register_handlers

SHUTDOWN_BUDGET = 4.0
"""Seconds the shutdown may take: inside the runner's 5 s after SIGTERM."""


async def main() -> None:
    settings = settings_from_env(os.environ)
    app = App(settings)
    register_handlers(app, settings)
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await app.start()
    try:
        await stop.wait()
    finally:
        await app.shutdown(timeout=SHUTDOWN_BUDGET)


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stderr, level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    logging.getLogger("grpc").setLevel(logging.WARNING)
    asyncio.run(main())
