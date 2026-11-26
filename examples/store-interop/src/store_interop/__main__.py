"""Runs the stores interoperability probes: one app per family, stopping on SIGTERM or SIGINT."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

from store_interop.service import Probe
from store_interop.settings import FAMILIES, Settings

log = logging.getLogger("store_interop")


async def main() -> None:
    settings = Settings.from_env(os.environ)
    probes = [Probe(settings, FAMILIES[name]) for name in settings.families]
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    started: list[Probe] = []
    try:
        for probe in probes:
            await probe.start()
            started.append(probe)
        log.info(
            "the store interoperability probes are registered: language %s, families %s, run %s",
            settings.language,
            ",".join(settings.families),
            settings.run,
        )
        await stop.wait()
    finally:
        for probe in reversed(started):
            try:
                await probe.stop()
            except Exception:
                log.exception("the %s probe did not stop cleanly", probe.family.name)


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(main())
