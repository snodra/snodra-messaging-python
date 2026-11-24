import asyncio
import os
import signal
import sys
import time
from pathlib import Path

from fakehub import FakeHub, ack


async def test_stops_within_five_seconds_of_sigterm(tmp_path: Path) -> None:
    hub = await FakeHub.start()
    env = os.environ | {
        "CONFORMANCE_SERVICE_NAME": "conformance-python",
        "CONFORMANCE_INSTANCE_ID": "i-1",
        "CONFORMANCE_HUBS": hub.address,
    }
    log = tmp_path / "service.log"
    with log.open("wb") as out:
        service = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "conformance_service", env=env, stdout=out, stderr=out
        )
    try:
        s = await hub.next_stream(within=30.0)
        register = await s.recv_register()
        assert (register.service_name, register.instance_id, register.initial_permits) == (
            "conformance-python",
            "i-1",
            64,
        )
        await s.ack(ack("hub-1"))
        sent = time.monotonic()
        service.send_signal(signal.SIGTERM)
        assert await asyncio.wait_for(service.wait(), 5.0) == 0, log.read_text()
        assert time.monotonic() - sent < 5.0
        assert not (await s.recv_register()).command_types  # it left the hub: the empty Register first
        await s.half_closed(within=1.0)
    finally:
        if service.returncode is None:
            service.kill()
            await service.wait()
        await hub.stop()
