"""``conformance.Send`` and ``conformance.Outcome``: the service sends a request through the library as an application
does, and reports the outcome in the contract's shape."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Coroutine
from typing import Any

from snodra import (
    App,
    HubError,
    LostParts,
    NeverSent,
    OutcomeUnknown,
    RawJson,
    ReservedHeader,
    RetryPolicy,
)

from conformance_service.messages import Echo, EchoQuery, Missing, Outcome, Probe, ProbeQuery, Send

_UNARY: dict[str, tuple[type[RawJson], bool]] = {
    "probe": (Probe, False),
    "probe-query": (ProbeQuery, True),
    "missing": (Missing, False),
    "echo": (Echo, False),
    "echo-query": (EchoQuery, True),
}
"""The unary targets: the message class and whether it is a query."""

KEPT_OUTCOMES = 1024
"""Finished asynchronous sends whose outcome is kept for ``conformance.Outcome``; older ones are forgotten."""


def classify(exc: BaseException) -> dict[str, Any]:
    """The contract's ``error`` of a failed call, by the library's exception types."""
    out: dict[str, Any] = {"kind": "local", "code": None, "message": str(exc), "details": None}
    if isinstance(exc, ReservedHeader):
        out["kind"] = "refused"
    elif isinstance(exc, NeverSent):
        out["kind"] = "never-sent"
    elif isinstance(exc, OutcomeUnknown):
        out["kind"] = "outcome-unknown"
    elif isinstance(exc, LostParts):
        out["kind"] = "lost-parts"
    elif isinstance(exc, HubError):
        out |= {"kind": "code", "code": exc.code.name, "message": exc.message}
        if exc.details:
            out["details"] = exc.details.decode("utf-8", errors="replace")
    return out


def _unsupported(reason: str) -> dict[str, Any]:
    return {"result": "unsupported", "reason": reason}


class Sends:
    """The service's sends: run at once, or in the background (``async``) and reported by ``conformance.Outcome``."""

    def __init__(self, app: App) -> None:
        self._app = app
        self._ops: dict[str, asyncio.Task[dict[str, Any]]] = {}

    async def send(self, m: Send) -> RawJson:
        if not m.async_:
            return RawJson(await self._run(m))
        op = uuid.uuid4().hex
        self._ops[op] = asyncio.create_task(self._run(m), name=f"conformance:send:{op}")
        self._prune()
        return RawJson({"opId": op})

    async def close(self) -> None:
        """Cancels the asynchronous sends still running and waits for them (at shutdown)."""
        running = [t for t in self._ops.values() if not t.done()]
        for t in running:
            t.cancel()
        await asyncio.gather(*running, return_exceptions=True)

    def _prune(self) -> None:
        finished = [op for op, t in self._ops.items() if t.done()]  # in start order
        for op in finished[: max(0, len(finished) - KEPT_OUTCOMES)]:
            del self._ops[op]

    async def outcome(self, m: Outcome) -> RawJson:
        task = self._ops.get(m.op_id)
        if task is None:
            return RawJson({"unknownOp": m.op_id})
        if not task.done():
            await asyncio.wait({task}, timeout=max(0, m.wait_ms) / 1000)
        if not task.done():
            return RawJson({"pending": True})
        if task.cancelled():
            return RawJson(
                {"result": "error", "error": classify(RuntimeError("the send was cancelled")), "elapsedMs": 0}
            )
        return RawJson(task.result())

    async def _run(self, m: Send) -> dict[str, Any]:
        started = time.monotonic()
        if m.op == "unary":
            out = await self._unary(m)
        elif m.op in ("scatter", "stream", "subscribe"):
            out = _unsupported(f"op {m.op} is not supported by this service yet")
        else:
            out = _unsupported(f"op {m.op} is not a contract op")
        out["elapsedMs"] = round((time.monotonic() - started) * 1000)
        return out

    async def _unary(self, m: Send) -> dict[str, Any]:
        target = _UNARY.get(m.target)
        if target is None:
            return _unsupported(f"target {m.target} is not a unary target of this service")
        cls, is_query = target
        try:
            options = _options(m)
            call: Coroutine[Any, Any, RawJson | None]
            if is_query:
                call = self._app.query(cls(m.payload), RawJson, **options)
            else:
                call = self._app.send(cls(m.payload), RawJson, **options)
            reply = await call
        except Exception as e:
            return {"result": "error", "error": classify(e)}
        return {"result": "ok", "payload": None if reply is None else reply.value}


def _options(m: Send) -> dict[str, Any]:
    """The library's send options the payload names."""
    options: dict[str, Any] = {"routing_key": m.routing_key, "headers": m.headers}
    if m.timeout_ms is not None:
        options["timeout"] = m.timeout_ms / 1000
    if m.retry is not None:
        backoff = m.retry.initial_backoff_ms
        options["retry"] = RetryPolicy(
            max_attempts=m.retry.max_attempts, initial_backoff=0.1 if backoff is None else backoff / 1000
        )
    return options
