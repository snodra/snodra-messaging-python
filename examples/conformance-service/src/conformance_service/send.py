"""``conformance.Send`` and ``conformance.Outcome``: the service sends a request through the library as an application
does, and reports the outcome in the contract's shape."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Coroutine
from typing import Any

from snodra import (
    App,
    Gathered,
    HubError,
    JsonCodec,
    LostParts,
    NeverSent,
    OutcomeUnknown,
    RawJson,
    ReservedHeader,
    RetryPolicy,
)

from conformance_service.messages import (
    Echo,
    EchoQuery,
    Missing,
    Outcome,
    Probe,
    ProbeQuery,
    ProbeStream,
    ProbeWatch,
    Send,
    SharedQuery,
    Stream,
    Watch,
)

_UNARY: dict[str, tuple[type[RawJson], bool]] = {
    "probe": (Probe, False),
    "probe-query": (ProbeQuery, True),
    "missing": (Missing, False),
    "echo": (Echo, False),
    "echo-query": (EchoQuery, True),
    "shared-query": (SharedQuery, True),
}
"""The unary targets: the message class and whether it is a query."""

_STREAMED: dict[str, type] = {
    "probe-stream": ProbeStream,
    "stream": Stream,
}
"""The streaming targets: the message class of each (its items are read as ``RawJson``)."""

_SUBSCRIBED: dict[str, type] = {
    "probe-watch": ProbeWatch,
    "watch": Watch,
}
"""The subscription targets: the request class of each (its initial result and updates are read as ``RawJson``)."""

_GATHERED: dict[str, type[RawJson]] = {
    "probe-query": ProbeQuery,
    "echo-query": EchoQuery,
    "shared-query": SharedQuery,
}
"""The targets of a scatter-gather: the query types."""

KEPT_OUTCOMES = 1024
"""Finished asynchronous sends whose outcome is kept for ``conformance.Outcome``; older ones are forgotten."""

_CODEC = JsonCodec()


def _request(cls: type, payload: Any) -> Any:
    """The target's request message: a ``RawJson`` message holds the payload document itself, a dataclass its fields
    (read with the library's codec, so the contract's names decode and an absent field takes its default)."""
    if issubclass(cls, RawJson):
        return cls(payload)
    return _CODEC.decode(json.dumps(payload).encode("utf-8"), cls)


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
        elif m.op == "scatter":
            out = await self._scatter(m)
        elif m.op == "stream":
            out = await self._stream(m)
        elif m.op == "subscribe":
            out = await self._subscribe(m)
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

    async def _stream(self, m: Send) -> dict[str, Any]:
        """A streaming query of the target, read as the contract's outcome: the items in order, and whether the stream
        completed or the caller stopped after ``read`` items (which cancels it)."""
        cls = _STREAMED.get(m.target)
        if cls is None:
            return _unsupported(f"target {m.target} is not a streaming target of this service")
        items: list[Any] = []
        stopped = False
        try:
            options = _options(m)
            del options["routing_key"]  # a stream goes to one target: unkeyed
            async with self._app.stream(
                _request(cls, m.payload),
                RawJson,
                initial_credit=m.initial_credit,
                max_results=m.max_results or 0,
                **options,
            ) as stream:
                async for item in stream:
                    items.append(item.value)
                    if m.read_delay_ms:
                        await asyncio.sleep(m.read_delay_ms / 1000)
                    if m.read is not None and len(items) >= m.read:
                        stopped = True
                        break
        except Exception as e:
            return {"result": "error", "items": items, "error": classify(e)}
        return {"result": "ok", "items": items, "end": "stopped" if stopped else "completed"}

    async def _subscribe(self, m: Send) -> dict[str, Any]:
        """A subscription query of the target, read as the contract's outcome: the initial result and the updates in
        order, and whether the subscription completed or the caller stopped after ``read`` items (which cancels it)."""
        cls = _SUBSCRIBED.get(m.target)
        if cls is None:
            return _unsupported(f"target {m.target} is not a subscription target of this service")
        items: list[Any] = []
        stopped = False
        try:
            options = _options(m)
            del options["routing_key"]  # a subscription goes to every target: unkeyed
            async with self._app.subscribe(
                _request(cls, m.payload),
                RawJson,
                RawJson,
                initial_credit=m.initial_credit,
                max_results=m.max_results or 0,
                **options,
            ) as sub:
                items.append(sub.initial.value)
                if m.read is not None and len(items) >= m.read:
                    stopped = True
                else:
                    async for update in sub:
                        items.append(update.value)
                        if m.read_delay_ms:
                            await asyncio.sleep(m.read_delay_ms / 1000)
                        if m.read is not None and len(items) >= m.read:
                            stopped = True
                            break
        except Exception as e:
            return {"result": "error", "items": items, "error": classify(e)}
        return {"result": "ok", "items": items, "end": "stopped" if stopped else "completed"}

    async def _scatter(self, m: Send) -> dict[str, Any]:
        """A scatter-gather of the query target with the library's gather: one part per answer, in arrival order,
        with the counts."""
        cls = _GATHERED.get(m.target)
        if cls is None:
            return _unsupported(f"target {m.target} is not a query target of this service")
        try:
            options = _options(m)
            del options["routing_key"]  # a gather goes to every target: unkeyed
            gathered: Gathered[RawJson] = await self._app.gather(
                cls(m.payload), RawJson, max_results=m.max_results or 0, **options
            )
        except Exception as e:
            return {"result": "error", "error": classify(e)}
        parts: list[dict[str, Any]] = []
        for p in gathered.parts:
            part: dict[str, Any] = {"service": p.service, "instance": p.instance, "ok": p.error is None}
            if p.error is not None:
                part["error"] = classify(p.error)
            else:
                part["payload"] = None if p.value is None else p.value.value
            parts.append(part)
        return {"result": "ok", "parts": parts, "failed": gathered.failed, "timedOut": gathered.timed_out}


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
