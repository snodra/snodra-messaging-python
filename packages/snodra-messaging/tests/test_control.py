"""The hub's control requests: off the read loop, one at a time per processor, exactly one reply on the arrival stream.

Check: L3-CONTROL.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Processor control") and
https://snodra.damoreira.com/docs/server/processor-control/.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import replace

from eventhelpers import OrderPlaced, dead_letter
from fakehub import FakeHub, FakeStream, ack, forward
from helpers import SCALED, acknowledged, eventually, library_tasks, new_app, processors_of
from snodra import App, DeadLetterReason, MemoryEventStore, MemoryStore
from snodra._hubpb import hub_pb2
from snodra._store import ProcessorControlStore
from snodra._timings import Timings


def _timings(**changes: object) -> Timings:
    values: dict[str, object] = {
        "event_handler_backoff": 0.01,
        "event_resubscribe_min": 0.01,
        "event_resubscribe_max": 0.02,
        "event_resubscribe_stable": 0.05,
        "processor_control_poll": 5.0,
        "status_interval": 300.0,
    }
    values.update(changes)
    return replace(SCALED, **values)  # type: ignore[arg-type]


def _control(
    kind: str, processor: str = "default", control_id: str = "ctl-1", **action: object
) -> hub_pb2.ControlRequest:
    return hub_pb2.ControlRequest(
        control_id=control_id,
        processor=processor,
        **{kind: action or {}},
    )


async def _app(hub: FakeHub | tuple[FakeHub, ...], store: MemoryStore | None = None) -> tuple[App, MemoryStore]:
    store = MemoryStore() if store is None else store
    app = new_app(
        hub,
        store=store,
        event_store=MemoryEventStore(),
        outbox_enabled=False,
        _timings=_timings(),
    )

    @app.event_handler
    async def on_placed(event: OrderPlaced) -> None:
        pass

    return app, store


async def _stream(hub: FakeHub, hub_id: str = "hub-1") -> FakeStream:
    s = await acknowledged(hub, hub_id)
    return s


async def test_pause_and_resume_through_the_hub_answer_ok_and_persist_the_control_id(hub: FakeHub) -> None:
    app, store = await _app(hub)
    await app.start()
    s = await _stream(hub)
    await s.send(hub_pb2.ServerMessage(control=_control("pause", control_id="p-1")))
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.WhichOneof("result"), reply.ok.detail) == ("p-1", "ok", "paused")
    doc = await store.control.get("default")
    assert (doc.paused, doc.paused_by) == (True, "p-1")
    assert processors_of(app).statuses()[0].state == hub_pb2.PROCESSOR_PAUSED

    await s.send(hub_pb2.ServerMessage(control=_control("resume", control_id="r-1")))
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.WhichOneof("result"), reply.ok.detail) == ("r-1", "ok", "resumed")
    doc = await store.control.get("default")
    assert (doc.paused, doc.paused_by) == (False, None)


async def test_an_unknown_processor_a_missing_action_and_a_reset_are_refused(hub: FakeHub) -> None:
    app, _ = await _app(hub)
    await app.start()
    s = await _stream(hub)
    await s.send(hub_pb2.ServerMessage(control=_control("pause", processor="audit", control_id="u-1")))
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.WhichOneof("result")) == ("u-1", "refused")
    assert reply.refused.code == hub_pb2.CONTROL_REFUSAL_UNKNOWN_PROCESSOR
    assert "does not run" in reply.refused.message

    # A malformed prepare is checked before the processor: INVALID even for one that does not run here.
    await s.send(
        hub_pb2.ServerMessage(
            control=_control("reset_prepare", processor="audit", control_id="u-2", operation_id="op-1", lease_ms=1)
        )
    )
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.refused.code) == ("u-2", hub_pb2.CONTROL_REFUSAL_INVALID)

    await s.send(hub_pb2.ServerMessage(control=hub_pb2.ControlRequest(control_id="n-1", processor="default")))
    reply = await s.recv_control_reply()
    assert reply.refused.code == hub_pb2.CONTROL_REFUSAL_INVALID
    assert reply.refused.message == "the control request has no action this instance knows"

    # A malformed prepare is INVALID; an execute of an operation this instance does not hold is NOT_PREPARED; a
    # finish with nothing held is ok (the operation ended elsewhere, or was aborted).
    await s.send(hub_pb2.ServerMessage(control=_control("reset_prepare", control_id="reset-1")))
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.refused.code) == ("reset-1", hub_pb2.CONTROL_REFUSAL_INVALID)
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="reset-2",
                processor="default",
                reset_execute=hub_pb2.ResetExecute(operation_id="op-1", target=hub_pb2.ResetTarget(earliest=True)),
            )
        )
    )
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.refused.code) == ("reset-2", hub_pb2.CONTROL_REFUSAL_NOT_PREPARED)
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="reset-3",
                processor="default",
                reset_finish=hub_pb2.ResetFinish(operation_id="op-1"),
            )
        )
    )
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.WhichOneof("result"), reply.ok.detail) == (
        "reset-3",
        "ok",
        "no operation holds the lock",
    )


async def test_every_request_is_answered_exactly_once_in_arrival_order(hub: FakeHub) -> None:
    app, _ = await _app(hub)
    await app.start()
    s = await _stream(hub)
    for n in range(3):
        action = "pause" if n % 2 == 0 else "resume"
        await s.send(hub_pb2.ServerMessage(control=_control(action, control_id=f"c-{n}")))
    replies = [await s.recv_control_reply() for _ in range(3)]
    assert [reply.control_id for reply in replies] == ["c-0", "c-1", "c-2"]
    assert all(reply.WhichOneof("result") == "ok" for reply in replies)
    await s.nothing(0.15, "control_reply")


async def test_the_reply_goes_back_on_the_arrival_stream(hub: FakeHub, hub2: FakeHub) -> None:
    app, _ = await _app((hub, hub2))
    await app.start()
    first = await _stream(hub, "hub-1")
    second = await _stream(hub2, "hub-2")
    await eventually(lambda: app.hub_ids() == ["hub-1", "hub-2"])
    await second.send(hub_pb2.ServerMessage(control=_control("pause", control_id="only-here")))
    reply = await second.recv_control_reply()
    assert reply.control_id == "only-here"
    await first.nothing(0.15, "control_reply")


class _SlowControl:
    """A control store whose writes wait, recording how many run at once."""

    def __init__(self, inner: ProcessorControlStore, delay: float) -> None:
        self._inner = inner
        self._delay = delay
        self.running = 0
        self.max_running = 0
        self.calls: list[tuple[str, bool, str]] = []

    async def set_paused(self, processor: str, paused: bool, by: str, now_ms: int) -> None:
        self.running += 1
        self.max_running = max(self.max_running, self.running)
        self.calls.append((processor, paused, by))
        try:
            await asyncio.sleep(self._delay)
            await self._inner.set_paused(processor, paused, by, now_ms)
        finally:
            self.running -= 1

    def __getattr__(self, name: str) -> object:
        return getattr(self._inner, name)


async def test_a_control_action_runs_off_the_read_loop_and_never_delays_the_stream(hub: FakeHub) -> None:
    store = MemoryStore()
    slow = _SlowControl(store.control, 0.3)
    store.control = slow  # type: ignore[assignment]
    app, _ = await _app(hub, store)
    await app.start()
    s = await hub.next_stream()
    await s.recv_register()
    await s.ack(ack("hub-1", interval_ms=100))

    began = time.monotonic()
    beats_before = len(s.heartbeats)
    await s.send(hub_pb2.ServerMessage(control=_control("pause", control_id="slow")))
    # A request forwarded while the pause waits is served: the control is not on the read loop.
    await s.send(
        forward(hub_pb2.Request(id="q-1", kind=hub_pb2.QUERY, type="orders.OrderPlaced", payload=b'{"orderId":"o-1"}'))
    )
    # The type is not a handler here; the hub's request is refused by the dispatcher, which proves the read loop ran.
    while True:
        m = await s.recv(within=1.0)
        if m.WhichOneof("message") in ("reply", "end", "error"):
            break
    replied_at = time.monotonic()
    assert replied_at - began < 0.25, "the forwarded request waited behind the control action"
    reply = await s.recv_control_reply(within=2.0)
    finished_at = time.monotonic()
    assert reply.control_id == "slow"
    assert finished_at - replied_at >= 0.2, "the control action had not finished when the request was answered"
    during = [at for at in s.heartbeats[beats_before:] if began <= at <= finished_at]
    assert len(during) >= 2, f"heartbeats during the control action: {during}"


async def test_one_action_per_processor_at_a_time(hub: FakeHub) -> None:
    store = MemoryStore()
    slow = _SlowControl(store.control, 0.1)
    store.control = slow  # type: ignore[assignment]
    app, _ = await _app(hub, store)
    await app.start()
    s = await _stream(hub)
    await s.send(hub_pb2.ServerMessage(control=_control("pause", control_id="a-1")))
    await s.send(hub_pb2.ServerMessage(control=_control("resume", control_id="a-2")))
    await s.send(hub_pb2.ServerMessage(control=_control("pause", control_id="a-3")))
    replies = [await s.recv_control_reply(within=3.0) for _ in range(3)]
    assert [reply.control_id for reply in replies] == ["a-1", "a-2", "a-3"]
    assert slow.max_running == 1, "two actions of one processor ran at once"
    assert [call[1] for call in slow.calls] == [True, False, True]


async def test_the_dead_letters_are_listed_retried_retried_all_and_discarded_through_control(hub: FakeHub) -> None:
    store = MemoryStore()
    app, _ = await _app(hub, store)
    await app.start()
    s = await _stream(hub)
    head = dead_letter("1", key="k1", event_id="e-1", payload=b'{"orderId":"o-1"}')
    follower = dead_letter("2", key="k1", event_id="e-2", payload=b'{"orderId":"o-2"}')
    invalid = dead_letter("3", reason=DeadLetterReason.INVALID, key=None, event_type=None)
    other = dead_letter("4", processor="mail", key=None)
    for letter in (head, follower, invalid, other):
        await store.dead_letters.park(letter)

    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="l-1", processor="default", dead_letter_list=hub_pb2.DeadLetterList(limit=10)
            )
        )
    )
    page = (await s.recv_control_reply()).dead_letters
    assert page.total == 3, "only the default processor's letters"
    rows = {row.event_id: row for row in page.letters}
    assert rows["e-1"].reason == "HANDLER_FAILED"
    assert rows["e-1"].error_type == "RuntimeError"
    assert rows["e-1"].error_message == "boom"
    assert rows["e-2"].reason == "HANDLER_FAILED"
    assert rows["e-3"].reason == "INVALID"

    # The retry of the head runs the handler, deletes the letter and answers Retried.
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="r-1", processor="default", dead_letter_retry=hub_pb2.DeadLetterRetry(letter_id=head.id)
            )
        )
    )
    outcome = (await s.recv_control_reply()).retry
    assert (outcome.result, outcome.detail) == ("Retried", "")
    assert await store.dead_letters.get(head.id) is None

    # Another processor's letter is NotFound to this processor.
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="r-2", processor="default", dead_letter_retry=hub_pb2.DeadLetterRetry(letter_id=other.id)
            )
        )
    )
    assert (await s.recv_control_reply()).retry.result == "NotFound"

    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="r-3", processor="default", dead_letter_retry_all=hub_pb2.DeadLetterRetryAll()
            )
        )
    )
    all_outcome = (await s.recv_control_reply()).retry_all
    assert (all_outcome.retried, all_outcome.failed, all_outcome.blocked, all_outcome.skipped) == (1, 0, 0, 1)

    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="d-1",
                processor="default",
                dead_letter_discard=hub_pb2.DeadLetterDiscard(letter_id=invalid.id),
            )
        )
    )
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.WhichOneof("result")) == ("d-1", "ok")
    assert await store.dead_letters.get(invalid.id) is None
    await s.send(
        hub_pb2.ServerMessage(
            control=hub_pb2.ControlRequest(
                control_id="d-2",
                processor="default",
                dead_letter_discard=hub_pb2.DeadLetterDiscard(letter_id=invalid.id),
            )
        )
    )
    reply = await s.recv_control_reply()
    assert (reply.control_id, reply.refused.code) == ("d-2", hub_pb2.CONTROL_REFUSAL_NOT_FOUND)

    for bad in (
        hub_pb2.ControlRequest(control_id="b-1", processor="default", dead_letter_list=hub_pb2.DeadLetterList(limit=0)),
        hub_pb2.ControlRequest(control_id="b-2", processor="default", dead_letter_discard=hub_pb2.DeadLetterDiscard()),
    ):
        await s.send(hub_pb2.ServerMessage(control=bad))
        reply = await s.recv_control_reply()
        assert (reply.control_id, reply.refused.code) == (bad.control_id, hub_pb2.CONTROL_REFUSAL_INVALID)


async def test_cancelling_the_app_leaves_no_control_task(hub: FakeHub) -> None:
    store = MemoryStore()
    slow = _SlowControl(store.control, 30.0)
    store.control = slow  # type: ignore[assignment]
    app, _ = await _app(hub, store)
    await app.start()
    s = await _stream(hub)
    await s.send(hub_pb2.ServerMessage(control=_control("pause", control_id="stuck")))
    await eventually(lambda: slow.running == 1)
    assert "snodra:control:default" in library_tasks()
    await app.shutdown(timeout=1.0)
    assert "snodra:control:default" not in library_tasks()
    assert not any(name.startswith("snodra:control:") for name in library_tasks())
