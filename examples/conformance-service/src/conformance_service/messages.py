"""The service's message types (the contract's "The service's types"). A payload the contract leaves open ("any JSON
value") is a ``RawJson``, so it is read whole."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from snodra import RawJson, message_type


@message_type("conformance.Echo")
class Echo(RawJson):
    """Answers what it got (command)."""


@message_type("conformance.EchoQuery")
class EchoQuery(RawJson):
    """Echo as a query."""


@message_type("conformance.Done")
class Done(RawJson):
    """A command without a result."""


@message_type("conformance.Sleep")
@dataclass
class Sleep:
    """Sleeps ``ms``, then answers (command)."""

    ms: int = 0


@message_type("conformance.SleepQuery")
@dataclass
class SleepQuery:
    """Sleep as a query."""

    ms: int = 0


@message_type("conformance.Fail")
@dataclass
class Fail:
    """Fails as a handler raising an application error."""

    message: str = ""
    details: Any = None


@message_type("conformance.Strict")
@dataclass
class Strict:
    """A typed payload: anything that does not decode to an integer count is the library's ``INVALID_REQUEST``."""

    count: int


@message_type("conformance.View")
class View(RawJson):
    """Asks the instance for its own view of its connections (query)."""


@dataclass
class RetryOption:
    """A retry policy for one call."""

    max_attempts: int
    initial_backoff_ms: int | None = None


@message_type("conformance.Send")
@dataclass
class Send:
    """Makes the library send a request as an application would, and reports the outcome (command)."""

    op: str
    target: str
    payload: Any = None
    routing_key: str | None = None
    timeout_ms: int | None = None
    headers: dict[str, str] | None = None
    retry: RetryOption | None = None
    initial_credit: int | None = None
    max_results: int | None = None
    read: int | None = None
    read_delay_ms: int | None = None
    async_: bool = False


@message_type("conformance.Outcome")
@dataclass
class Outcome:
    """The outcome of an asynchronous ``conformance.Send``, waiting up to ``waitMs`` (query)."""

    op_id: str
    wait_ms: int = 0


@message_type("conformance.Relay")
@dataclass
class Relay:
    """Sends ``payload`` on as a command to the runner's ``Probe`` (``probe``) or ``Missing`` (``missing``)."""

    target: str = "probe"
    payload: Any = None


@message_type("conformance.runner.Probe")
class Probe(RawJson):
    """The runner's command; its payload is the runner's script, passed unchanged."""


@message_type("conformance.runner.ProbeQuery")
class ProbeQuery(RawJson):
    """The runner's query."""


@message_type("conformance.runner.Missing")
class Missing(RawJson):
    """A command nobody handles."""


@dataclass
class RelayAnswer:
    reply: Any


@dataclass
class EchoAnswer:
    payload: Any
    headers: dict[str, str]
    instance_id: str
    request_id: str


@dataclass
class SleepAnswer:
    instance_id: str
    slept_ms: int


@dataclass
class StrictAnswer:
    count: int


@dataclass
class ViewAnswer:
    service: str
    instance_id: str
    hub_ids: list[str] = field(default_factory=list)
    last_rejection: str | None = None
    subscriptions_served: int = 0
    retry: bool = False
