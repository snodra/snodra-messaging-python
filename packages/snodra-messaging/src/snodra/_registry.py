"""The handlers of an app, by kind and type, and the type lists its ``Register`` declares."""

from __future__ import annotations

import inspect
import sys
import typing
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from snodra._errors import RegistrationClosed
from snodra._identity import Guard
from snodra._messages import type_name

Kind = Literal["command", "query"]

_UNEVALUATED: dict[str, Any] = {}
"""``inspect.signature`` leaves an annotation it cannot evaluate as a forward reference where it can (Python 3.14)."""
if sys.version_info >= (3, 14):
    import annotationlib

    _UNEVALUATED["annotation_format"] = annotationlib.Format.FORWARDREF

HandlerFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class Handler:
    """A registered handler: an ``async def`` taking the message, the class its first parameter is annotated with."""

    fn: HandlerFn
    message: type
    type: str
    kind: Kind
    label: str
    """How logs and errors name it: the function's qualified name, without ``<locals>``."""
    guard: Guard | None = None
    """Who may call it; None: anyone, an anonymous caller too."""
    shared: bool = False
    """A query type many services may own (``Register.shared_query_types``); a command is never shared."""
    stream: bool = False
    """A streaming query handler: an async generator function, whose query type is served in mode ``STREAMING``."""
    subscription: bool = False
    """A subscription query handler: an async function answering the initial result, whose query type is served in mode
    ``SUBSCRIPTION``."""


def label_of(fn: object) -> str:
    """The function's qualified name without ``<locals>.`` (``register.place_order``)."""
    name = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None)
    return repr(fn) if not isinstance(name, str) else name.replace("<locals>.", "")


class Registry:
    """Command and query handlers by type; closed when the app starts."""

    def __init__(self) -> None:
        self._handlers: dict[Kind, dict[str, Handler]] = {"command": {}, "query": {}}
        self._closed = False

    def close(self) -> None:
        """No handler is registered from now on."""
        self._closed = True

    def add(
        self,
        kind: Kind,
        fn: object,
        guard: Guard | None = None,
        *,
        shared: bool = False,
        stream: bool = False,
        subscription: bool = False,
    ) -> Handler:
        """Registers fn as the kind's handler of the type its first parameter is annotated with, for the callers
        guard lets through; a shared query type when shared, a streaming query handler when stream (an async generator
        function, whose type is served in mode ``STREAMING``), a subscription query handler when subscription (an async
        function whose result is a subscription's initial result, its type served in mode ``SUBSCRIPTION``). Raises
        :class:`RegistrationClosed` once closed and ``TypeError`` for what cannot be served, naming the handler."""
        label = label_of(fn)
        if self._closed:
            raise RegistrationClosed(f"handler {label}: handlers are registered before the app starts")
        if stream:
            if kind != "query":
                raise TypeError(f"handler {label}: a command type is never streamed")
            if not inspect.isasyncgenfunction(fn):
                raise TypeError(f"stream handler {label} must be an async generator function taking the message")
        elif not inspect.iscoroutinefunction(fn):
            raise TypeError(f"handler {label} must be an async def taking the message")
        if subscription and kind != "query":
            raise TypeError(f"handler {label}: a command type is never a subscription")
        if subscription and stream:
            raise TypeError(f"handler {label}: a streaming query handler is never a subscription query handler")
        message = _message_class(fn, label)
        name = type_name(message)
        other: Kind = "query" if kind == "command" else "command"
        if (taken := self._handlers[other].get(name)) is not None:
            raise TypeError(
                f"handler {label}: type {name} has the {other} handler {taken.label}; a type cannot be both a command "
                "and a query"
            )
        if (taken := self._handlers[kind].get(name)) is not None:
            raise TypeError(f"handler {label}: type {name} already has the {kind} handler {taken.label}")
        if shared and kind != "query":
            raise TypeError(f"handler {label}: a command type is never shared")
        handler = Handler(typing.cast("HandlerFn", fn), message, name, kind, label, guard, shared, stream, subscription)
        self._handlers[kind][name] = handler
        return handler

    def find(self, kind: Kind, type_: str) -> Handler | None:
        """The kind's handler of a type; None when there is none."""
        return self._handlers[kind].get(type_)

    def types(self, kind: Kind) -> list[str]:
        """The kind's types that are not shared, sorted: ``Register.command_types`` or ``Register.query_types``."""
        return sorted(t for t, h in self._handlers[kind].items() if not h.shared)

    def shared_types(self) -> list[str]:
        """The shared query types, sorted: ``Register.shared_query_types``."""
        return sorted(t for t, h in self._handlers["query"].items() if h.shared)

    def subscription_types(self) -> list[str]:
        """The query types served by a subscription handler, sorted: ``Register.subscription_query_types`` (each is also
        one of the register's query types: exclusive or shared)."""
        return sorted(t for t, h in self._handlers["query"].items() if h.subscription)


def _message_class(fn: object, label: str) -> type:
    """The class the first parameter of fn is annotated with; ``TypeError`` when fn cannot be called with the message
    alone or its annotation is not a message class."""
    try:
        # Annotations are not evaluated here (Python 3.14 evaluates them lazily): only the message's is resolved below.
        signature = inspect.signature(fn, **_UNEVALUATED)  # type: ignore[arg-type]
    except (TypeError, ValueError) as e:
        raise TypeError(f"handler {label}: its signature cannot be read ({e})") from None
    params = list(signature.parameters.values())
    positional = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    if not params or params[0].kind not in positional:
        raise TypeError(f"handler {label} must take the message as its first parameter")
    first, rest = params[0], params[1:]
    for p in rest:
        if p.default is inspect.Parameter.empty and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            raise TypeError(f"handler {label}: its parameter {p.name} has no default; the message is the only argument")
    if first.annotation is inspect.Parameter.empty:
        raise TypeError(f"handler {label}: its parameter {first.name} must be annotated with the message class")
    try:
        cls = _resolve(first.annotation, fn)
    except Exception as e:
        raise TypeError(f"handler {label}: the annotation of {first.name} cannot be resolved ({e})") from None
    if cls is None:
        raise TypeError(f"handler {label}: its parameter {first.name} must be annotated with the message class")
    if not isinstance(cls, type):
        raise TypeError(f"handler {label}: {first.name} is annotated {cls!r}, not a message class")
    try:
        type_name(cls)
    except TypeError:
        raise TypeError(
            f"handler {label}: {cls.__qualname__} is not a message: declare its wire name with "
            "@snodra.message_type(...)"
        ) from None
    return cls


def _resolve(annotation: object, fn: object) -> object:
    """An annotation's value: a text (``from __future__ import annotations``) or a forward reference is evaluated in the
    function's module, as ``typing.get_type_hints`` does; ``Annotated`` is unwrapped."""
    text = annotation if isinstance(annotation, str) else getattr(annotation, "__forward_arg__", None)
    if isinstance(text, str):
        annotation = eval(text, dict(getattr(fn, "__globals__", {})))  # an annotation, evaluated as typing does
    if typing.get_origin(annotation) is typing.Annotated:
        annotation = typing.get_args(annotation)[0]
    return annotation
