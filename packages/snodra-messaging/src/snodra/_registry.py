"""The handlers of an app, by kind and type, and the type lists its ``Register`` declares."""

from __future__ import annotations

import inspect
import typing
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from snodra._errors import RegistrationClosed
from snodra._messages import type_name

Kind = Literal["command", "query"]

HandlerFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class Handler:
    """A registered handler: an ``async def`` taking the message, the class its first parameter is annotated with."""

    fn: HandlerFn
    message: type
    type: str
    kind: Kind
    label: str
    """How logs and errors name it: the function's qualified name."""


def label_of(fn: object) -> str:
    return getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or repr(fn)


class Registry:
    """Command and query handlers by type; closed when the app starts."""

    def __init__(self) -> None:
        self._handlers: dict[Kind, dict[str, Handler]] = {"command": {}, "query": {}}
        self._closed = False

    def close(self) -> None:
        """No handler is registered from now on."""
        self._closed = True

    def add(self, kind: Kind, fn: object) -> Handler:
        """Registers fn as the kind's handler of the type its first parameter is annotated with. Raises
        :class:`RegistrationClosed` once closed and ``TypeError`` for what cannot be served, naming the handler."""
        label = label_of(fn)
        if self._closed:
            raise RegistrationClosed(f"handler {label}: handlers are registered before the app starts")
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(f"handler {label} must be an async def taking the message")
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
        handler = Handler(typing.cast("HandlerFn", fn), message, name, kind, label)
        self._handlers[kind][name] = handler
        return handler

    def find(self, kind: Kind, type_: str) -> Handler | None:
        """The kind's handler of a type; None when there is none."""
        return self._handlers[kind].get(type_)

    def types(self, kind: Kind) -> list[str]:
        """The kind's types, sorted: a ``Register`` list."""
        return sorted(self._handlers[kind])


def _message_class(fn: object, label: str) -> type:
    """The class the first parameter of fn is annotated with; ``TypeError`` when fn cannot be called with the message
    alone or its annotation is not a message class."""
    try:
        signature = inspect.signature(fn)  # type: ignore[arg-type]
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
    try:
        hints = typing.get_type_hints(fn)
    except Exception as e:
        raise TypeError(f"handler {label}: the annotation of {first.name} cannot be resolved ({e})") from None
    cls = hints.get(first.name)
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
