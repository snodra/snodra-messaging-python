"""Message types: the wire name lives on the class.

See https://snodra.damoreira.com/library-authors/messages/ (Type names).
"""

from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

_ATTRIBUTE = "__snodra_type__"


def message_type(name: str) -> Callable[[type[T]], type[T]]:
    """Declare the wire type name of a message class, for example ``orders.PlaceOrder``.

    The name is what the hub routes on. It belongs to the decorated class only: a subclass declares its own.
    """
    if not isinstance(name, str):
        raise TypeError(f"a message type name is text, not {type(name).__name__}")
    if not name.strip():
        raise ValueError("a message type name must not be empty")

    def mark(cls: type[T]) -> type[T]:
        setattr(cls, _ATTRIBUTE, name)
        return cls

    return mark


def type_name(message: object | type) -> str:
    """The wire type name of a message class or instance; ``TypeError`` for a class not marked ``message_type``."""
    cls = message if isinstance(message, type) else type(message)
    name = vars(cls).get(_ATTRIBUTE)
    if not isinstance(name, str):
        raise TypeError(f"{cls.__qualname__} is not a message: declare its wire name with @snodra.message_type(...)")
    return name
