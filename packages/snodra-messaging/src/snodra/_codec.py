"""The payload codec: JSON documents, one object per message.

See https://snodra.damoreira.com/docs/library-authors/messages/ (Payloads).
"""

import base64
import binascii
import dataclasses
import datetime as dt
import enum
import json
import math
import re
import types
import typing
import uuid
from typing import Any, Protocol, TypeVar

from snodra._errors import DecodeError

T = TypeVar("T")

_EMPTY_OBJECT = b"{}"
_MISSING: Any = object()
_JSON_KEY = "json"


class Codec(Protocol):
    """Turns messages into payload bytes and back."""

    def encode(self, value: object) -> bytes:
        """The payload of a message; ``None`` is the empty object."""
        ...

    def decode(self, data: bytes, into: type[T]) -> T:
        """The message a payload holds; raises ``DecodeError`` when it cannot be read as ``into``."""
        ...


class RawJson:
    """A payload kept as any JSON document. Subclass it and declare a wire name with ``message_type``."""

    def __init__(self, value: object) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.value!r})"

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and other.value == self.value

    __hash__ = None  # type: ignore[assignment]


# TODO(spec-gap): SG-7
class JsonCodec:
    """The default codec.

    A dataclass is an object whose field names are camelCase (``order_id`` is ``orderId``; a field's
    ``metadata={"json": "orderID"}`` names it exactly). Nested dataclasses, ``list``, ``dict[str, X]``, ``X | None``,
    ``str``, ``int``, ``float``, ``bool``, ``bytes`` (base64), ``datetime`` (ISO 8601 UTC), ``uuid.UUID`` and
    ``enum.Enum`` are read and written; an enum is written by its member value, which another service reads as an enum
    only when it is the name of one of its constants. ``None`` fields are written as ``null``, unknown fields are
    ignored on read, an absent field takes its default, reads as ``None`` when it is optional (``X | None``) and
    fails otherwise. One trailing underscore of a field name is dropped (``type_`` is ``type``), and two fields on one
    JSON key are a ``TypeError``. A class with ``model_validate`` and ``model_dump``
    (a Pydantic model) is read and written through them. ``RawJson`` holds any document.
    """

    def encode(self, value: object) -> bytes:
        if value is None:
            return _EMPTY_OBJECT
        return _dumps(_to_json(value))

    def decode(self, data: bytes, into: type[T]) -> T:
        name = _name(into)
        if into is type(None) and not data:
            return None
        try:
            document = _loads(data)
        except (UnicodeDecodeError, ValueError, RecursionError) as e:
            raise DecodeError(f"Cannot decode payload as {name}: not a JSON document ({_reason(e)})") from None
        if into is type(None):
            if isinstance(document, dict):
                return None
            raise DecodeError(f"Cannot decode payload as {name}: expected an empty object")
        if document is None:
            raise DecodeError(f"Cannot decode payload as {name}: null")
        try:
            return _from_json(document, into, "")  # type: ignore[no-any-return]
        except _Bad as e:
            where = f" at {e.path}" if e.path else ""
            raise DecodeError(f"Cannot decode payload as {name}{where}: {e.reason}") from None
        except RecursionError:
            raise DecodeError(f"Cannot decode payload as {name}: nested too deeply") from None


class _Bad(Exception):
    def __init__(self, path: str, reason: str) -> None:
        super().__init__(reason)
        self.path = path
        self.reason = reason


def _name(into: object) -> str:
    return getattr(into, "__qualname__", None) or str(into)


def _reason(e: BaseException) -> str:
    return type(e).__name__ if isinstance(e, UnicodeDecodeError | RecursionError) else str(e).split(":")[0]


def _no_constant(name: str) -> Any:
    raise ValueError(f"{name} is not JSON")


def _finite_float(text: str) -> float:
    number = float(text)
    if not math.isfinite(number):
        raise ValueError("a number out of range")
    return number


def _loads(data: bytes) -> Any:
    return json.loads(data.decode("utf-8"), parse_constant=_no_constant, parse_float=_finite_float)


def _dumps(document: Any) -> bytes:
    try:
        return json.dumps(document, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except ValueError as e:
        if "Out of range float" in str(e):
            raise ValueError("a float must be finite to be written as JSON") from None
        raise


# ---- writing ------------------------------------------------------------------------------------------------------


def wire_name(f: dataclasses.Field[Any]) -> str:
    """The JSON name of a dataclass field."""
    named = f.metadata.get(_JSON_KEY)
    if isinstance(named, str) and named:
        return named
    name = f.name[:-1] if f.name.endswith("_") and f.name.strip("_") else f.name
    return _camel(name)


_names_checked: set[type] = set()


def _check_names(cls: type) -> None:
    """Two fields on one JSON key are a definition error, found when the class is first used."""
    if cls in _names_checked:
        return
    seen: dict[str, str] = {}
    for f in dataclasses.fields(cls):
        key = wire_name(f)
        if key in seen:
            raise TypeError(f"{cls.__qualname__}: the fields '{seen[key]}' and '{f.name}' are both JSON key '{key}'")
        seen[key] = f.name
    _names_checked.add(cls)


def _camel(name: str) -> str:
    body = name.lstrip("_")
    lead = name[: len(name) - len(body)]
    return lead + re.sub(r"_+([A-Za-z0-9])", lambda m: m.group(1).upper(), body)


def _format_datetime(value: dt.datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("a naive datetime has no JSON form: give it a time zone")
    utc = value.astimezone(dt.UTC)
    text = utc.strftime("%Y-%m-%dT%H:%M:%S")
    micro = utc.microsecond
    if micro:
        fraction = f"{micro:06d}"
        text += "." + (fraction[:3] if fraction[3:] == "000" else fraction)
    return text + "Z"


def _stable(items: list[Any]) -> list[Any]:
    """A set's elements in a fixed order: sorted when they sort, else by their JSON text."""
    try:
        return sorted(items)
    except TypeError:
        return sorted(items, key=lambda v: json.dumps(v, separators=(",", ":"), sort_keys=True))


def _has_model_protocol(cls: type) -> bool:
    return callable(getattr(cls, "model_validate", None)) and callable(getattr(cls, "model_dump", None))


def _to_json(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        # an IntEnum or StrEnum member is an int or str, but written as its value
        return value.value if isinstance(value, enum.Enum) else value
    if isinstance(value, enum.Enum):
        return _to_json(value.value)
    if isinstance(value, RawJson):
        return value.value
    if isinstance(value, dt.datetime):
        return _format_datetime(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes | bytearray):
        return base64.b64encode(bytes(value)).decode("ascii")
    if not isinstance(value, type) and _has_model_protocol(type(value)):
        return value.model_dump(mode="json", by_alias=True)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        _check_names(type(value))
        return {wire_name(f): _to_json(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, set | frozenset):
        return _stable([_to_json(v) for v in value])
    if isinstance(value, list | tuple):
        return [_to_json(v) for v in value]
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if not isinstance(k, str):
                raise TypeError(f"a JSON object has text keys, not {type(k).__name__}")
            out[k] = _to_json(v)
        return out
    raise TypeError(f"{type(value).__qualname__} has no JSON form: use a dataclass, a model or RawJson")


# ---- reading ------------------------------------------------------------------------------------------------------

_DATETIME = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
    re.ASCII,
)

_hints_cache: dict[type, dict[str, Any]] = {}


def _hints(cls: type) -> dict[str, Any]:
    cached = _hints_cache.get(cls)
    if cached is None:
        try:
            cached = typing.get_type_hints(cls)
        except Exception as e:
            raise TypeError(f"cannot resolve the field types of {cls.__qualname__}: {e}") from e
        _hints_cache[cls] = cached
    return cached


def _parse_datetime(text: str) -> dt.datetime | None:
    m = _DATETIME.fullmatch(text)
    if m is None:
        return None
    year, month, day, hour, minute, second = (int(g) for g in m.groups()[:6])
    micro = int((m.group(7) or "0").ljust(6, "0")[:6])
    zone = m.group(8)
    try:
        if zone == "Z":
            offset = dt.timedelta(0)
        else:
            sign = -1 if zone[0] == "-" else 1
            offset = sign * dt.timedelta(hours=int(zone[1:3]), minutes=int(zone[4:6]))
        local = dt.datetime(year, month, day, hour, minute, second, micro, tzinfo=dt.timezone(offset))
    except ValueError:
        return None
    return local.astimezone(dt.UTC)


def _kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "a boolean"
    if isinstance(value, int | float):
        return "a number"
    if isinstance(value, str):
        return "a string"
    if isinstance(value, list):
        return "an array"
    return "an object"


def _join(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _from_json(value: Any, hint: Any, path: str) -> Any:
    if isinstance(hint, typing.TypeAliasType):
        return _from_json(value, hint.__value__, path)
    if hint is typing.Any or hint is object:
        return value
    origin = typing.get_origin(hint)
    if origin is typing.Union or origin is types.UnionType:
        arms = [a for a in typing.get_args(hint) if a is not type(None)]
        if value is None:
            if len(arms) < len(typing.get_args(hint)):
                return None
            raise _Bad(path, "null is not allowed")
        if len(arms) == 1:
            return _from_json(value, arms[0], path)
        raise TypeError(f"{hint} is not supported: only a single type or an optional one")
    if hint is None or hint is type(None):
        if value is None:
            return None
        raise _Bad(path, f"expected null, found {_kind(value)}")
    if value is None:
        raise _Bad(path, "null is not allowed")
    if origin is list or hint is list:
        (item,) = typing.get_args(hint) or (Any,)
        return [_from_json(v, item, f"{path}[{i}]") for i, v in enumerate(_array(value, path))]
    if origin in (set, frozenset) or hint in (set, frozenset):
        (item,) = typing.get_args(hint) or (Any,)
        items = [_from_json(v, item, f"{path}[{i}]") for i, v in enumerate(_array(value, path))]
        return (origin or hint)(items)
    if origin is tuple or hint is tuple:
        args = typing.get_args(hint)
        if len(args) == 2 and args[1] is Ellipsis:
            return tuple(_from_json(v, args[0], f"{path}[{i}]") for i, v in enumerate(_array(value, path)))
        if not args:
            return tuple(_array(value, path))
        raise TypeError(f"{hint} is not supported: use tuple[X, ...]")
    if origin is dict or hint is dict:
        args = typing.get_args(hint) or (str, Any)
        if args[0] is not str:
            raise TypeError(f"{hint} is not supported: a JSON object has text keys")
        return {k: _from_json(v, args[1], _join(path, k)) for k, v in _object(value, path).items()}
    if not isinstance(hint, type):
        raise TypeError(f"{hint} is not supported in a payload")
    if issubclass(hint, RawJson):
        return hint(value)
    if hint is bool:
        if isinstance(value, bool):
            return value
        raise _Bad(path, f"expected a boolean, found {_kind(value)}")
    if hint is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        raise _Bad(path, f"expected an integer, found {_kind(value)}")
    if hint is float:
        if isinstance(value, int | float) and not isinstance(value, bool):
            try:
                number = float(value)
            except OverflowError:
                raise _Bad(path, "the number is out of range") from None
            if not math.isfinite(number):
                raise _Bad(path, "the number is out of range")
            return number
        raise _Bad(path, f"expected a number, found {_kind(value)}")
    if hint is str:
        if isinstance(value, str):
            return value
        raise _Bad(path, f"expected a string, found {_kind(value)}")
    if hint is bytes:
        if not isinstance(value, str):
            raise _Bad(path, f"expected base64 text, found {_kind(value)}")
        try:
            return base64.b64decode(value, validate=True)
        except binascii.Error:
            raise _Bad(path, "expected base64 text") from None
    if issubclass(hint, enum.Enum):
        for member in hint:
            if type(member.value) is type(value) and member.value == value:
                return member
        raise _Bad(path, f"not a value of {hint.__qualname__}")
    if hint is dt.datetime:
        parsed = _parse_datetime(value) if isinstance(value, str) else None
        if parsed is None:
            raise _Bad(path, "expected an ISO 8601 date and time with a zone, like 2026-10-28T14:03:22Z")
        return parsed
    if hint is uuid.UUID:
        try:
            return uuid.UUID(value) if isinstance(value, str) else _bad_uuid(path, value)
        except ValueError:
            raise _Bad(path, "expected a UUID") from None
    if _has_model_protocol(hint):
        try:
            return hint.model_validate(value)  # type: ignore[attr-defined]
        except Exception as e:
            raise _Bad(path, f"the model refused it ({type(e).__name__})") from None
    if dataclasses.is_dataclass(hint):
        return _dataclass(value, hint, path)
    raise TypeError(f"{hint.__qualname__} is not supported in a payload: use a dataclass, a model or RawJson")


def _bad_uuid(path: str, value: Any) -> Any:
    raise _Bad(path, f"expected a UUID string, found {_kind(value)}")


def _array(value: Any, path: str) -> list[Any]:
    if isinstance(value, list):
        return value
    raise _Bad(path, f"expected an array, found {_kind(value)}")


def _object(value: Any, path: str) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    raise _Bad(path, f"expected an object, found {_kind(value)}")


def _dataclass(value: Any, cls: type, path: str) -> Any:
    obj = _object(value, path)
    _check_names(cls)
    hints = _hints(cls)
    kwargs: dict[str, Any] = {}
    for f in dataclasses.fields(cls):
        if not f.init:
            continue
        key = wire_name(f)
        if key in obj:
            kwargs[f.name] = _from_json(obj[key], hints[f.name], _join(path, key))
        elif f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
            if not _is_optional(hints[f.name]):
                raise _Bad(path, f"missing field '{key}'")
            kwargs[f.name] = None
    return cls(**kwargs)


def _is_optional(hint: Any) -> bool:
    return typing.get_origin(hint) in (typing.Union, types.UnionType) and type(None) in typing.get_args(hint)
