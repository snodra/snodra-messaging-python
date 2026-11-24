import enum
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
import snodra
from snodra import DecodeError, JsonCodec, RawJson


@dataclass
class Line:
    sku: str
    quantity: int


@snodra.message_type("orders.PlaceOrder")
@dataclass
class PlaceOrder:
    order_id: str
    lines: list[Line]
    note: str | None = None
    placed_at: datetime | None = None


@snodra.message_type("orders.Echo")
class Echo(RawJson):
    pass


class OrderModel:
    """A stand-in for a Pydantic model: it has `model_validate` and `model_dump`."""

    def __init__(self, order_id: str) -> None:
        self.order_id = order_id

    @classmethod
    def model_validate(cls, obj: Any) -> "OrderModel":
        if not isinstance(obj, dict) or "orderId" not in obj:
            raise ValueError("validation failed")
        return cls(obj["orderId"])

    def model_dump(self, *, mode: str = "python", by_alias: bool = False) -> dict[str, Any]:
        assert mode == "json"
        assert by_alias
        return {"orderId": self.order_id}


@dataclass
class Counted:
    count: int


class Colour(enum.Enum):
    RED = "red"
    GREEN = "green"


class Level(enum.IntEnum):
    LOW = 1
    HIGH = 2


@dataclass
class Everything:
    flag: bool
    ratio: float
    colour: Colour
    level: Level
    ident: uuid.UUID
    tags: dict[str, list[int]]
    blob: bytes = b""
    anything: Any = None
    json_name: str = field(default="d", metadata={"json": "orderID"})


def test_payload_mapping() -> None:
    c = JsonCodec()
    stamp = datetime(2026, 10, 28, 14, 3, 22, tzinfo=UTC)
    data = c.encode(PlaceOrder("o-1", [Line("sku-1", 2)], placed_at=stamp))
    assert json.loads(data) == {
        "orderId": "o-1",
        "lines": [{"sku": "sku-1", "quantity": 2}],
        "note": None,
        "placedAt": "2026-10-28T14:03:22Z",
    }
    assert c.decode(data, PlaceOrder) == PlaceOrder("o-1", [Line("sku-1", 2)], None, stamp)
    assert c.decode(b'{"orderId":"o-2","lines":[],"extra":1}', PlaceOrder).order_id == "o-2"
    assert c.encode(None) == b"{}"
    assert c.decode(b'[1,"a"]', Echo).value == [1, "a"]
    assert c.encode(Echo({"a": 1})) == b'{"a":1}'
    assert c.decode(b'{"orderId":"o-3"}', OrderModel).order_id == "o-3"
    assert c.encode(OrderModel("o-4")) == b'{"orderId":"o-4"}'


def test_strict_decoding() -> None:
    for bad in [b"{}", b"null", b'{"count":true}', b'{"count":1.5}', b'{"count":"1"}', b"not json", b"[3]"]:
        with pytest.raises(DecodeError):
            JsonCodec().decode(bad, Counted)
    assert JsonCodec().decode(b'{"count":3}', Counted) == Counted(3)


def test_every_scalar_both_ways() -> None:
    c = JsonCodec()
    ident = uuid.UUID("12345678-1234-5678-1234-567812345678")
    value = Everything(True, 2.5, Colour.GREEN, Level.HIGH, ident, {"a": [1, 2]}, b"\x00\xff", {"k": [1, None]})
    data = c.encode(value)
    assert json.loads(data) == {
        "flag": True,
        "ratio": 2.5,
        "colour": "green",
        "level": 2,
        "ident": "12345678-1234-5678-1234-567812345678",
        "tags": {"a": [1, 2]},
        "blob": "AP8=",
        "anything": {"k": [1, None]},
        "orderID": "d",
    }
    assert c.decode(data, Everything) == value


def test_an_int_is_a_float_but_a_bool_is_neither() -> None:
    @dataclass
    class F:
        x: float

    assert JsonCodec().decode(b'{"x":2}', F) == F(2.0)
    for bad in (b'{"x":true}', b'{"x":"2"}', b'{"x":null}'):
        with pytest.raises(DecodeError):
            JsonCodec().decode(bad, F)


def test_a_bool_is_never_an_int_or_a_str() -> None:
    @dataclass
    class S:
        s: str

    for bad in (b'{"s":true}', b'{"s":1}'):
        with pytest.raises(DecodeError):
            JsonCodec().decode(bad, S)
    with pytest.raises(DecodeError):
        JsonCodec().decode(b'{"flag":1,"ratio":1}', Everything)


def test_datetime_forms() -> None:
    @dataclass
    class D:
        at: datetime

    c = JsonCodec()
    want = datetime(2026, 10, 28, 14, 3, 22, 123000, tzinfo=UTC)
    for text in ("2026-10-28T14:03:22.123Z", "2026-10-28T16:03:22.123+02:00", "2026-10-28T14:03:22.123000Z"):
        got = c.decode(json.dumps({"at": text}).encode(), D).at
        assert got == want
        assert got.utcoffset() == timedelta(0)
    assert c.decode(b'{"at":"2026-10-28T14:03:22Z"}', D).at == want.replace(microsecond=0)
    assert c.decode(b'{"at":"2026-10-28T14:03:22.123456789Z"}', D).at == want.replace(microsecond=123456)
    for bad in ("2026-10-28", "2026-10-28T14:03:22", "yesterday", "2026-13-28T14:03:22Z", 5):
        with pytest.raises(DecodeError):
            c.decode(json.dumps({"at": bad}).encode(), D)


def test_datetime_is_written_in_utc() -> None:
    c = JsonCodec()
    plus2 = timezone(timedelta(hours=2))
    assert json.loads(c.encode(Wrap(datetime(2026, 10, 28, 16, 3, 22, tzinfo=plus2))))["at"] == "2026-10-28T14:03:22Z"
    assert json.loads(c.encode(Wrap(datetime(2026, 10, 28, 14, 3, 22, 120000, tzinfo=UTC))))["at"] == (
        "2026-10-28T14:03:22.120Z"
    )
    assert json.loads(c.encode(Wrap(datetime(2026, 10, 28, 14, 3, 22, 1, tzinfo=UTC))))["at"] == (
        "2026-10-28T14:03:22.000001Z"
    )
    with pytest.raises(ValueError, match="naive"):
        c.encode(Wrap(datetime(2026, 10, 28, 14, 3, 22)))


@dataclass
class Wrap:
    at: datetime


def test_missing_null_and_defaults() -> None:
    c = JsonCodec()
    assert c.decode(b'{"orderId":"o","lines":[]}', PlaceOrder) == PlaceOrder("o", [])
    assert c.decode(b'{"orderId":"o","lines":[],"note":null}', PlaceOrder).note is None
    for bad in (b'{"orderId":null,"lines":[]}', b'{"lines":[]}', b'{"orderId":"o","lines":null}'):
        with pytest.raises(DecodeError):
            c.decode(bad, PlaceOrder)


def test_default_factories_and_init_false() -> None:
    @dataclass
    class H:
        items: list[int] = field(default_factory=list)
        derived: int = field(init=False, default=7)

    h = JsonCodec().decode(b'{"derived":9}', H)
    assert (h.items, h.derived) == ([], 7)


def test_nested_errors_name_the_path_but_never_the_value() -> None:
    with pytest.raises(DecodeError) as info:
        JsonCodec().decode(b'{"orderId":"o","lines":[{"sku":"s","quantity":"secret"}]}', PlaceOrder)
    assert "lines[0].quantity" in str(info.value)
    assert "PlaceOrder" in str(info.value)
    assert "secret" not in str(info.value)


def test_the_whole_payload() -> None:
    c = JsonCodec()
    assert c.decode(b'{"a":1}', dict) == {"a": 1}
    assert c.decode(b"[1]", list[int]) == [1]
    assert c.decode(b'[1,"a",null]', object) == [1, "a", None]
    assert c.decode(b"3", object) == 3
    for bad in (b"[1]", b"null", b""):
        with pytest.raises(DecodeError):
            c.decode(bad, dict)
    for into in (object, Echo, list[int]):
        with pytest.raises(DecodeError):
            c.decode(b"null", into)
    assert c.decode(b"", type(None)) is None
    assert c.decode(b"{}", type(None)) is None
    assert c.decode(b'{"ignored":1}', type(None)) is None
    with pytest.raises(DecodeError):
        c.decode(b"[1]", type(None))


def test_malformed_documents() -> None:
    c = JsonCodec()
    for bad in (b"\xff\xfe", b'\xef\xbb\xbf{"count":1}', b'{"count":NaN}', b'{"count":1}{', b"[" * 100000):
        with pytest.raises(DecodeError):
            c.decode(bad, Counted)
    with pytest.raises(DecodeError):
        c.decode(b"[" * 100000, object)


def test_enum_values() -> None:
    c = JsonCodec()
    assert (
        c.decode(
            b'{"flag":true,"ratio":1,"colour":"red","level":1,"ident":"12345678-1234-5678-1234-567812345678",'
            b'"tags":{}}',
            Everything,
        ).colour
        is Colour.RED
    )
    for bad in (b'"purple"', b"1", b"true"):
        with pytest.raises(DecodeError):
            c.decode(b'{"flag":true,"ratio":1,"colour":' + bad + b',"level":1,"ident":"x","tags":{}}', Everything)
    # a bool never reads as the enum member 1
    with pytest.raises(DecodeError):
        c.decode(
            b'{"flag":true,"ratio":1,"colour":"red","level":true,"ident":"12345678-1234-5678-1234-567812345678",'
            b'"tags":{}}',
            Everything,
        )


def test_camel_case_names() -> None:
    @dataclass
    class N:
        order_id: str = "a"
        url_2: str = "b"
        already_camelCase: str = "c"
        _private: str = "d"
        x: str = "e"

    assert json.loads(JsonCodec().encode(N())) == {
        "orderId": "a",
        "url2": "b",
        "alreadyCamelCase": "c",
        "_private": "d",
        "x": "e",
    }


def test_encoding_refuses_what_has_no_json_form() -> None:
    c = JsonCodec()
    with pytest.raises(TypeError):
        c.encode(object())
    with pytest.raises(TypeError):
        c.encode({1: "a"})
    with pytest.raises(ValueError, match="finite"):
        c.encode(Echo(float("nan")))


def test_text_is_written_as_utf8() -> None:
    assert JsonCodec().encode(Echo("é")) == '"é"'.encode()


def test_codec_protocol_is_satisfied() -> None:
    codec: snodra.Codec = JsonCodec()
    assert codec.decode(codec.encode(Counted(1)), Counted) == Counted(1)


def test_exact_bytes() -> None:
    value = PlaceOrder("o-1", [Line("sku-1", 2)], placed_at=datetime(2026, 10, 28, 14, 3, 22, tzinfo=UTC))
    assert JsonCodec().encode(value).decode() == (
        '{"orderId":"o-1","lines":[{"sku":"sku-1","quantity":2}],"note":null,"placedAt":"2026-10-28T14:03:22Z"}'
    )


def test_numbers_out_of_range_are_decode_errors() -> None:
    @dataclass
    class F:
        x: float

    for bad in (b'{"x":1' + b"0" * 400 + b"}", b'{"x":1e999}', b'{"x":-1e999}'):
        with pytest.raises(DecodeError):
            JsonCodec().decode(bad, F)
    for inf in (float("inf"), float("nan")):
        with pytest.raises(ValueError, match="finite"):
            JsonCodec().encode(F(inf))


def test_an_absent_optional_field_reads_as_none() -> None:
    @dataclass
    class Opt:
        a: str
        b: int | None

    assert JsonCodec().decode(b'{"a":"x"}', Opt) == Opt("x", None)
    with pytest.raises(DecodeError):
        JsonCodec().decode(b'{"b":1}', Opt)


def test_field_names_acronyms_digits_and_trailing_underscore() -> None:
    @dataclass
    class N:
        http_url: str = "a"
        line_2: str = "b"
        type_: str = "c"

    assert JsonCodec().encode(N()).decode() == '{"httpUrl":"a","line2":"b","type":"c"}'
    assert JsonCodec().decode(b'{"httpUrl":"x","line2":"y","type":"z"}', N) == N("x", "y", "z")


def test_two_fields_on_one_key_are_a_definition_error() -> None:
    @dataclass
    class Clash:
        a_b: int = 1
        aB: int = 2

    with pytest.raises(TypeError, match="aB"):
        JsonCodec().encode(Clash())
    with pytest.raises(TypeError, match="aB"):
        JsonCodec().decode(b"{}", Clash)


def test_sets_are_written_in_a_stable_order() -> None:
    @dataclass
    class S:
        tags: set[str] | frozenset[int]

    c = JsonCodec()
    assert c.encode(S({"b", "a", "c"})).decode() == '{"tags":["a","b","c"]}'
    assert c.encode(S(frozenset({3, 1, 2}))).decode() == '{"tags":[1,2,3]}'
    assert c.encode(S({1, "a"})).decode() in ('{"tags":["a",1]}', '{"tags":[1,"a"]}')  # type: ignore[arg-type]
    assert c.encode(S({1, "a"})) == c.encode(S({"a", 1}))  # type: ignore[arg-type]
