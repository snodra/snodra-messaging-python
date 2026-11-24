import pickle
from typing import Any, cast

import pytest
import snodra
from snodra._codec import JsonCodec
from snodra._errors import error_from_wire, error_to_wire
from snodra._hubpb import hub_pb2


def wire(code: int, message: str = "m", details: bytes = b"") -> hub_pb2.Error:
    return hub_pb2.Error(code=cast("Any", code), message=message, details=details)


def test_every_code_reads_as_itself() -> None:
    for code in range(1, 10):
        e = error_from_wire(wire(code, "m", b'{"a":1}'), "orders.PlaceOrder")
        assert e.code == code
        assert e.details == (b'{"a":1}' if code == 4 else b"")
    assert error_from_wire(wire(0, "m"), "x.Y").code == snodra.ErrorCode.HANDLER_ERROR
    assert error_from_wire(wire(42, "m"), "x.Y").code == snodra.ErrorCode.HANDLER_ERROR


def test_the_messages_the_caller_reads() -> None:  # SG-1
    t = "orders.PlaceOrder"

    def msg(c: int, m: str = "hub says") -> str:
        return error_from_wire(wire(c, m), t).message

    assert msg(1) == "No handler registered for 'orders.PlaceOrder'"
    assert msg(2) == "Timed out waiting for reply to 'orders.PlaceOrder'"
    assert msg(3) == "Handler for 'orders.PlaceOrder' disconnected"
    assert msg(8) == "Handler for 'orders.PlaceOrder' is busy: its hub queue or session buffer is full"
    assert msg(9) == "hub says"
    assert msg(9, "") == "cancelled by the hub"
    assert msg(42) == "hub says"
    for c in (4, 5, 6, 7):
        assert msg(c) == "hub says"


def test_a_hub_error_reads_as_its_message() -> None:
    e = error_from_wire(wire(4, "boom", b"[]"), "x.Y")
    assert isinstance(e, snodra.HubError)
    assert isinstance(e, snodra.SnodraError)
    assert str(e) == "boom"
    again = pickle.loads(pickle.dumps(e))
    assert (again.code, again.message, again.details) == (4, "boom", b"[]")


def test_handler_failures_map_to_their_codes() -> None:  # SG-1
    def w(exc: BaseException) -> hub_pb2.Error:
        return error_to_wire("r-1", exc, JsonCodec())

    assert (w(RuntimeError("out of stock")).code, w(RuntimeError("out of stock")).message) == (
        4,
        "RuntimeError: out of stock",
    )
    assert (w(ValueError("bad sku")).message, w(ValueError("bad sku")).details) == ("bad sku", b"[]")
    assert w(RuntimeError("a\nb\r\nc")).message == "RuntimeError: a b c"
    d = w(snodra.HandlerError("insufficient stock", details={"sku": "a"}))
    assert (d.code, d.message, d.details, d.request_id) == (4, "insufficient stock", b'{"sku":"a"}', "r-1")
    assert (
        w(snodra.Unauthenticated("no token")).code,
        w(snodra.PermissionDenied("no role")).code,
        w(snodra.InvalidRequest("bad")).code,
    ) == (5, 7, 6)
    assert w(snodra.Unauthenticated("no token")).details == b""
    nested = error_from_wire(wire(1, "x"), "stock.Reserve")
    assert (w(nested).code, w(nested).message) == (4, "HubError: No handler registered for 'stock.Reserve'")


def test_empty_messages_get_a_text() -> None:
    def w(exc: BaseException) -> str:
        return error_to_wire("r", exc, JsonCodec()).message

    assert w(snodra.Unauthenticated("")) == "unauthenticated"
    assert w(snodra.PermissionDenied("")) == "access denied"
    assert w(snodra.InvalidRequest("")) == "invalid request"
    assert w(ValueError()) == "ValueError"
    assert w(RuntimeError()) == "RuntimeError: "


def test_a_handler_error_without_details_has_none() -> None:
    e = error_to_wire("r", snodra.HandlerError("no"), JsonCodec())
    assert (e.code, e.message, e.details) == (4, "no", b"")


def test_details_the_codec_cannot_write_never_fail_the_mapping() -> None:
    e = error_to_wire("r", snodra.HandlerError("no", details={"x": object()}), JsonCodec())
    assert (e.code, e.message, e.details) == (4, "no", b"")


def test_the_class_hierarchy() -> None:
    assert issubclass(snodra.ReservedHeader, ValueError)
    assert issubclass(snodra.RegistrationClosed, RuntimeError)
    assert issubclass(snodra.ConfigurationError, ValueError)
    assert issubclass(snodra.DecodeError, ValueError)
    for cls in (
        snodra.HubError,
        snodra.NeverSent,
        snodra.OutcomeUnknown,
        snodra.LostParts,
        snodra.Unauthenticated,
        snodra.PermissionDenied,
        snodra.InvalidRequest,
        snodra.HandlerError,
        snodra.DecodeError,
    ):
        assert issubclass(cls, snodra.SnodraError)
    assert [c.name for c in snodra.ErrorCode] == [
        "NO_HANDLER",
        "TIMEOUT",
        "HANDLER_DISCONNECTED",
        "HANDLER_ERROR",
        "UNAUTHENTICATED",
        "INVALID_REQUEST",
        "PERMISSION_DENIED",
        "RESOURCE_EXHAUSTED",
        "CANCELLED",
    ]
    assert snodra.HandlerError("m", details=[1]).details == [1]
    with pytest.raises(TypeError):
        snodra.HandlerError("m", [1])  # type: ignore[call-arg]
