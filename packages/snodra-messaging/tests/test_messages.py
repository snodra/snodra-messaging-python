import pytest
import snodra
from snodra._messages import type_name


@snodra.message_type("orders.PlaceOrder")
class PlaceOrder:
    def __init__(self, order_id: str) -> None:
        self.order_id = order_id


class Line:
    pass


class Sub(PlaceOrder):
    pass


def test_type_names() -> None:
    assert type_name(PlaceOrder) == "orders.PlaceOrder"
    assert type_name(PlaceOrder("o-1")) == "orders.PlaceOrder"
    with pytest.raises(TypeError, match="message_type"):
        type_name(Line)
    with pytest.raises(TypeError, match="message_type"):
        type_name(Line())
    with pytest.raises(ValueError, match="empty"):
        snodra.message_type("")


def test_a_subclass_does_not_inherit_the_name() -> None:
    with pytest.raises(TypeError, match="message_type"):
        type_name(Sub)


def test_the_decorator_returns_the_class_itself() -> None:
    @snodra.message_type("orders.GetOrder")
    class GetOrder:
        pass

    assert type_name(GetOrder) == "orders.GetOrder"
    assert GetOrder.__name__ == "GetOrder"


def test_a_blank_or_non_text_name_is_refused() -> None:
    with pytest.raises(ValueError, match="empty"):
        snodra.message_type("  ")
    with pytest.raises(TypeError):
        snodra.message_type(3)  # type: ignore[arg-type]
