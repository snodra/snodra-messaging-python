import uuid

import pytest
from snodra import ConfigurationError, Settings


def _refused(variable: str, **fields: object) -> None:
    values: dict[str, object] = {"service": "orders-service"} | fields
    with pytest.raises(ConfigurationError, match=variable):
        Settings(**values)  # type: ignore[arg-type]


async def test_settings_refuse_bad_values() -> None:
    with pytest.raises(ConfigurationError, match="SNODRA_SERVICE_NAME"):
        Settings.from_env({})
    _refused("SNODRA_SERVICE_NAME", service="")
    _refused("SNODRA_SERVICE_NAME", service="  ")
    _refused("SNODRA_INSTANCE_ID", instance="")
    _refused("SNODRA_MAX_CONCURRENT_HANDLERS", max_concurrent=0)
    _refused("SNODRA_MAX_CONCURRENT_HANDLERS", max_concurrent=2**32)
    _refused("SNODRA_MAX_CONCURRENT_HANDLERS", max_concurrent=True)
    _refused("SNODRA_RECONNECT_BACKOFF_MIN_MS", reconnect_backoff=(0, 5.0))
    _refused("SNODRA_RECONNECT_BACKOFF_MIN_MS", reconnect_backoff=(-1.0, 5.0))
    _refused("SNODRA_RECONNECT_BACKOFF_MAX_MS", reconnect_backoff=(2.0, 1.0))
    _refused("SNODRA_RECONNECT_BACKOFF_MAX_MS", reconnect_backoff=(0.5, float("inf")))
    _refused("SNODRA_SERVER_LOAD_FACTOR", load_factor=-1)
    _refused("SNODRA_SERVER_LOAD_FACTOR", load_factor=2**32)
    _refused("SNODRA_SERVER_TAGS", tags={"": "a"})
    _refused("SNODRA_SERVER_ADDRESS", hubs=())
    _refused("SNODRA_SERVER_ADDRESS", hubs="h:7000")
    _refused("SNODRA_SERVER_ADDRESS", hubs=("",))
    _refused("SNODRA_SERVER_ADDRESS", hubs=("static://",))
    _refused("SNODRA_SERVER_ADDRESS", hubs=("dns:///hubs.internal:7000", "h:7000"))
    _refused("SNODRA_SERVER_ADDRESS", hubs=("dns:///hubs.internal",))
    _refused("SNODRA_SERVER_ADDRESS", hubs=("dns:///hubs.internal:0",))
    _refused("SNODRA_SERVER_ADDRESS", hubs=("dns://resolver/hubs.internal:7000",))


@pytest.mark.parametrize("address", ["http://h:7000", "https://h:7000", "static://https://h:7000", "grpc://h:7000"])
def test_a_hub_address_with_another_scheme_is_refused(address: str) -> None:
    # SG-2: the accepted forms are named in the error.
    with pytest.raises(ConfigurationError) as raised:
        Settings(service="orders-service", hubs=(address,))
    text = str(raised.value)
    assert address in text
    assert "SNODRA_SERVER_ADDRESS" in text
    assert "static://host:port" in text
    assert "dns:///name:port" in text


def test_accepted_hub_addresses() -> None:
    for hubs in (("h:7000",), ("static://h:7000", "h2:7000"), ("dns:///hubs.internal:7000",), ("[::1]:7000",)):
        assert Settings(service="orders-service", hubs=hubs).hubs == hubs


def test_settings_defaults() -> None:
    s = Settings(service="orders-service")
    assert s.hubs == ("static://snodra-server:7000",)
    assert (s.max_concurrent, s.reconnect_backoff, s.load_factor, dict(s.tags)) == (256, (0.5, 5.0), 100, {})
    assert uuid.UUID(s.instance).version == 4
    assert Settings(service="orders-service").instance != s.instance  # a fresh one each time


def test_from_env() -> None:
    s = Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service"})
    assert s.service == "orders-service"
    assert uuid.UUID(s.instance).version == 4
    assert s.hubs == ("static://snodra-server:7000",)
    assert (s.max_concurrent, s.reconnect_backoff, s.load_factor, dict(s.tags)) == (256, (0.5, 5.0), 100, {})

    s = Settings.from_env(
        {
            "SNODRA_SERVICE_NAME": " orders-service ",
            "SNODRA_INSTANCE_ID": "i-7",
            "SNODRA_SERVER_ADDRESS": "hub-1:7000, static://hub-2:7000,,",
            "SNODRA_MAX_CONCURRENT_HANDLERS": "32",
            "SNODRA_RECONNECT_BACKOFF_MIN_MS": "250",
            "SNODRA_RECONNECT_BACKOFF_MAX_MS": "4000",
            "SNODRA_SERVER_LOAD_FACTOR": "0",
            "SNODRA_SERVER_TAGS": "zone=a, rack = r1 ,",
        }
    )
    assert (s.service, s.instance, s.hubs) == ("orders-service", "i-7", ("hub-1:7000", "static://hub-2:7000"))
    assert (s.max_concurrent, s.reconnect_backoff, s.load_factor) == (32, (0.25, 4.0), 0)
    assert dict(s.tags) == {"zone": "a", "rack": "r1"}

    s = Settings.from_env(
        {"SNODRA_SERVICE_NAME": "orders-service", "SNODRA_SERVER_ADDRESS": "dns:///hubs.internal:7000"}
    )
    assert s.hubs == ("dns:///hubs.internal:7000",)

    # One bound of the backoff alone keeps the other's default.
    s = Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service", "SNODRA_RECONNECT_BACKOFF_MAX_MS": "8000"})
    assert s.reconnect_backoff == (0.5, 8.0)


def test_from_env_a_blank_variable_changes_nothing() -> None:
    blank = {
        name: "  "
        for name in (
            "SNODRA_INSTANCE_ID",
            "SNODRA_SERVER_ADDRESS",
            "SNODRA_MAX_CONCURRENT_HANDLERS",
            "SNODRA_RECONNECT_BACKOFF_MIN_MS",
            "SNODRA_RECONNECT_BACKOFF_MAX_MS",
            "SNODRA_SERVER_LOAD_FACTOR",
            "SNODRA_SERVER_TAGS",
        )
    }
    s = Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service"} | blank)
    assert s.hubs == ("static://snodra-server:7000",)
    assert (s.max_concurrent, s.reconnect_backoff, s.load_factor, dict(s.tags)) == (256, (0.5, 5.0), 100, {})
    assert uuid.UUID(s.instance).version == 4
    with pytest.raises(ConfigurationError, match="SNODRA_SERVICE_NAME"):
        Settings.from_env({"SNODRA_SERVICE_NAME": " "})


def test_from_env_overrides_win() -> None:
    env = {"SNODRA_SERVICE_NAME": "orders-service", "SNODRA_MAX_CONCURRENT_HANDLERS": "32", "SNODRA_INSTANCE_ID": "i-7"}
    s = Settings.from_env(env, max_concurrent=4, reconnect_backoff=(0.1, 1.0))
    assert (s.service, s.instance, s.max_concurrent, s.reconnect_backoff) == ("orders-service", "i-7", 4, (0.1, 1.0))
    assert Settings.from_env({}, service="billing-service").service == "billing-service"


def test_from_env_reads_the_process_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNODRA_SERVICE_NAME", "orders-service")
    monkeypatch.setenv("SNODRA_INSTANCE_ID", "i-9")
    s = Settings.from_env()
    assert (s.service, s.instance) == ("orders-service", "i-9")


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("SNODRA_MAX_CONCURRENT_HANDLERS", "many"),
        ("SNODRA_MAX_CONCURRENT_HANDLERS", "-1"),
        ("SNODRA_MAX_CONCURRENT_HANDLERS", "0"),
        ("SNODRA_RECONNECT_BACKOFF_MIN_MS", "1.5"),
        ("SNODRA_RECONNECT_BACKOFF_MAX_MS", "x"),
        ("SNODRA_SERVER_LOAD_FACTOR", "-3"),
        ("SNODRA_SERVER_TAGS", "zone"),
        ("SNODRA_SERVER_TAGS", "=a"),
        ("SNODRA_SERVER_ADDRESS", "http://h:7000"),
        ("SNODRA_SERVER_ADDRESS", " , "),
    ],
)
def test_from_env_refuses_bad_values(variable: str, value: str) -> None:
    with pytest.raises(ConfigurationError, match=variable):
        Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service", variable: value})
