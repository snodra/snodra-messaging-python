import uuid

import pytest
from snodra import (
    ClientCertificate,
    ClientCredentials,
    ConfigurationError,
    RetryPolicy,
    Settings,
    TLSConfig,
    Token,
    TokenFile,
)


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
    _refused("SNODRA_SERVER_MAX_SUBSCRIPTIONS", max_subscriptions=0)
    _refused("SNODRA_SERVER_MAX_SUBSCRIPTIONS", max_subscriptions=2**32)
    _refused("SNODRA_SERVER_MAX_SUBSCRIPTIONS", max_subscriptions=True)
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


def test_the_sending_settings() -> None:
    s = Settings(service="orders-service")
    assert (s.default_timeout, s.default_retry, dict(s.priorities)) == (None, None, {})
    policy = RetryPolicy(max_attempts=5)
    s = Settings(
        service="orders-service", default_timeout=2.5, default_retry=policy, priorities={"orders.PlaceOrder": 3}
    )
    assert (s.default_timeout, s.default_retry, dict(s.priorities)) == (2.5, policy, {"orders.PlaceOrder": 3})
    _refused("SNODRA_SERVER_REQUEST_TIMEOUT_MS", default_timeout=0)
    _refused("SNODRA_SERVER_REQUEST_TIMEOUT_MS", default_timeout=-1.0)
    _refused("SNODRA_SERVER_REQUEST_TIMEOUT_MS", default_timeout=float("inf"))
    _refused("SNODRA_SERVER_REQUEST_TIMEOUT_MS", default_timeout=True)
    _refused("SNODRA_SERVER_RETRY", default_retry=3)
    _refused("SNODRA_SERVER_PRIORITIES", priorities={"orders.PlaceOrder": 2**31})
    _refused("SNODRA_SERVER_PRIORITIES", priorities={"orders.PlaceOrder": True})
    _refused("SNODRA_SERVER_PRIORITIES", priorities={"": 1})


def test_the_sending_settings_from_env() -> None:
    base = {"SNODRA_SERVICE_NAME": "orders-service"}
    s = Settings.from_env(base | {"SNODRA_SERVER_REQUEST_TIMEOUT_MS": "0", "SNODRA_SERVER_RETRY_MAX_ATTEMPTS": "5"})
    assert (s.default_timeout, s.default_retry) == (None, None)  # 0 is none; retry is off unless enabled
    s = Settings.from_env(
        base
        | {
            "SNODRA_SERVER_REQUEST_TIMEOUT_MS": "1500",
            "SNODRA_SERVER_RETRY_ENABLED": "true",
            "SNODRA_SERVER_PRIORITIES": "orders.PlaceOrder=5, orders.GetOrder=-2,",
        }
    )
    assert s.default_timeout == 1.5
    assert s.default_retry == RetryPolicy(max_attempts=3, initial_backoff=0.1, multiplier=2.0, max_backoff=2.0)
    assert dict(s.priorities) == {"orders.PlaceOrder": 5, "orders.GetOrder": -2}
    s = Settings.from_env(
        base
        | {
            "SNODRA_SERVER_RETRY_ENABLED": "TRUE",
            "SNODRA_SERVER_RETRY_MAX_ATTEMPTS": "4",
            "SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS": "50",
            "SNODRA_SERVER_RETRY_MULTIPLIER": "1.5",
            "SNODRA_SERVER_RETRY_MAX_BACKOFF_MS": "800",
        }
    )
    assert s.default_retry == RetryPolicy(max_attempts=4, initial_backoff=0.05, multiplier=1.5, max_backoff=0.8)


def test_the_streaming_settings() -> None:
    s = Settings(service="orders-service")
    assert s.stream_credit_timeout == 600.0  # the hub's default maximum stream duration
    assert Settings(service="orders-service", stream_credit_timeout=0.25).stream_credit_timeout == 0.25
    _refused("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", stream_credit_timeout=0)
    _refused("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", stream_credit_timeout=-1.0)
    _refused("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", stream_credit_timeout=float("inf"))


def test_the_subscription_limit() -> None:
    assert Settings(service="orders-service").max_subscriptions == 1024  # the hub's default per instance
    assert (
        Settings.from_env({"SNODRA_SERVICE_NAME": "s", "SNODRA_SERVER_MAX_SUBSCRIPTIONS": "7"}).max_subscriptions == 7
    )
    assert Settings(service="s", max_subscriptions=1).max_subscriptions == 1
    for bad in ("0", "-1", "many"):
        with pytest.raises(ConfigurationError, match="SNODRA_SERVER_MAX_SUBSCRIPTIONS"):
            Settings.from_env({"SNODRA_SERVICE_NAME": "s", "SNODRA_SERVER_MAX_SUBSCRIPTIONS": bad})
    _refused("SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS", stream_credit_timeout=True)


def test_the_streaming_settings_from_env() -> None:
    base = {"SNODRA_SERVICE_NAME": "orders-service"}
    assert Settings.from_env(base).stream_credit_timeout == 600.0
    assert Settings.from_env(base | {"SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS": "2000"}).stream_credit_timeout == 2.0
    for value in ("0", "-1", "soon", "1.5"):
        with pytest.raises(ConfigurationError, match="SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS"):
            Settings.from_env(base | {"SNODRA_SERVER_STREAM_CREDIT_TIMEOUT_MS": value})


def test_the_status_interval() -> None:
    assert Settings(service="orders-service").status_interval == 5.0
    assert (
        Settings.from_env({"SNODRA_SERVICE_NAME": "s", "SNODRA_SERVER_STATUS_INTERVAL_MS": "250"}).status_interval
        == 0.25
    )
    _refused("SNODRA_SERVER_STATUS_INTERVAL_MS", status_interval=0)
    _refused("SNODRA_SERVER_STATUS_INTERVAL_MS", status_interval=-1.0)
    _refused("SNODRA_SERVER_STATUS_INTERVAL_MS", status_interval=float("inf"))
    for value in ("0", "-1", "soon", "1.5"):
        with pytest.raises(ConfigurationError, match="SNODRA_SERVER_STATUS_INTERVAL_MS"):
            Settings.from_env({"SNODRA_SERVICE_NAME": "s", "SNODRA_SERVER_STATUS_INTERVAL_MS": value})


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("SNODRA_SERVER_REQUEST_TIMEOUT_MS", "-1"),
        ("SNODRA_SERVER_REQUEST_TIMEOUT_MS", "soon"),
        ("SNODRA_SERVER_RETRY_ENABLED", "yes please"),
        ("SNODRA_SERVER_RETRY_MAX_ATTEMPTS", "0"),
        ("SNODRA_SERVER_RETRY_INITIAL_BACKOFF_MS", "-5"),
        ("SNODRA_SERVER_RETRY_MULTIPLIER", "0.5"),
        ("SNODRA_SERVER_RETRY_MULTIPLIER", "inf"),
        ("SNODRA_SERVER_RETRY_MULTIPLIER", "two"),
        ("SNODRA_SERVER_RETRY_MAX_BACKOFF_MS", "x"),
        ("SNODRA_SERVER_PRIORITIES", "orders.PlaceOrder"),
        ("SNODRA_SERVER_PRIORITIES", "orders.PlaceOrder=high"),
        ("SNODRA_SERVER_PRIORITIES", "orders.PlaceOrder=99999999999"),
    ],
)
def test_the_sending_settings_from_env_refuse_bad_values(variable: str, value: str) -> None:
    # A retry variable is checked even while retry is off.
    with pytest.raises(ConfigurationError, match=variable):
        Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service", variable: value})


def test_the_event_settings() -> None:
    s = Settings(service="orders-service")
    assert (s.events_source, s.outbox_enabled, s.consumer_enabled) == ("snodra.events", True, True)
    assert (s.outbox_poll_interval, s.outbox_batch_size) == (0.2, 100)
    assert (s.outbox_max_attempts, s.outbox_lease, s.consumer_max_attempts) == (10, 120.0, 5)
    s = Settings.from_env(
        {
            "SNODRA_SERVICE_NAME": "orders-service",
            "SNODRA_EVENTS_SOURCE": "orders.events",
            "SNODRA_OUTBOX_ENABLED": "false",
            "SNODRA_OUTBOX_POLL_INTERVAL_MS": "50",
            "SNODRA_OUTBOX_BATCH_SIZE": "7",
            "SNODRA_OUTBOX_MAX_ATTEMPTS": "2",
            "SNODRA_OUTBOX_LEASE_MS": "90000",
            "SNODRA_CONSUMER_ENABLED": "false",
            "SNODRA_CONSUMER_MAX_ATTEMPTS": "3",
        }
    )
    assert (s.events_source, s.outbox_enabled, s.consumer_enabled) == ("orders.events", False, False)
    assert (s.outbox_poll_interval, s.outbox_batch_size) == (0.05, 7)
    assert (s.outbox_max_attempts, s.outbox_lease, s.consumer_max_attempts) == (2, 90.0, 3)
    assert Settings(service="orders-service", events_source="  orders.events  ").events_source == "orders.events"


def test_the_event_settings_refuse_bad_values() -> None:
    _refused("SNODRA_EVENTS_SOURCE", events_source="")
    _refused("SNODRA_EVENTS_SOURCE", events_source="  ")
    _refused("SNODRA_OUTBOX_ENABLED", outbox_enabled=1)
    _refused("SNODRA_CONSUMER_ENABLED", consumer_enabled="yes")
    _refused("SNODRA_OUTBOX_BATCH_SIZE", outbox_batch_size=0)
    _refused("SNODRA_OUTBOX_MAX_ATTEMPTS", outbox_max_attempts=0)
    _refused("SNODRA_CONSUMER_MAX_ATTEMPTS", consumer_max_attempts=0)
    _refused("SNODRA_OUTBOX_POLL_INTERVAL_MS", outbox_poll_interval=0)
    _refused("SNODRA_OUTBOX_POLL_INTERVAL_MS", outbox_poll_interval=float("inf"))
    _refused("SNODRA_OUTBOX_LEASE_MS", outbox_lease=0)
    _refused("SNODRA_OUTBOX_LEASE_MS", outbox_lease=40.0)
    for variable, value in (
        ("SNODRA_OUTBOX_ENABLED", "maybe"),
        ("SNODRA_OUTBOX_POLL_INTERVAL_MS", "0"),
        ("SNODRA_OUTBOX_BATCH_SIZE", "0"),
        ("SNODRA_OUTBOX_MAX_ATTEMPTS", "x"),
        ("SNODRA_OUTBOX_LEASE_MS", "40000"),
        ("SNODRA_CONSUMER_ENABLED", "on off"),
        ("SNODRA_CONSUMER_MAX_ATTEMPTS", "-1"),
    ):
        with pytest.raises(ConfigurationError, match=variable):
            Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service", variable: value})


def test_the_identity_and_routing_settings() -> None:
    s = Settings(service="orders-service")
    assert (s.jwks_url, s.issuer, s.roles_claim, s.forward_jwt, s.always_route_through_hub) == (
        None,
        None,
        "roles",
        True,
        False,
    )
    s = Settings(
        service="orders-service",
        jwks_url="https://issuer.test/jwks",
        issuer="https://issuer.test",
        roles_claim=" realm_access.roles ",
        forward_jwt=False,
        always_route_through_hub=True,
    )
    assert (s.jwks_url, s.issuer, s.roles_claim, s.forward_jwt, s.always_route_through_hub) == (
        "https://issuer.test/jwks",
        "https://issuer.test",
        "realm_access.roles",
        False,
        True,
    )
    assert Settings(service="orders-service", roles_claim="  ").roles_claim == "roles"
    _refused("SNODRA_ISSUER", jwks_url="https://issuer.test/jwks")
    _refused("SNODRA_ISSUER", jwks_url="https://issuer.test/jwks", issuer=" ")
    _refused("SNODRA_JWKS_URL", jwks_url=" ", issuer="https://issuer.test")
    _refused("SNODRA_JWKS_URL", jwks_url="file:///etc/jwks.json", issuer="https://issuer.test")
    _refused("SNODRA_JWKS_URL", jwks_url="issuer.test/jwks", issuer="https://issuer.test")
    _refused("SNODRA_SECURITY_ROLES_CLAIM", roles_claim=None)
    _refused("SNODRA_FORWARD_JWT", forward_jwt="yes")
    _refused("SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB", always_route_through_hub=1)


def test_the_identity_and_routing_settings_from_env() -> None:
    base = {"SNODRA_SERVICE_NAME": "orders-service"}
    s = Settings.from_env(
        base
        | {
            "SNODRA_JWKS_URL": " http://127.0.0.1:9000/jwks ",
            "SNODRA_ISSUER": "http://127.0.0.1:9000/issuer",
            "SNODRA_SECURITY_ROLES_CLAIM": "scope",
            "SNODRA_FORWARD_JWT": "false",
            "SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB": "TRUE",
        }
    )
    assert (s.jwks_url, s.issuer, s.roles_claim, s.forward_jwt, s.always_route_through_hub) == (
        "http://127.0.0.1:9000/jwks",
        "http://127.0.0.1:9000/issuer",
        "scope",
        False,
        True,
    )
    blank = {
        n: " "
        for n in (
            "SNODRA_JWKS_URL",
            "SNODRA_ISSUER",
            "SNODRA_SECURITY_ROLES_CLAIM",
            "SNODRA_FORWARD_JWT",
            "SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB",
        )
    }
    s = Settings.from_env(base | blank)
    assert (s.jwks_url, s.issuer, s.roles_claim, s.forward_jwt, s.always_route_through_hub) == (
        None,
        None,
        "roles",
        True,
        False,
    )


@pytest.mark.parametrize(
    ("env", "variable"),
    [
        ({"SNODRA_JWKS_URL": "http://127.0.0.1:9000/jwks"}, "SNODRA_ISSUER"),
        ({"SNODRA_FORWARD_JWT": "maybe"}, "SNODRA_FORWARD_JWT"),
        ({"SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB": "2"}, "SNODRA_SERVER_ALWAYS_ROUTE_THROUGH_HUB"),
    ],
)
def test_the_identity_settings_from_env_refuse_bad_values(env: dict[str, str], variable: str) -> None:
    with pytest.raises(ConfigurationError, match=variable):
        Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service"} | env)


def test_the_credential_settings() -> None:
    s = Settings(service="orders-service", credentials=Token(" t-1 "))
    assert (s.credentials, s.tls, s.client_certificate) == (Token("t-1"), None, None)
    s = Settings(service="orders-service", client_certificate=ClientCertificate("c.pem", "k.pem"))
    assert s.tls == TLSConfig()  # a client certificate implies TLS
    s = Settings(service="orders-service", tls=TLSConfig(" ca.pem ", " hub.test "))
    assert s.tls == TLSConfig("ca.pem", "hub.test")
    _refused("SNODRA_SERVER_TOKEN", credentials=Token("  "))
    _refused("SNODRA_SERVER_TOKEN_FILE", credentials=TokenFile(" "))
    _refused("SNODRA_SERVER_OAUTH2_TOKEN_URI", credentials=ClientCredentials("idp.test/token", "c", "s"))
    _refused("SNODRA_SERVER_OAUTH2_TOKEN_URI", credentials=ClientCredentials("ftp://idp.test/token", "c", "s"))
    _refused("SNODRA_SERVER_OAUTH2_CLIENT_ID", credentials=ClientCredentials("https://idp.test/token", " ", "s"))
    _refused("SNODRA_SERVER_OAUTH2_CLIENT_SECRET", credentials=ClientCredentials("https://idp.test/token", "c"))
    _refused("SNODRA_SERVER_TLS", tls="on")
    _refused("SNODRA_SERVER_TLS_CLIENT_CERT", client_certificate=ClientCertificate(" ", "k.pem"))
    _refused("SNODRA_SERVER_TLS_CLIENT_KEY", client_certificate=ClientCertificate("c.pem", ""))


def test_the_credential_settings_from_env() -> None:
    base = {"SNODRA_SERVICE_NAME": "orders-service"}
    assert Settings.from_env(base | {"SNODRA_SERVER_TOKEN": "t-1"}).credentials == Token("t-1")
    s = Settings.from_env(base | {"SNODRA_SERVER_TOKEN": "t-1", "SNODRA_SERVER_TOKEN_FILE": "/run/token"})
    assert s.credentials == TokenFile("/run/token")  # the file wins
    s = Settings.from_env(
        base
        | {
            "SNODRA_SERVER_OAUTH2_TOKEN_URI": "https://idp.test/token",
            "SNODRA_SERVER_OAUTH2_CLIENT_SECRET_FILE": "/run/secret",
            "SNODRA_SERVER_OAUTH2_SCOPE": "hub",
            "SNODRA_SERVER_OAUTH2_CA": "/run/idp-ca.pem",
        }
    )
    assert s.credentials == ClientCredentials(
        "https://idp.test/token",
        "orders-service",  # the service name by default
        client_secret_file="/run/secret",
        scope="hub",
        ca_file="/run/idp-ca.pem",
    )
    s = Settings.from_env(
        base
        | {
            "SNODRA_SERVER_OAUTH2_TOKEN_URI": "https://idp.test/token",
            "SNODRA_SERVER_OAUTH2_CLIENT_ID": "orders-client",
            "SNODRA_SERVER_OAUTH2_CLIENT_SECRET": "s",
        }
    )
    assert s.credentials == ClientCredentials("https://idp.test/token", "orders-client", "s")
    s = Settings.from_env(
        base | {"SNODRA_SERVER_TLS": "true", "SNODRA_SERVER_TLS_CA": "ca.pem", "SNODRA_SERVER_TLS_SERVER_NAME": "hub"}
    )
    assert (s.tls, s.client_certificate) == (TLSConfig("ca.pem", "hub"), None)
    s = Settings.from_env(base | {"SNODRA_SERVER_TLS_CLIENT_CERT": "c.pem", "SNODRA_SERVER_TLS_CLIENT_KEY": "k.pem"})
    assert (s.tls, s.client_certificate) == (TLSConfig(), ClientCertificate("c.pem", "k.pem"))
    assert Settings.from_env(base | {"SNODRA_SERVER_TLS": "false"}).tls is None


@pytest.mark.parametrize(
    ("env", "variable"),
    [
        ({"SNODRA_SERVER_OAUTH2_CLIENT_ID": "c"}, "SNODRA_SERVER_OAUTH2_TOKEN_URI"),
        ({"SNODRA_SERVER_OAUTH2_SCOPE": "hub"}, "SNODRA_SERVER_OAUTH2_TOKEN_URI"),
        ({"SNODRA_SERVER_TLS_CA": "ca.pem"}, "SNODRA_SERVER_TLS"),
        ({"SNODRA_SERVER_TLS_SERVER_NAME": "hub"}, "SNODRA_SERVER_TLS"),
        ({"SNODRA_SERVER_TLS": "yes please"}, "SNODRA_SERVER_TLS"),
        ({"SNODRA_SERVER_TLS_CLIENT_CERT": "c.pem"}, "SNODRA_SERVER_TLS_CLIENT_KEY"),
        ({"SNODRA_SERVER_TLS_CLIENT_KEY": "k.pem"}, "SNODRA_SERVER_TLS_CLIENT_CERT"),
        (
            {"SNODRA_SERVER_TLS": "false", "SNODRA_SERVER_TLS_CLIENT_CERT": "c", "SNODRA_SERVER_TLS_CLIENT_KEY": "k"},
            "SNODRA_SERVER_TLS",
        ),
        (
            {"SNODRA_SERVER_TOKEN": "t", "SNODRA_SERVER_OAUTH2_TOKEN_URI": "https://idp.test/t"},
            "SNODRA_SERVER_OAUTH2_TOKEN_URI",
        ),
        (
            {"SNODRA_SERVER_TOKEN": "t", "SNODRA_SERVER_TLS_CLIENT_CERT": "c", "SNODRA_SERVER_TLS_CLIENT_KEY": "k"},
            "SNODRA_SERVER_TLS_CLIENT_CERT",
        ),
        ({"SNODRA_SERVER_OAUTH2_TOKEN_URI": "https://idp.test/t"}, "SNODRA_SERVER_OAUTH2_CLIENT_SECRET"),
    ],
)
def test_the_credential_settings_from_env_refuse_bad_values(env: dict[str, str], variable: str) -> None:
    with pytest.raises(ConfigurationError, match=variable):
        Settings.from_env({"SNODRA_SERVICE_NAME": "orders-service"} | env)
