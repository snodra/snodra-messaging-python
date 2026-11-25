import uuid

import pytest
from conformance_service.env import settings_from_env
from snodra import ClientCertificate, ClientCredentials, ConfigurationError, TLSConfig, Token


def test_env_maps_onto_settings() -> None:
    s = settings_from_env(
        {
            "CONFORMANCE_SERVICE_NAME": "conformance-python",
            "CONFORMANCE_INSTANCE_ID": "i-1",
            "CONFORMANCE_HUBS": "127.0.0.1:7000, 127.0.0.1:7100",
            "CONFORMANCE_CAPACITY": "4",
        }
    )
    assert (s.service, s.instance, s.hubs, s.max_concurrent) == (
        "conformance-python",
        "i-1",
        ("127.0.0.1:7000", "127.0.0.1:7100"),
        4,
    )


def test_absent_variables_are_not_configured() -> None:
    s = settings_from_env({"CONFORMANCE_SERVICE_NAME": "conformance-python", "CONFORMANCE_INSTANCE_ID": " "})
    assert s.max_concurrent == 64
    assert uuid.UUID(s.instance).version == 4
    assert s.hubs == ("static://snodra-server:7000",)
    with pytest.raises(ConfigurationError, match="SNODRA_SERVICE_NAME"):
        settings_from_env({})


def test_the_service_routes_every_call_through_the_hub_and_reads_the_callers_issuer() -> None:
    base = {"CONFORMANCE_SERVICE_NAME": "conformance-python"}
    s = settings_from_env(base)
    assert (s.always_route_through_hub, s.jwks_url, s.issuer) == (True, None, None)
    s = settings_from_env(
        base
        | {
            "CONFORMANCE_JWKS_URL": "http://127.0.0.1:9000/jwks",
            "CONFORMANCE_ISSUER": "http://127.0.0.1:9000/conformance",
        }
    )
    assert (s.jwks_url, s.issuer) == ("http://127.0.0.1:9000/jwks", "http://127.0.0.1:9000/conformance")
    with pytest.raises(ConfigurationError, match="SNODRA_ISSUER"):  # the URL without the issuer
        settings_from_env(base | {"CONFORMANCE_JWKS_URL": "http://127.0.0.1:9000/jwks"})


def test_the_streaming_credit_bound() -> None:
    base = {"CONFORMANCE_SERVICE_NAME": "conformance-python"}
    assert settings_from_env(base).stream_credit_timeout == 600.0
    assert settings_from_env(base | {"CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS": "2000"}).stream_credit_timeout == 2.0
    for bad in ("0", "-1", "soon", "1.5"):
        with pytest.raises(ConfigurationError, match="CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS"):
            settings_from_env(base | {"CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS": bad})


def test_the_subscription_limit() -> None:
    base = {"CONFORMANCE_SERVICE_NAME": "conformance-python"}
    assert settings_from_env(base).max_subscriptions == 1024
    assert settings_from_env(base | {"CONFORMANCE_MAX_SUBSCRIPTIONS": "2"}).max_subscriptions == 2
    for bad in ("0", "-1", "soon", "1.5"):
        with pytest.raises(ConfigurationError, match="CONFORMANCE_MAX_SUBSCRIPTIONS"):
            settings_from_env(base | {"CONFORMANCE_MAX_SUBSCRIPTIONS": bad})


def test_the_service_reads_its_credentials() -> None:
    base = {"CONFORMANCE_SERVICE_NAME": "conformance-python"}
    s = settings_from_env(base)
    assert (s.credentials, s.tls, s.client_certificate) == (None, None, None)
    assert settings_from_env(base | {"CONFORMANCE_HUB_TOKEN": "t-1"}).credentials == Token("t-1")
    s = settings_from_env(
        base
        | {
            "CONFORMANCE_OAUTH2_TOKEN_URL": "http://127.0.0.1:9000/token",
            "CONFORMANCE_OAUTH2_CLIENT_ID": "conformance-python",
            "CONFORMANCE_OAUTH2_CLIENT_SECRET": "s",
        }
    )
    assert s.credentials == ClientCredentials("http://127.0.0.1:9000/token", "conformance-python", "s")
    s = settings_from_env(base | {"CONFORMANCE_TLS_CA": "/tmp/ca.pem", "CONFORMANCE_TLS_SERVER_NAME": "hub.test"})
    assert (s.tls, s.client_certificate) == (TLSConfig("/tmp/ca.pem", "hub.test"), None)
    s = settings_from_env(
        base
        | {
            "CONFORMANCE_TLS_CA": "/tmp/ca.pem",
            "CONFORMANCE_TLS_SERVER_NAME": "hub.test",
            "CONFORMANCE_TLS_CLIENT_CERT": "/tmp/c.pem",
            "CONFORMANCE_TLS_CLIENT_KEY": "/tmp/k.pem",
        }
    )
    assert (s.tls, s.client_certificate) == (
        TLSConfig("/tmp/ca.pem", "hub.test"),
        ClientCertificate("/tmp/c.pem", "/tmp/k.pem"),
    )
