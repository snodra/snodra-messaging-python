import uuid

import pytest
from conformance_service.env import settings_from_env
from snodra import ConfigurationError


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
