"""Identification of this client library."""

from importlib.metadata import version


def client_version() -> str:
    """The `client_version` the library announces to the hub."""
    return f"snodra-messaging-python/{version('snodra-messaging')}"
