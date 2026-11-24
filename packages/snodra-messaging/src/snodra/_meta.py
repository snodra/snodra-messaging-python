"""Identification of this client library."""

from snodra import __version__


def client_version() -> str:
    """The `client_version` the library announces to the hub."""
    return f"snodra-messaging-python/{__version__}"
