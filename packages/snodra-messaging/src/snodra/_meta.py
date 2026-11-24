"""Identification of this client library."""

import functools
from importlib.metadata import version


@functools.cache
def _version() -> str:
    return version("snodra-messaging")  # reads the installed metadata: once


def client_version() -> str:
    """The `client_version` the library announces to the hub."""
    return f"snodra-messaging-python/{_version()}"
