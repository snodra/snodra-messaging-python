"""Identifier generation."""

import uuid


def new_id() -> str:
    """A random (version 4) UUID in its canonical text form."""
    return str(uuid.uuid4())
