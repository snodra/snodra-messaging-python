import importlib.metadata
import re


def test_core_requires_only_grpc_protobuf_cryptography() -> None:
    requires = importlib.metadata.requires("snodra-messaging") or []
    names = set()
    for r in requires:
        m = re.match(r"[A-Za-z0-9_.-]+", r)
        assert m is not None
        names.add(m.group(0).lower())
    assert names == {"grpcio", "protobuf", "cryptography"}
