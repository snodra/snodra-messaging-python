import importlib.metadata
import re

import snodra
from snodra._ids import new_id
from snodra._meta import client_version

UUID_V4 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def test_the_import_package_is_snodra_and_the_distribution_snodra_messaging() -> None:
    assert snodra.__name__ == "snodra"
    assert snodra.__version__ == importlib.metadata.version("snodra-messaging")
    assert re.fullmatch(r"\d+\.\d+\.\d+", snodra.__version__)


def test_client_version() -> None:  # never a literal version: a release bumps it
    assert client_version() == f"snodra-messaging-python/{snodra.__version__}"


def test_new_id_is_a_random_v4_uuid() -> None:
    ids = [new_id() for _ in range(1000)]
    assert len(set(ids)) == 1000
    assert all(UUID_V4.match(i) for i in ids)
