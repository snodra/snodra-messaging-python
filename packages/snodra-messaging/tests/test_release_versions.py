import tomllib
from pathlib import Path
from typing import Any, cast

import pytest

ROOT = Path(__file__).resolve().parents[3]
CORE = "snodra-messaging"


def _project(path: Path) -> dict[str, Any]:
    with path.open("rb") as file:
        return cast(dict[str, Any], tomllib.load(file)["project"])


@pytest.mark.repo
def test_every_package_carries_the_core_version_and_its_exact_pin() -> None:
    core_version = _project(ROOT / "packages" / CORE / "pyproject.toml")["version"]
    manifests = sorted((ROOT / "packages").glob("*/pyproject.toml"))
    assert manifests
    for path in manifests:
        project = _project(path)
        name = project["name"]
        assert project["version"] == core_version, f"{name} carries {project['version']}, not {core_version}"
        if name == CORE:
            continue
        pins = [requirement for requirement in project.get("dependencies", []) if requirement.startswith(f"{CORE}==")]
        assert pins == [f"{CORE}=={core_version}"], f"{name} requires {pins}, not {CORE}=={core_version}"
