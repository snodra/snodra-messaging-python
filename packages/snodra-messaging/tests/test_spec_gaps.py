import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


@pytest.mark.repo
def test_spec_gap_markers_and_entries_match() -> None:
    markers: set[str] = set()
    for base in ("packages", "examples"):
        for path in (ROOT / base).rglob("*.py"):
            if ".venv" in path.parts:
                continue
            markers |= set(re.findall(r"TODO\(spec-gap\): (SG-\d+)", path.read_text(encoding="utf-8")))
    entries = set(re.findall(r"^### (SG-\d+):", (ROOT / "SPEC-GAPS.md").read_text(encoding="utf-8"), re.M))
    assert markers == entries
