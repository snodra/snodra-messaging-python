import re
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
ALLOWED_PREFIXES = (
    "https://snodra.damoreira.com/",
    "https://github.com/snodra/",
    "https://pypi.org/",
    "https://img.shields.io/",
)


@pytest.mark.repo
def test_readme_follows_the_library_pattern() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    # Centred header block checks
    assert '<img alt="Snodra Messaging for Python mascot" src="docs/assets/brand/svg/snodra-messaging.svg"' in readme
    assert '<h1 align="center">Snodra Messaging (Python)</h1>' in readme
    assert "https://github.com/snodra/snodra-messaging-python/actions/workflows/ci.yml" in readme
    assert "https://img.shields.io/pypi/v/snodra-messaging" in readme
    assert "https://snodra.damoreira.com/docs/messaging/python/" in readme
    assert "Apache-2.0" in readme

    # Section headings: exactly the required sections
    sections = re.findall(r"^## (.+)$", readme, re.MULTILINE)
    assert sections == [
        "What it is",
        "Install",
        "A handler",
        "An event",
        "Documentation",
        "Contributing",
        "Releases",
        "License",
    ]


@pytest.mark.repo
def test_readme_links_are_public() -> None:
    readmes = [
        ROOT / "README.md",
        ROOT / "packages" / "snodra-messaging" / "README.md",
        ROOT / "packages" / "snodra-messaging-observability" / "README.md",
    ]
    for path in readmes:
        text = path.read_text(encoding="utf-8")
        links = re.findall(r"https?://[^\s\)\]\"\>]+", text)
        assert links, f"no links found in {path}"
        for link in links:
            assert any(link.startswith(prefix) for prefix in ALLOWED_PREFIXES), (
                f"link {link} in {path} does not start with an allowed prefix {ALLOWED_PREFIXES}"
            )


def _without_fmt(code: str) -> str:
    """The code without the formatter directives that keep an example's blank lines."""
    return "\n".join(line for line in code.splitlines() if line.strip() not in ("# fmt: off", "# fmt: on"))


@pytest.mark.repo
def test_every_python_block_of_the_readme_is_run() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    service_py = (ROOT / "examples" / "quickstart" / "src" / "orders_quickstart" / "service.py").read_text(
        encoding="utf-8"
    )

    snippets: dict[str, str] = {}
    pattern = re.compile(r"^[ \t]*# snippet:(\w+) begin\n(.*?)\n[ \t]*# snippet:\1 end", re.MULTILINE | re.DOTALL)
    for match in pattern.finditer(service_py):
        name = match.group(1)
        snippets[name] = _without_fmt(textwrap.dedent(match.group(2)).strip())

    assert snippets, "no snippet regions found in service.py"

    python_blocks = [
        textwrap.dedent(block).strip() for block in re.findall(r"```python\n(.*?)\n```", readme, re.DOTALL)
    ]
    assert python_blocks, "no python blocks found in README.md"
    for block in python_blocks:
        assert block in snippets.values(), f"README python block not found in service.py snippets:\n{block}"


@pytest.mark.repo
def test_install_lines_name_no_version() -> None:
    readmes = [
        ROOT / "README.md",
        ROOT / "packages" / "snodra-messaging" / "README.md",
        ROOT / "packages" / "snodra-messaging-observability" / "README.md",
    ]
    pip_lines: list[str] = []
    for path in readmes:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("pip install"):
                pip_lines.append(line)
                assert "==" not in line, f"version pinned in {path}: {line}"

    assert any("pip install snodra-messaging" in line for line in pip_lines)
    assert any("pip install snodra-messaging-observability" in line for line in pip_lines)
