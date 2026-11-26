#!/bin/sh
# The packaging check of a release commit, before anything is pushed: PyPI keeps a version forever,
# so a wrong file, a wrong requirement or a version published twice cannot be taken back. Run from
# the repository root with the release commit checked out:
#
#   scripts/release-check.sh 0.1.0
#
# - vX.Y.Z names HEAD, and the tree is clean.
# - uv.lock is the lock uv resolves now.
# - PyPI has neither published distribution at X.Y.Z.
# - Only the distributions the build job lists in PUBLISHED are built. A workspace member that list
#   does not name -- an example, or a level-2 add-on without a PyPI pending trusted publisher -- is
#   never built and never reaches dist/, which then holds exactly the two published distributions,
#   the sdist and the wheel of each.
# - twine check --strict passes on them.
# - Each wheel holds exactly the files scripts/wheel-contents/<distribution>.txt names (X.Y.Z
#   substituted), its METADATA carries "Version: X.Y.Z", and the observability wheel requires
#   "snodra-messaging==X.Y.Z".
# - Both wheels install into a fresh 3.12 and a fresh 3.14 virtual environment, and the tests that
#   read no repository file pass there over a copy of the tests outside the source tree: what runs
#   is the wheel, not the checkout.
# - dist/SHA256SUMS records the four files, for the publish job.
#
# The level-2 add-ons (snodra-messaging-{kafka,mongo,rabbitmq,sql}) are workspace members but are
# not published: PyPI has pending trusted publishers only for the two distributions above. An add-on
# joins PUBLISHED, bump-version.sh and the publish jobs only after the owner has created its PyPI
# pending trusted publisher (one environment per add-on, for example pypi-kafka); until then,
# building it here would produce an artifact no publisher accepts.
#
# Needs git, uv, unzip and curl. SNODRA_PYPI_PROBE names a program standing in for the PyPI probe
# (PROJECT VERSION, printing an HTTP status), which scripts/release.test.sh uses.
set -eu

die() {
  echo "release-check: $*" >&2
  exit 1
}

version=${1:-}
grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' <<EOF || die "usage: $0 X.Y.Z"
$version
EOF

core=snodra_messaging
observability=snodra_messaging_observability
core_project=snodra-messaging
observability_project=snodra-messaging-observability
core_wheel="$core-$version-py3-none-any.whl"
observability_wheel="$observability-$version-py3-none-any.whl"
expected="$core-$version.tar.gz
$core_wheel
$observability-$version.tar.gz
$observability_wheel"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

head=$(git rev-parse HEAD)
[ "$(git rev-parse -q --verify "refs/tags/v$version^{commit}" || true)" = "$head" ] \
  || die "the tag v$version does not name HEAD"
[ -z "$(git status --porcelain)" ] || die "the tree differs from HEAD"

uv lock --check || die "uv.lock is not the lock uv resolves now"

# The distributions this repository publishes: the build job's PUBLISHED list, one per line. The
# check builds that list and nothing else, so the workspace's unpublished members (the examples and
# the level-2 add-ons) never produce a release artifact.
published=$(
  awk '
    /^[[:space:]]*PUBLISHED:[[:space:]]*[|][[:space:]]*$/ {
      match($0, /^[[:space:]]*/)
      key = RLENGTH
      found = 1
      next
    }
    found {
      match($0, /^[[:space:]]*/)
      if (RLENGTH <= key) exit
      print substr($0, RLENGTH + 1)
    }
  ' .github/workflows/ci.yml
)
[ -n "$published" ] || die "the build job of .github/workflows/ci.yml has no PUBLISHED list"

# A version PyPI has is never published again, so nothing is built until both projects answer 404.
pypi_status() {
  if [ -n "${SNODRA_PYPI_PROBE:-}" ]; then
    "$SNODRA_PYPI_PROBE" "$1" "$2"
  else
    curl -sS -o /dev/null -w '%{http_code}' "https://pypi.org/pypi/$1/$2/json"
  fi
}
for project in $core_project $observability_project; do
  status=$(pypi_status "$project" "$version") || die "the PyPI probe for $project $version failed"
  [ "$status" = 404 ] \
    || die "PyPI answered $status for $project $version: a version it has is never published again"
done

rm -rf dist
# --package, never --all-packages: only the distributions that are published build. Each contributes
# its sdist and its wheel, and anything else in dist/ fails the count below.
for project in $published; do
  uv build --package "$project" --out-dir dist >/dev/null || die "uv build failed for $project"
done
# uv writes dist/.gitignore (`*`) so a build's output is never committed: the release's dist/ holds
# its four files and nothing else.
rm -f dist/.gitignore
count=$(find dist -mindepth 1 -maxdepth 1 -type f | wc -l | tr -d ' ')
[ "$count" -eq 4 ] \
  || die "dist/ does not hold exactly the four files of $version: $(cd dist && ls | tr '\n' ' ')"
for f in $expected; do
  [ -f "dist/$f" ] || die "dist/ does not hold exactly the four files of $version: $f is missing"
done

uv run --frozen twine check --strict dist/* || die "twine check --strict failed"

# files_in WHEEL: the wheel's entries, sorted.
files_in() {
  unzip -Z1 "dist/$1" | LC_ALL=C sort
}

# metadata WHEEL: the wheel's METADATA, checked for the version, in $work/METADATA.
metadata() {
  entry=$(files_in "$1" | grep -E '\.dist-info/METADATA$') || die "dist/$1 has no .dist-info/METADATA"
  unzip -p "dist/$1" "$entry" >"$work/METADATA"
  grep -qx "Version: $version" "$work/METADATA" \
    || die "dist/$1 carries a version that is not $version"
}

# check_wheel DISTRIBUTION WHEEL: its file list is the one pinned for it.
check_wheel() {
  files_in "$2" >"$work/files"
  sed "s/X\.Y\.Z/$version/g" "scripts/wheel-contents/$1.txt" | LC_ALL=C sort >"$work/want"
  if ! cmp -s "$work/want" "$work/files"; then
    diff "$work/want" "$work/files" >&2 || true
    die "dist/$2 does not hold the files scripts/wheel-contents/$1.txt names"
  fi
  metadata "$2"
}

check_wheel "$core" "$core_wheel"
check_wheel "$observability" "$observability_wheel"
grep -qx "Requires-Dist: $core_project==$version" "$work/METADATA" \
  || die "dist/$observability_wheel does not require $core_project==$version"

# The tests of both distributions, as a consumer has them: not the checkout, and the interpreter is
# the wheel's. The pins are the workspace's test dependencies (pyproject.toml).
for python in 3.12 3.14; do
  venv="$work/venv-$python"
  uv venv -p "$python" "$venv" >/dev/null
  uv pip install --python "$venv" dist/*.whl \
    pytest==9.2.0 pytest-asyncio==1.5.0 pytest-timeout==2.4.0 opentelemetry-sdk==1.45.2 \
    || die "the wheels do not install on $python"
  "$venv/bin/python" -c 'import sys, snodra; assert snodra.__file__.startswith(sys.prefix), snodra.__file__' \
    || die "the snodra that imports on $python is not the wheel's"
  tests="$work/tests-$python"
  mkdir -p "$tests"
  cp -R packages/snodra-messaging/tests "$tests/core"
  cp -R packages/snodra-messaging-observability/tests "$tests/observability"
  find "$tests" -name __pycache__ -prune -exec rm -rf {} +
  # The workspace's [tool.pytest.ini_options], plus the copy of the tests the core's tests import.
  cat >"$tests/pytest.ini" <<'EOF'
[pytest]
asyncio_mode = auto
asyncio_default_fixture_loop_scope = function
timeout = 120
addopts = --import-mode=importlib
pythonpath = core
markers =
    repo: tests that read repository files
EOF
  (cd "$tests" && PYTHONPATH='' "$venv/bin/python" -m pytest -c pytest.ini -m "not repo" -q) \
    || die "the tests fail against the wheels on $python"
done

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}
(cd dist && for f in $expected; do sha256 "$f"; done >SHA256SUMS)

echo "release-check: $version packages as four files and installs on 3.12 and 3.14"
cat dist/SHA256SUMS
