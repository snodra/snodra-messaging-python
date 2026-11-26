#!/bin/sh
# Tests for release-check.sh on a scratch copy of the workspace: sh scripts/release.test.sh
#
# The scratch workspace carries the four level-2 add-ons: they are members, but no release publishes
# them until PyPI has their pending trusted publishers, so the check must build only the
# distributions ci.yml's PUBLISHED names. Slow on purpose: the case that passes builds the four
# files, installs both wheels into a fresh 3.12 and a fresh 3.14 environment and runs the tests
# there. Needs uv, git and unzip. The PyPI probe is stubbed through SNODRA_PYPI_PROBE, so the case
# where a version is already on PyPI needs no network, and the test never talks to PyPI.
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0

fail() {
  echo "FAIL $*"
  failed=1
}

ok() {
  echo "ok   $*"
}

version=9.8.7
core_wheel="snodra_messaging-$version-py3-none-any.whl"
four="snodra_messaging-$version.tar.gz
$core_wheel
snodra_messaging_observability-$version.tar.gz
snodra_messaging_observability-$version-py3-none-any.whl"
obs=packages/snodra-messaging-observability/pyproject.toml

# The workspace as committed, without what a build or a test run leaves behind: the check owns
# dist/.
repo=$work/repo
mkdir -p "$repo"
(
  cd "$root" &&
    tar -cf - --exclude ./.git --exclude ./.venv --exclude ./dist --exclude ./bin \
      --exclude ./.mypy_cache --exclude ./.ruff_cache --exclude ./.pytest_cache .
) | (cd "$repo" && tar -xf -)
find "$repo" -name __pycache__ -prune -exec rm -rf {} +

cd "$repo" || exit 1
git init -q
git config user.name test
git config user.email test@example.invalid
git add -A
git commit -qm "feat: the start"

# The check's PyPI probe: 404, or 200 for a project and version listed in $SNODRA_TEST_TAKEN.
probe=$work/pypi
: >"$work/taken"
cat >"$probe" <<'EOF'
#!/bin/sh
if grep -qx "$1 $2" "$SNODRA_TEST_TAKEN"; then echo 200; else echo 404; fi
EOF
chmod +x "$probe"
export SNODRA_PYPI_PROBE=$probe SNODRA_TEST_TAKEN=$work/taken

# The build log: a uv standing in front of the real one records every call, so the test can see
# which distributions the check builds. The add-ons are workspace members; only PUBLISHED is built.
# A pyenv or asdf shim would resolve uv through PATH and find the wrapper again: the real one is the
# first uv on PATH that is a binary, not a script.
real_uv=
old_ifs=$IFS
IFS=:
for dir in $PATH; do
  if [ ! -f "$dir/uv" ] || [ ! -x "$dir/uv" ]; then continue; fi
  if head -c 2 "$dir/uv" | grep -q '^#!'; then continue; fi
  real_uv=$dir/uv
  break
done
IFS=$old_ifs
[ -n "$real_uv" ] || { echo "FAIL no uv binary on PATH" >&2; exit 1; }
uv_log=$work/uv.log
mkdir -p "$work/bin"
cat >"$work/bin/uv" <<'EOF'
#!/bin/sh
printf '%s\n' "$*" >>"$SNODRA_TEST_UV_LOG"
exec "$SNODRA_TEST_REAL_UV" "$@"
EOF
chmod +x "$work/bin/uv"
export SNODRA_TEST_UV_LOG="$uv_log" SNODRA_TEST_REAL_UV="$real_uv"
PATH="$work/bin:$PATH"
export PATH

# sums: dist/SHA256SUMS verifies, as the publish job checks it.
sums() {
  (
    cd dist &&
      if command -v sha256sum >/dev/null 2>&1; then sha256sum -c SHA256SUMS; else shasum -a 256 -c SHA256SUMS; fi
  ) >/dev/null 2>&1
}
# refused NAME TEXT [VERSION]: the check fails, saying TEXT.
refused() {
  name=$1 want=$2 ver=${3:-$version}
  if out=$(sh scripts/release-check.sh "$ver" 2>&1); then
    fail "$name: the check passed"
  elif printf '%s' "$out" | grep -Fq "$want"; then
    ok "$name"
  else
    fail "$name: $(printf '%s' "$out" | tail -3)"
  fi
}
# retag: the working tree's changes are the commit vX.Y.Z names, as a broken release step leaves it.
retag() {
  git add -A
  git commit -q --amend --no-edit
  git tag -f -a "v$version" -m test >/dev/null
}
# restore: back to the release commit.
restore() {
  git reset -q --hard "$release"
  git tag -f -a "v$version" -m test >/dev/null
}

# The release, as the version job makes it.
if sh scripts/bump-version.sh "$version"; then ok "the bump"; else fail "the bump"; fi
git commit -qam "release: $version"
git tag -a "v$version" -m test
release=$(git rev-parse HEAD)

if out=$(sh scripts/release-check.sh "$version" 2>&1); then
  ok "the check passes"
else
  fail "the check: $(printf '%s' "$out" | tail -5)"
fi

# The four add-ons are workspace members, but none is published: the check builds only the two
# distributions PUBLISHED names, never --all-packages.
builds=$(grep '^build ' "$uv_log" || true)
want_builds='build --package snodra-messaging --out-dir dist
build --package snodra-messaging-observability --out-dir dist'
[ "$builds" = "$want_builds" ] && ok "only the published distributions are built" \
  || fail "uv build calls: $(printf '%s' "$builds" | tr '\n' ';')"

# What the publish job checks: the four names, in that order, and the files as they are now.
[ "$(awk '{ print $2 }' dist/SHA256SUMS)" = "$four" ] && ok "SHA256SUMS names the four files" \
  || fail "SHA256SUMS: $(cat dist/SHA256SUMS)"
sums && ok "the four files verify" || fail "the four files do not verify"
cp "dist/$core_wheel" "$work/wheel"
printf x >>"dist/$core_wheel"
sums && fail "a tampered wheel verifies" || ok "a tampered wheel fails the sums"
cp "$work/wheel" "dist/$core_wheel"
awk 'NR == 1 { $0 = ($0 ~ /^0/ ? "1" : "0") substr($0, 2) } { print }' dist/SHA256SUMS >"$work/SHA256SUMS"
mv "$work/SHA256SUMS" dist/SHA256SUMS
sums && fail "a tampered SHA256SUMS verifies" || ok "a tampered SHA256SUMS fails the sums"
mv dist/SHA256SUMS "$work/SHA256SUMS.bak"
sums && fail "a missing SHA256SUMS verifies" || ok "a missing SHA256SUMS fails the sums"
mv "$work/SHA256SUMS.bak" dist/SHA256SUMS

# The failures, each one a release that would have to be published to be found out.
git tag -f -a "v$version" -m test "$(git rev-parse 'HEAD^')" >/dev/null
refused "a tag on another commit" "the tag v$version does not name HEAD"
restore

printf '\n' >>README.md
refused "a tree that differs from HEAD" "the tree differs from HEAD"
git checkout -q -- README.md

printf 'EXTRA = 1\n' >packages/snodra-messaging/src/snodra/_extra.py
retag
refused "a file the wheel carries and the list does not" \
  "does not hold the files scripts/wheel-contents/snodra_messaging.txt names"
restore

# A requirement a consumer may resolve to anything newer: uv locks it, and the wheel is wrong.
sed 's/snodra-messaging==9\.8\.7/snodra-messaging>=9.8.7/' "$obs" >"$work/pyproject.toml" && mv "$work/pyproject.toml" "$obs"
uv lock >/dev/null 2>&1 || fail "uv lock with a range"
retag
refused "a requirement that is not the exact release" "does not require snodra-messaging==$version"
restore

# The tag of a version the tree does not carry.
git tag -a v9.8.8 -m test
refused "a tag for a version the tree does not carry" "does not hold exactly the four files of 9.8.8" 9.8.8
git tag -d v9.8.8 >/dev/null

# A version PyPI already has.
echo "snodra-messaging-observability $version" >>"$work/taken"
refused "a version PyPI already has" "PyPI answered 200 for snodra-messaging-observability $version"
: >"$work/taken"

[ -z "$(git status --porcelain)" ] && [ "$(git rev-parse HEAD)" = "$release" ] \
  && ok "the scratch tree is back at the release commit" || fail "the scratch tree: $(git status --porcelain)"

exit $failed
