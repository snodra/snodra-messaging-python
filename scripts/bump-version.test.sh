#!/bin/sh
# Tests for bump-version.sh on a copy of the workspace's manifests: sh scripts/bump-version.test.sh
#
# The bump ends with `uv lock`, so the copy carries the lock and every workspace member's manifest.
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

# The three files the bump writes, and the whole workspace `uv lock` reads: every member's manifest,
# including the level-2 add-ons, which are members even though no release publishes them.
core=packages/snodra-messaging/pyproject.toml
observability=packages/snodra-messaging-observability/pyproject.toml
versioned="$core $observability uv.lock"
files="$versioned pyproject.toml"
for f in "$root"/packages/*/pyproject.toml "$root"/examples/*/pyproject.toml; do
  f=${f#"$root/"}
  case $f in
    "$core" | "$observability") ;;
    *) files="$files $f" ;;
  esac
done

for f in $files; do
  mkdir -p "$work/$(dirname "$f")"
  cp "$root/$f" "$work/$f"
done
mkdir -p "$work/scripts"
cp "$root/scripts/bump-version.sh" "$work/scripts/"
cd "$work" || exit 1

# line NAME FILE TEXT: FILE has TEXT as a line.
line() {
  if grep -qxF "$3" "$2"; then ok "$1"; else fail "$1: '$3' is not a line of $2"; fi
}

# changed FILE: the lines FILE has more than the repository's copy.
changed() {
  diff "$root/$1" "$1" | grep -c '^>'
}

if sh scripts/bump-version.sh 9.8.7; then ok "it runs"; else fail "it runs"; fi

line "the core's version" "$core" 'version = "9.8.7"'
line "the observability version" "$observability" 'version = "9.8.7"'
line "the observability pin" "$observability" '    "snodra-messaging==9.8.7",'
for p in snodra-messaging snodra-messaging-observability; do
  got=$(awk -v n="$p" '$0 == "name = \"" n "\"" { getline; print; exit }' uv.lock)
  [ "$got" = 'version = "9.8.7"' ] && ok "uv.lock records $p at 9.8.7" || fail "uv.lock records $p as '$got'"
done
if uv lock --check >/dev/null 2>&1; then ok "the lock is the one uv resolves"; else fail "the lock is stale"; fi

# Only the version lines move: one in the core, two in the observability package (its version and
# its pin), and in the lock every added line carries 9.8.7, at least the two versions. The lock's
# `revision` is uv's own header, not a package: a newer uv writes a newer revision whenever it
# writes the lock, so it is dropped from both copies before the diff. Nothing else moves, and no
# other line carries the version.
[ "$(changed "$core")" -eq 1 ] && ok "one line in the core" || fail "$(changed "$core") lines in the core"
[ "$(changed "$observability")" -eq 2 ] && ok "two lines in the observability package" \
  || fail "$(changed "$observability") lines in the observability package"
grep -v '^revision = ' "$root/uv.lock" >"$work/lock.before"
grep -v '^revision = ' uv.lock >"$work/lock.after"
added=$(diff "$work/lock.before" "$work/lock.after" | grep '^>')
[ "$(printf '%s\n' "$added" | grep -cF 9.8.7)" -ge 2 ] && ok "two or more version lines in uv.lock" \
  || fail "$(printf '%s\n' "$added" | grep -cF 9.8.7) version lines in uv.lock"
[ "$(printf '%s\n' "$added" | grep -vcF 9.8.7)" -eq 0 ] && ok "every added uv.lock line carries 9.8.7" \
  || fail "uv.lock lines without 9.8.7: $(printf '%s\n' "$added" | grep -vF 9.8.7 | tr '\n' ' ')"
[ "$(grep -cF 9.8.7 "$core")" -eq 1 ] && ok "the core names 9.8.7 once" || fail "the core names 9.8.7 more than once"
[ "$(grep -cF 9.8.7 "$observability")" -eq 2 ] && ok "the observability package names 9.8.7 twice" \
  || fail "the observability package names 9.8.7 $(grep -cF 9.8.7 "$observability") times"

# Twice is the same as once: the release of a version already written still runs.
for f in $versioned; do cp "$f" "$f.once"; done
sh scripts/bump-version.sh 9.8.7 || fail "a second run"
same=yes
for f in $versioned; do cmp -s "$f" "$f.once" || same=no; done
[ "$same" = yes ] && ok "a second run changes nothing" || fail "a second run changed a file"

# A failure writes nothing: the files are as they were when the script started.
snapshot() {
  for f in $versioned; do cp "$f" "$f.before"; done
}
untouched() {
  for f in $versioned; do cmp -s "$f" "$f.before" || return 1; done
}
for f in $versioned; do cp "$f" "$f.good"; done

sed '/^version = "/d' "$core.good" >"$core"
snapshot
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then
  fail "a missing version line in the core fails"
elif untouched; then
  ok "a missing version line in the core fails, and writes nothing"
else
  fail "a missing version line in the core wrote a file"
fi
cp "$core.good" "$core"

sed '/^version = "/d' "$observability.good" >"$observability"
snapshot
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then
  fail "a missing version line in the observability package fails"
elif untouched; then
  ok "a missing version line in the observability package fails, and writes nothing"
else
  fail "a missing version line in the observability package wrote a file"
fi
cp "$observability.good" "$observability"

sed '/^    "snodra-messaging==/d' "$observability.good" >"$observability"
snapshot
if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then
  fail "a missing pin fails"
elif untouched; then
  ok "a missing pin fails, and writes nothing"
else
  fail "a missing pin wrote a file"
fi
cp "$observability.good" "$observability"

[ "$(diff "$root/$core" "$core" | grep -c '^>')" -eq 1 ] && ok "the files are back" || fail "the files are not back"

refused=yes
for v in 1.x 1.2 01.2.3 1.2.3-rc1 v1.2.3 1.2.3.4 ' 1.2.3' 1.2.3-; do
  snapshot
  if sh scripts/bump-version.sh "$v" 2>/dev/null; then
    fail "the version '$v' is refused"
    refused=no
  fi
  untouched || fail "the version '$v' wrote a file"
done
[ $refused = yes ] && ok "a malformed version is refused, and writes nothing"

# The real release: 0.1.0, the version this repository ships first.
if sh scripts/bump-version.sh 0.1.0; then
  line "the core at 0.1.0" "$core" 'version = "0.1.0"'
  line "the observability package at 0.1.0" "$observability" 'version = "0.1.0"'
  line "the pin at 0.1.0" "$observability" '    "snodra-messaging==0.1.0",'
  awk '$0 == "name = \"snodra-messaging\"" { getline; print; exit }' uv.lock | grep -qxF 'version = "0.1.0"' \
    && ok "uv.lock at 0.1.0" || fail "uv.lock: $(awk '$0 == "name = \"snodra-messaging\"" { getline; print; exit }' uv.lock)"
else
  fail "the 0.1.0 bump"
fi

exit $failed
