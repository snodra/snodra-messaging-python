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

# The seven files the bump writes, and the whole workspace `uv lock` reads: every member's
# manifest, including the RabbitMQ add-on, which is a member even though no release publishes it.
core=packages/snodra-messaging/pyproject.toml
observability=packages/snodra-messaging-observability/pyproject.toml
kafka=packages/snodra-messaging-kafka/pyproject.toml
sql=packages/snodra-messaging-sql/pyproject.toml
mongo=packages/snodra-messaging-mongo/pyproject.toml
rabbitmq=packages/snodra-messaging-rabbitmq/pyproject.toml
packages="$core $observability $kafka $sql $mongo $rabbitmq"
addons="$observability $kafka $sql $mongo $rabbitmq"
versioned="$packages uv.lock"
files="$versioned pyproject.toml"
for f in "$root"/packages/*/pyproject.toml "$root"/examples/*/pyproject.toml; do
  f=${f#"$root/"}
  case $f in
    "$core" | "$observability" | "$kafka" | "$sql" | "$mongo" | "$rabbitmq") ;;
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

# records NAME: uv.lock records NAME at 9.8.7.
records() {
  got=$(awk -v n="$1" '$0 == "name = \"" n "\"" { getline; print; exit }' uv.lock)
  [ "$got" = 'version = "9.8.7"' ] && ok "uv.lock records $1 at 9.8.7" || fail "uv.lock records $1 as '$got'"
}

if sh scripts/bump-version.sh 9.8.7; then ok "it runs"; else fail "it runs"; fi

line "the core's version" "$core" 'version = "9.8.7"'
line "the observability version" "$observability" 'version = "9.8.7"'
line "the Kafka version" "$kafka" 'version = "9.8.7"'
line "the SQL version" "$sql" 'version = "9.8.7"'
line "the MongoDB version" "$mongo" 'version = "9.8.7"'
line "the RabbitMQ version" "$rabbitmq" 'version = "9.8.7"'
line "the observability pin" "$observability" '    "snodra-messaging==9.8.7",'
line "the Kafka pin" "$kafka" '    "snodra-messaging==9.8.7",'
line "the SQL pin" "$sql" '    "snodra-messaging==9.8.7",'
line "the MongoDB pin" "$mongo" '    "snodra-messaging==9.8.7",'
line "the RabbitMQ pin" "$rabbitmq" '    "snodra-messaging==9.8.7",'
for p in snodra-messaging snodra-messaging-observability snodra-messaging-kafka snodra-messaging-sql \
  snodra-messaging-mongo snodra-messaging-rabbitmq; do
  records "$p"
done
if uv lock --check >/dev/null 2>&1; then ok "the lock is the one uv resolves"; else fail "the lock is stale"; fi

# Only the version lines move: one in the core, two in each add-on (its version and its pin), and
# in the lock every added line carries 9.8.7, at least the six versions. The lock's `revision` is
# uv's own header, not a package: a newer uv writes a newer revision whenever it writes the lock,
# so it is dropped from both copies before the diff. Nothing else moves, and no other line carries
# the version.
[ "$(changed "$core")" -eq 1 ] && ok "one line in the core" || fail "$(changed "$core") lines in the core"
for addon in $addons; do
  [ "$(changed "$addon")" -eq 2 ] && ok "two lines in $addon" || fail "$(changed "$addon") lines in $addon"
done
grep -v '^revision = ' "$root/uv.lock" >"$work/lock.before"
grep -v '^revision = ' uv.lock >"$work/lock.after"
added=$(diff "$work/lock.before" "$work/lock.after" | grep '^>')
[ "$(printf '%s\n' "$added" | grep -cF 9.8.7)" -ge 6 ] && ok "six or more version lines in uv.lock" \
  || fail "$(printf '%s\n' "$added" | grep -cF 9.8.7) version lines in uv.lock"
[ "$(printf '%s\n' "$added" | grep -vcF 9.8.7)" -eq 0 ] && ok "every added uv.lock line carries 9.8.7" \
  || fail "uv.lock lines without 9.8.7: $(printf '%s\n' "$added" | grep -vF 9.8.7 | tr '\n' ' ')"
[ "$(grep -cF 9.8.7 "$core")" -eq 1 ] && ok "the core names 9.8.7 once" || fail "the core names 9.8.7 more than once"
for addon in $addons; do
  [ "$(grep -cF 9.8.7 "$addon")" -eq 2 ] && ok "$addon names 9.8.7 twice" \
    || fail "$addon names 9.8.7 $(grep -cF 9.8.7 "$addon") times"
done

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
# missing_line NAME FILE PATTERN: with PATTERN's lines removed, the bump fails and writes nothing.
missing_line() {
  name=$1 file=$2 pattern=$3
  grep -v "$pattern" "$file.good" >"$file"
  snapshot
  if sh scripts/bump-version.sh 9.8.9 2>/dev/null; then
    fail "$name"
  elif untouched; then
    ok "$name, and writes nothing"
  else
    fail "$name wrote a file"
  fi
  cp "$file.good" "$file"
}
for f in $versioned; do cp "$f" "$f.good"; done

missing_line "a missing version line in the core fails" "$core" '^version = "'
missing_line "a missing version line in the observability package fails" "$observability" '^version = "'
missing_line "a missing version line in the Kafka package fails" "$kafka" '^version = "'
missing_line "a missing version line in the SQL package fails" "$sql" '^version = "'
missing_line "a missing version line in the MongoDB package fails" "$mongo" '^version = "'
missing_line "a missing version line in the RabbitMQ package fails" "$rabbitmq" '^version = "'
missing_line "a missing observability pin fails" "$observability" '^    "snodra-messaging=='
missing_line "a missing Kafka pin fails" "$kafka" '^    "snodra-messaging=='
missing_line "a missing SQL pin fails" "$sql" '^    "snodra-messaging=='
missing_line "a missing MongoDB pin fails" "$mongo" '^    "snodra-messaging=='
missing_line "a missing RabbitMQ pin fails" "$rabbitmq" '^    "snodra-messaging=='

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
  line "the Kafka package at 0.1.0" "$kafka" 'version = "0.1.0"'
  line "the SQL package at 0.1.0" "$sql" 'version = "0.1.0"'
  line "the MongoDB package at 0.1.0" "$mongo" 'version = "0.1.0"'
  line "the RabbitMQ package at 0.1.0" "$rabbitmq" 'version = "0.1.0"'
  line "the observability pin at 0.1.0" "$observability" '    "snodra-messaging==0.1.0",'
  line "the Kafka pin at 0.1.0" "$kafka" '    "snodra-messaging==0.1.0",'
  line "the SQL pin at 0.1.0" "$sql" '    "snodra-messaging==0.1.0",'
  line "the MongoDB pin at 0.1.0" "$mongo" '    "snodra-messaging==0.1.0",'
  line "the RabbitMQ pin at 0.1.0" "$rabbitmq" '    "snodra-messaging==0.1.0",'
  awk '$0 == "name = \"snodra-messaging\"" { getline; print; exit }' uv.lock | grep -qxF 'version = "0.1.0"' \
    && ok "uv.lock at 0.1.0" || fail "uv.lock: $(awk '$0 == "name = \"snodra-messaging\"" { getline; print; exit }' uv.lock)"
else
  fail "the 0.1.0 bump"
fi

exit $failed
