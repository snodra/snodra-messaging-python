#!/bin/sh
# Writes a version into every file of this repository that carries it. Run from the repository root:
#
#   scripts/bump-version.sh 0.1.0
#
# - packages/snodra-messaging/pyproject.toml: the distribution's version.
# - packages/snodra-messaging-{observability,kafka,sql,mongo,rabbitmq}/pyproject.toml: each
#   version, and each requirement on snodra-messaging becomes ==X.Y.Z. A consumer resolves the
#   core through that line, so it must name a release.
# - uv.lock: rewritten by `uv lock`, which records all six versions.
#
# Every line is found before anything is written, and a failure puts the files back: a release
# commit is the whole bump or none of it.
set -eu

version=${1:-}
grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' <<EOF || { echo "usage: $0 X.Y.Z" >&2; exit 2; }
$version
EOF

core=packages/snodra-messaging/pyproject.toml
observability=packages/snodra-messaging-observability/pyproject.toml
kafka=packages/snodra-messaging-kafka/pyproject.toml
sql=packages/snodra-messaging-sql/pyproject.toml
mongo=packages/snodra-messaging-mongo/pyproject.toml
rabbitmq=packages/snodra-messaging-rabbitmq/pyproject.toml
addons="$observability $kafka $sql $mongo $rabbitmq"
files="$core $addons"

work=$(mktemp -d)
# A failed rewrite leaves the files it already prepared as .tmp: they go too, so a failed run writes
# nothing at all.
cleanup() {
  rm -rf "$work"
  for f in $files; do rm -f "$f.tmp"; done
}
trap cleanup EXIT

# rewrite FILE COUNT AWK: FILE.tmp carries the program's changes; the program adds one to done for
# each replacement it made, and must have made COUNT of them.
rewrite() {
  rm -f "$1.tmp"
  if ! awk -v v="$version" -v count="$2" "$3"' END { exit !((done + 0) == (count + 0)) }' "$1" >"$1.tmp"; then
    rm -f "$1.tmp"
    echo "bump-version: $1 no longer has the $2 line(s) the version goes into" >&2
    exit 1
  fi
}

# shellcheck disable=SC2016 # the quotes are awk's, not the shell's
rewrite "$core" 1 '
  /^version = "/ { $0 = "version = \"" v "\""; done = done + 1 }
  { print }'

# Every add-on carries its own version and its exact requirement on the core.
for addon in $addons; do
  # shellcheck disable=SC2016 # the quotes are awk's, not the shell's
  rewrite "$addon" 2 '
    /^version = "/ { $0 = "version = \"" v "\""; done = done + 1 }
    /^[[:space:]]*"snodra-messaging==/ { sub(/snodra-messaging==[^"]*/, "snodra-messaging==" v); done = done + 1 }
    { print }'
done

# The six files are rewritten together: a failure above leaves all of them as they were. `uv lock`
# then records the versions; if it fails, the files go back.
for f in $files; do cp "$f" "$work/$(basename "$f").before"; done
for f in $files; do mv "$f.tmp" "$f"; done
if ! uv lock; then
  for f in $files; do cp "$work/$(basename "$f").before" "$f"; done
  echo "bump-version: uv lock failed, so the version was put back" >&2
  exit 1
fi
