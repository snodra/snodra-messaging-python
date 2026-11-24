#!/usr/bin/env bash
# Runs the Snodra conformance runner against examples/conformance-service.
#
#   scripts/conformance.sh [--hubs 1|2] [--report text|json] [runner args...]
#
# Syncs the workspace (uv sync --frozen --all-packages), writes bin/conformance-service (the
# service, run with the workspace's Python), fetches the runner (snodra-conformance
# $runner_version) into bin/ unless SNODRA_CONFORMANCE_BIN names one (the download is checked
# against the sha256 pinned in scripts/conformance-runner.sha256), starts
# snodra/snodra-server:${SNODRA_SERVER_VERSION} (default 0.6.0) on 127.0.0.1:7000 (and
# 127.0.0.1:7100 with --hubs 2), runs the runner with the remaining arguments (for example
# --only L1-REGISTER-FIRST) and removes the hubs on exit.
#
# --report json prints the runner's JSON report instead of the text one. With
# SNODRA_CONFORMANCE_REPORT=<file> the runner's report is also written to that file; the exit
# code is the runner's either way.
#
# SNODRA_HUB_PORT moves the hubs to other local ports (the second hub is 100 above): on macOS
# the AirPlay Receiver holds port 7000, so run there with, say, SNODRA_HUB_PORT=17000.
set -euo pipefail

runner_version=v0.3.0
server_image="snodra/snodra-server:${SNODRA_SERVER_VERSION:-0.6.0}"
base_port="${SNODRA_HUB_PORT:-7000}"
port_error="SNODRA_HUB_PORT must be a port number from 1 to 65435, got '$base_port'"
if ! [[ "$base_port" =~ ^0*[0-9]{1,5}$ ]]; then
  echo "$port_error" >&2
  exit 2
fi
base_port=$((10#$base_port)) # base 10: bash arithmetic reads a leading zero as octal
if ((base_port < 1 || base_port > 65435)); then
  echo "$port_error" >&2
  exit 2
fi

hubs=1
if [[ "${1:-}" == "--hubs" ]]; then
  hubs="${2:-}"
  shift 2 || shift
fi
case "$hubs" in
  1 | 2) ;;
  *)
    echo "--hubs takes 1 or 2" >&2
    exit 2
    ;;
esac

# The runner takes --report once: pull it out of the arguments, text by default.
report=text
args=()
while (($# > 0)); do
  case "$1" in
    --report)
      report="${2:-}"
      shift 2 || shift
      ;;
    --report=*)
      report="${1#--report=}"
      shift
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done
case "$report" in
  text | json) ;;
  *)
    echo "--report takes text or json" >&2
    exit 2
    ;;
esac

cd "$(dirname "$0")/.."
uv sync --frozen --all-packages
mkdir -p bin
cat >bin/conformance-service <<SERVICE
#!/bin/sh
PYTHONUNBUFFERED=1 exec "$PWD/.venv/bin/python" -m conformance_service "\$@"
SERVICE
chmod +x bin/conformance-service

runner="${SNODRA_CONFORMANCE_BIN:-}"
if [[ -z "$runner" ]]; then
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) target=x86_64-unknown-linux-musl ;;
    Linux-aarch64 | Linux-arm64) target=aarch64-unknown-linux-musl ;;
    Darwin-x86_64) target=x86_64-apple-darwin ;;
    Darwin-arm64) target=aarch64-apple-darwin ;;
    *)
      echo "no runner binary for $(uname -s)-$(uname -m): set SNODRA_CONFORMANCE_BIN" >&2
      exit 2
      ;;
  esac
  runner="bin/snodra-conformance-$runner_version"
  if [[ ! -x "$runner" ]]; then
    archive="snodra-conformance-$runner_version-$target.tar.gz"
    sums=scripts/conformance-runner.sha256
    want="$(awk -v a="$archive" '$2 == a { print $1 }' "$sums")"
    if [[ -z "$want" ]]; then
      echo "no sha256 pinned for $archive in $sums: set SNODRA_CONFORMANCE_BIN to a runner binary" >&2
      exit 2
    fi
    tmp="$(mktemp -d)"
    curl -fsSL -o "$tmp/$archive" \
      "https://github.com/snodra/snodra-conformance/releases/download/$runner_version/$archive"
    if command -v sha256sum >/dev/null 2>&1; then
      got="$(sha256sum "$tmp/$archive" | cut -d' ' -f1)"
    else
      got="$(shasum -a 256 "$tmp/$archive" | cut -d' ' -f1)"
    fi
    if [[ "$got" != "$want" ]]; then
      echo "$archive has sha256 $got, not the $want pinned in $sums" >&2
      rm -rf "$tmp"
      exit 1
    fi
    tar -xzf "$tmp/$archive" -C "$tmp"
    found="$(find "$tmp" -type f -name snodra-conformance | head -n 1)"
    if [[ -z "$found" ]]; then
      echo "no snodra-conformance binary in $archive" >&2
      rm -rf "$tmp"
      exit 2
    fi
    mv "$found" "$runner"
    chmod +x "$runner"
    rm -rf "$tmp"
  fi
fi

containers=()
cleanup() {
  for c in ${containers[@]+"${containers[@]}"}; do
    docker rm -f "$c" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

hub_args=()
for i in $(seq 1 "$hubs"); do
  port=$((base_port + (i - 1) * 100))
  name="snodra-conformance-hub-$port-$$"
  docker run -d --rm --name "$name" -p "127.0.0.1:$port:7000" "$server_image" >/dev/null
  containers+=("$name")
  hub_args+=(--hub "127.0.0.1:$port")
done

# Wait for each hub's port (60 s at most).
for i in $(seq 1 "$hubs"); do
  port=$((base_port + (i - 1) * 100))
  up=no
  for _ in $(seq 1 120); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      up=yes
      break
    fi
    sleep 0.5
  done
  if [[ "$up" != yes ]]; then
    echo "the hub $server_image did not open 127.0.0.1:$port within 60 s" >&2
    exit 1
  fi
done

run_runner() {
  "$runner" run \
    --service-cmd "'$PWD/bin/conformance-service'" \
    --service conformance-python \
    "${hub_args[@]}" \
    --report "$report" \
    ${args[@]+"${args[@]}"}
}

if [[ -n "${SNODRA_CONFORMANCE_REPORT:-}" ]]; then
  status=0
  run_runner >"$SNODRA_CONFORMANCE_REPORT" || status=$?
  cat "$SNODRA_CONFORMANCE_REPORT"
  exit "$status"
fi
run_runner
