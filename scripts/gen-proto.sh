#!/usr/bin/env bash
# Regenerates packages/snodra-messaging/src/snodra/_hubpb/{hub_pb2.py,hub_pb2.pyi,hub_pb2_grpc.py} from proto/hub.proto
# with the pinned grpcio-tools (dependency group "gen"). The proto is copied into a temporary tree as
# snodra/_hubpb/hub.proto so the generated gRPC module imports `from snodra._hubpb import hub_pb2`.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/packages/snodra-messaging/src"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

cd "$root"
mkdir -p "$tmp/snodra/_hubpb"
cp proto/hub.proto "$tmp/snodra/_hubpb/hub.proto"

uv run --frozen --only-group gen python -c '
import importlib.metadata, sys
v = importlib.metadata.version("grpcio-tools")
sys.exit(0 if v == "1.85.4" else "grpcio-tools %s installed, 1.85.4 required" % v)
'
uv run --frozen --only-group gen python -m grpc_tools.protoc -I "$tmp" \
  --python_out="$out" --pyi_out="$out" --grpc_python_out="$out" snodra/_hubpb/hub.proto
