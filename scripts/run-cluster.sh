#!/usr/bin/env bash
# Start three real Concord nodes (gRPC + one WAL file each) on localhost.
# Ctrl-C stops them. Data lives in ./data and survives restarts.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build -p concord-server --release
BIN=target/release/concord-server
PEERS="1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003"
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT INT TERM
for id in 1 2 3; do
  "$BIN" serve --id "$id" --peers "$PEERS" --data-dir "data/node$id" &
  pids+=($!)
done
echo "cluster up. try:"
echo "  $BIN put --addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 city lisbon"
echo "  $BIN get --addr 127.0.0.1:7002 city"
wait
