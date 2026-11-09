#!/usr/bin/env bash
# Real standalone hard restart; only this run's child processes are stopped.
set -euo pipefail
export MC_ENABLE_HA_REBUILD=1
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build}"
MASTER_BIN="${MASTER_BIN:-$BUILD_DIR/mooncake-store/src/mooncake_master}"
CLIENT_BIN="${CLIENT_BIN:-$BUILD_DIR/mooncake-store/tests/ha_recovery_live_main}"
MASTER_PORT="${MASTER_PORT:-50055}"
METRICS_PORT="${METRICS_PORT:-$((MASTER_PORT + 1000))}"
WORKDIR="$(mktemp -d /tmp/ha_live_test.XXXXXX)"
export MC_REBUILD_EPOCH_FILE="$WORKDIR/epoch"
MASTER_LOG="$WORKDIR/master.log"
CLIENT_LOG="$WORKDIR/client.log"
MASTER_PID=""; CLIENT_PID=""
cleanup() {
    for pid in "$CLIENT_PID" "$MASTER_PID"; do
        if [ -n "$pid" ]; then
            kill -9 "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT
start_master() {
    "$MASTER_BIN" -rpc_port="$MASTER_PORT" -metrics_port="$METRICS_PORT" \
        -enable_ha=false -enable_metric_reporting=false >>"$MASTER_LOG" 2>&1 &
    MASTER_PID=$!
    sleep 2
    kill -0 "$MASTER_PID" 2>/dev/null || { cat "$MASTER_LOG"; return 1; }
}
wait_for_line() {
    local pattern="$1" seconds="$2"
    for ((i=0; i<seconds*2; i++)); do
        grep -q "$pattern" "$CLIENT_LOG" && return 0
        kill -0 "$CLIENT_PID" 2>/dev/null || break
        sleep 0.5
    done
    grep -q "$pattern" "$CLIENT_LOG" || { tail -30 "$CLIENT_LOG"; return 1; }
}
[ -x "$MASTER_BIN" ] && [ -x "$CLIENT_BIN" ]
echo "[ha_live_test] workdir=$WORKDIR"
start_master
"$CLIENT_BIN" -master="127.0.0.1:$MASTER_PORT" \
    -local="${LOCAL_ADDR:-127.0.0.1:19110}" -protocol="${PROTOCOL:-tcp}" \
    -nkeys="${NKEYS:-50}" -npu_device="${NPU_DEVICE:--1}" >>"$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!
wait_for_line READY_FOR_KILL 60
kill -9 "$MASTER_PID"
wait "$MASTER_PID" 2>/dev/null || true
MASTER_PID=""
sleep 4
start_master
wait_for_line RESULT= 150
wait "$CLIENT_PID"
CLIENT_PID=""
grep -q 'RESULT=PASS' "$CLIENT_LOG"
tail -20 "$CLIENT_LOG"
echo '[ha_live_test] PASS: real master hard restart and byte-verified recovery'
