#!/usr/bin/env bash
# Live HA recovery test (out-of-process):
#   Drives ha_recovery_live_main against a REAL, separate mooncake_master
#   PROCESS. Verifies that after the master is killed and restarted on the same
#   port, the standalone client re-registers its held key->location metadata so
#   that every key is readable again with ZERO recompute.
#
# Timeline:
#   1. start mooncake_master (non-HA) on $MASTER_PORT
#   2. start ha_recovery_live_main -> mounts a segment, Puts N keys, prints
#      "READY_FOR_KILL", then polls Get in a loop
#   3. once we see READY_FOR_KILL: kill -9 the master (reads start failing)
#   4. restart mooncake_master on the SAME port -> client reconnects, resends
#      its local_replica_table_, master rebuilds metadata, reads succeed again
#   5. client prints RESULT=PASS/FAIL; this script propagates that as exit code
set -u

BUILD_DIR="${BUILD_DIR:-/export/home/shenshuwei.3/Mooncake/build}"
MASTER_BIN="${MASTER_BIN:-$BUILD_DIR/mooncake-store/src/mooncake_master}"
CLIENT_BIN="${CLIENT_BIN:-$BUILD_DIR/mooncake-store/tests/ha_recovery_live_main}"

MASTER_PORT="${MASTER_PORT:-50055}"
LOCAL_ADDR="${LOCAL_ADDR:-127.0.0.1:19110}"
NKEYS="${NKEYS:-50}"
PROTOCOL="${PROTOCOL:-tcp}"

WORKDIR="$(mktemp -d /tmp/ha_live_test.XXXXXX)"
MASTER_LOG="$WORKDIR/master.log"
CLIENT_LOG="$WORKDIR/client.log"

MASTER_PID=""
CLIENT_PID=""

log() { echo "[ha_live_test] $*"; }

cleanup() {
    [ -n "$CLIENT_PID" ] && kill -9 "$CLIENT_PID" 2>/dev/null
    [ -n "$MASTER_PID" ] && kill -9 "$MASTER_PID" 2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

start_master() {
    "$MASTER_BIN" -rpc_port="$MASTER_PORT" -enable_ha=false \
        -enable_metric_reporting=false >>"$MASTER_LOG" 2>&1 &
    MASTER_PID=$!
    log "started master pid=$MASTER_PID port=$MASTER_PORT"
}

wait_for_line() {  # $1=file  $2=pattern  $3=timeout_sec
    local f="$1" pat="$2" t="$3" i=0
    while [ "$i" -lt "$((t * 2))" ]; do
        grep -q "$pat" "$f" 2>/dev/null && return 0
        # bail out early if the process we depend on already died
        [ -n "$CLIENT_PID" ] && ! kill -0 "$CLIENT_PID" 2>/dev/null && \
            grep -q "$pat" "$f" 2>/dev/null && return 0
        sleep 0.5
        i=$((i + 1))
    done
    return 1
}

[ -x "$MASTER_BIN" ] || { log "FATAL master bin missing: $MASTER_BIN"; exit 3; }
[ -x "$CLIENT_BIN" ] || { log "FATAL client bin missing: $CLIENT_BIN"; exit 3; }

log "workdir=$WORKDIR"

# --- 1. start master ---
start_master
sleep 2
if ! kill -0 "$MASTER_PID" 2>/dev/null; then
    log "FATAL master died on startup; log:"; cat "$MASTER_LOG"; exit 3
fi

# --- 2. start client ---
"$CLIENT_BIN" -master="127.0.0.1:$MASTER_PORT" -local="$LOCAL_ADDR" \
    -protocol="$PROTOCOL" -nkeys="$NKEYS" >>"$CLIENT_LOG" 2>&1 &
CLIENT_PID=$!
log "started client pid=$CLIENT_PID"

# --- 3. wait for baseline + READY_FOR_KILL ---
if ! wait_for_line "$CLIENT_LOG" "READY_FOR_KILL" 60; then
    log "FATAL client never reached READY_FOR_KILL; client log:"; cat "$CLIENT_LOG"
    exit 3
fi
log "client is READY_FOR_KILL; baseline done"

# --- 4. KILL the master hard ---
log "kill -9 master pid=$MASTER_PID"
kill -9 "$MASTER_PID" 2>/dev/null
wait "$MASTER_PID" 2>/dev/null
MASTER_PID=""
# give the client time to observe read failures (saw_down)
sleep 4

# --- 5. RESTART master on the same port ---
log "restart master on same port $MASTER_PORT"
start_master
sleep 2
if ! kill -0 "$MASTER_PID" 2>/dev/null; then
    log "FATAL master failed to restart; log:"; cat "$MASTER_LOG"; exit 3
fi

# --- 6. wait for client's verdict ---
if ! wait_for_line "$CLIENT_LOG" "RESULT=" 150; then
    log "FATAL client never printed RESULT; client log tail:"; tail -30 "$CLIENT_LOG"
    exit 3
fi

RESULT_LINE="$(grep -m1 "RESULT=" "$CLIENT_LOG")"
log "client verdict: $RESULT_LINE"
log "----- client log tail -----"; tail -20 "$CLIENT_LOG"

if echo "$RESULT_LINE" | grep -q "RESULT=PASS"; then
    log "OVERALL: PASS (out-of-process master kill+restart, metadata rebuilt)"
    exit 0
else
    log "OVERALL: FAIL"
    exit 1
fi
