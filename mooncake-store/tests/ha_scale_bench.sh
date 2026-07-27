#!/usr/bin/env bash
# HA scale benchmark driver. For each --nkeys in the sweep:
#   1. start a fresh mooncake_master (non-HA) on $MASTER_PORT
#   2. start ha_scale_bench_main -> mounts a segment, BatchPuts N keys,
#      records fill throughput + steady Get latency, prints READY_FOR_KILL,
#      then polls a sampled probe every --poll_ms
#   3. on READY_FOR_KILL: kill -9 the master (probe reads start failing)
#   4. restart master on the SAME port -> client resends its local table,
#      master rebuilds; client measures rebuild latency + outage-window stats
#   5. scrape the JSON_RESULT line into a summary table
#
# Each nkeys value runs on a fresh master (clean state). Results -> $OUT_TSV.
set -u

BUILD_DIR="${BUILD_DIR:-/export/home/shenshuwei.3/Mooncake/build}"
MASTER_BIN="${MASTER_BIN:-$BUILD_DIR/mooncake-store/src/mooncake_master}"
CLIENT_BIN="${CLIENT_BIN:-$BUILD_DIR/mooncake-store/tests/ha_scale_bench_main}"

# Per-run ports are derived from these bases + RUN_IDX*10 (see run_one), so
# consecutive runs never reuse a socket still in TIME_WAIT.
BASE_MASTER_PORT="${MASTER_PORT:-50057}"
BASE_METRICS_PORT="${METRICS_PORT:-9013}"
BASE_LOCAL_PORT="${LOCAL_PORT:-19120}"
MASTER_PORT=""      # set per-run in run_one
METRICS_PORT=""
LOCAL_ADDR=""
PROTOCOL="${PROTOCOL:-tcp}"
VSIZE="${VSIZE:-100}"
BATCH="${BATCH:-2000}"
PROBE="${PROBE:-1000}"
POLL_MS="${POLL_MS:-20}"
SEG_MB="${SEG_MB:-0}"       # 0 => client auto-sizes from nkeys*vsize
ALLOC_MB="${ALLOC_MB:-0}"   # 0 => client default (64MB)
DOWN_WAIT="${DOWN_WAIT:-4}"       # seconds to let client observe the outage
# nkeys sweep (override by passing args: ha_scale_bench.sh 1000 10000 ...)
NKEYS_SWEEP=("$@")
if [ "${#NKEYS_SWEEP[@]}" -eq 0 ]; then
    NKEYS_SWEEP=(1000 10000 100000 1000000)
fi

OUT_DIR="${OUT_DIR:-$(mktemp -d /tmp/ha_scale_bench.XXXXXX)}"
mkdir -p "$OUT_DIR"
OUT_TSV="$OUT_DIR/results.tsv"
MASTER_PID=""
CLIENT_PID=""

log() { echo "[ha_scale_bench] $*"; }

cleanup() {
    [ -n "$CLIENT_PID" ] && kill -9 "$CLIENT_PID" 2>/dev/null
    [ -n "$MASTER_PID" ] && kill -9 "$MASTER_PID" 2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

start_master() {
    # $1=logfile. Uses per-run $MASTER_PORT and $METRICS_PORT (set by run_one)
    # so consecutive runs don't collide on a socket still in TIME_WAIT.
    "$MASTER_BIN" -rpc_port="$MASTER_PORT" -metrics_port="$METRICS_PORT" \
        -enable_ha=false -enable_metric_reporting=false >>"$1" 2>&1 &
    MASTER_PID=$!
}

wait_for_line() {  # $1=file $2=pattern $3=timeout_sec
    local f="$1" pat="$2" t="$3" i=0
    while [ "$i" -lt "$((t * 2))" ]; do
        grep -q "$pat" "$f" 2>/dev/null && return 0
        sleep 0.5
        i=$((i + 1))
    done
    return 1
}

[ -x "$MASTER_BIN" ] || { log "FATAL master bin missing: $MASTER_BIN"; exit 3; }
[ -x "$CLIENT_BIN" ] || { log "FATAL client bin missing: $CLIENT_BIN"; exit 3; }

log "out_dir=$OUT_DIR  sweep=${NKEYS_SWEEP[*]}"
printf 'nkeys\tvsize\tfill_keys_per_s\trebuild_ms\trecover_since_ready_ms\tsteady_get_lat_ms\toutage_get_lat_ms\tmin_avail_pct\n' >"$OUT_TSV"

run_one() {
    local NKEYS="$1"
    local tag="n${NKEYS}"
    local MLOG="$OUT_DIR/master_${tag}.log"
    local CLOG="$OUT_DIR/client_${tag}.log"
    : >"$MLOG"; : >"$CLOG"
    MASTER_PID=""; CLIENT_PID=""

    # Unique ports per run so a socket left in TIME_WAIT by the previous run
    # can't stop this run's master from binding. RUN_IDX is bumped by the caller.
    MASTER_PORT=$(( BASE_MASTER_PORT + RUN_IDX * 10 ))
    METRICS_PORT=$(( BASE_METRICS_PORT + RUN_IDX * 10 ))
    local LOCAL_PORT=$(( BASE_LOCAL_PORT + RUN_IDX * 10 ))
    LOCAL_ADDR="127.0.0.1:${LOCAL_PORT}"

    log "=== nkeys=$NKEYS : start master (rpc=$MASTER_PORT metrics=$METRICS_PORT local=$LOCAL_ADDR) ==="
    start_master "$MLOG"
    sleep 2
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        log "FATAL master died on startup (nkeys=$NKEYS); log:"; tail -20 "$MLOG"; return 1
    fi

    # client: fill + baseline + wait-for-kill
    local FILL_TIMEOUT=$(( 120 + NKEYS / 5000 ))   # scale fill wait with size
    "$CLIENT_BIN" -master="127.0.0.1:$MASTER_PORT" -local="$LOCAL_ADDR" \
        -protocol="$PROTOCOL" -nkeys="$NKEYS" -vsize="$VSIZE" -batch="$BATCH" \
        -probe="$PROBE" -poll_ms="$POLL_MS" -seg_mb="$SEG_MB" -alloc_mb="$ALLOC_MB" \
        >>"$CLOG" 2>&1 &
    CLIENT_PID=$!

    if ! wait_for_line "$CLOG" "READY_FOR_KILL" "$FILL_TIMEOUT"; then
        log "FATAL client never reached READY_FOR_KILL (nkeys=$NKEYS); tail:"; tail -25 "$CLOG"
        kill -9 "$CLIENT_PID" 2>/dev/null; kill -9 "$MASTER_PID" 2>/dev/null
        return 1
    fi
    grep -m1 "FILL done" "$CLOG" | sed 's/^/[ha_scale_bench]   /'
    log "  nkeys=$NKEYS: baseline done, killing master pid=$MASTER_PID"

    kill -9 "$MASTER_PID" 2>/dev/null
    wait "$MASTER_PID" 2>/dev/null
    MASTER_PID=""
    sleep "$DOWN_WAIT"

    log "  nkeys=$NKEYS: restart master on same port"
    start_master "$MLOG"
    sleep 2
    if ! kill -0 "$MASTER_PID" 2>/dev/null; then
        log "FATAL master failed to restart (nkeys=$NKEYS); log:"; tail -20 "$MLOG"; return 1
    fi

    local REC_TIMEOUT=$(( 120 + NKEYS / 2000 ))
    if ! wait_for_line "$CLOG" "JSON_RESULT=" "$REC_TIMEOUT"; then
        log "FATAL client never printed JSON_RESULT (nkeys=$NKEYS); tail:"; tail -25 "$CLOG"
        kill -9 "$CLIENT_PID" 2>/dev/null; kill -9 "$MASTER_PID" 2>/dev/null
        return 1
    fi

    local JLINE
    JLINE="$(grep -m1 "JSON_RESULT=" "$CLOG" | sed 's/.*JSON_RESULT=//')"
    log "  nkeys=$NKEYS JSON: $JLINE"

    # parse with python for robustness, append a TSV row
    python3 - "$JLINE" >>"$OUT_TSV" <<'PY'
import sys, json
d = json.loads(sys.argv[1])
print("%d\t%d\t%.0f\t%.1f\t%.1f\t%.3f\t%.3f\t%.2f" % (
    d["nkeys"], d["vsize"], d["fill_keys_per_s"], d["rebuild_ms"],
    d["recover_since_ready_ms"], d["steady_get_lat_ms"],
    d["outage_get_lat_ms"], d["min_avail_pct"]))
PY

    kill -9 "$CLIENT_PID" 2>/dev/null
    wait "$CLIENT_PID" 2>/dev/null
    CLIENT_PID=""
    # IMPORTANT: kill this run's (restarted) master too, else it lingers holding
    # its rpc/metrics ports and the next run that reuses a nearby port fails.
    kill -9 "$MASTER_PID" 2>/dev/null
    wait "$MASTER_PID" 2>/dev/null
    MASTER_PID=""
    log "  nkeys=$NKEYS: DONE"
    return 0
}

overall=0
RUN_IDX=0
for nk in "${NKEYS_SWEEP[@]}"; do
    if ! run_one "$nk"; then
        log "nkeys=$nk FAILED"
        overall=1
    fi
    RUN_IDX=$(( RUN_IDX + 1 ))
    sleep 2
done

log "================ SUMMARY ================"
column -t -s $'\t' "$OUT_TSV" | sed 's/^/[ha_scale_bench] /'
log "TSV: $OUT_TSV"
log "logs: $OUT_DIR"
exit "$overall"
