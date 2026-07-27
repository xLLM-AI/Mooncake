# HA Client-Driven Metadata Rebuild — Benchmark Report

Corresponding change: `feat: add client metadata rebuild for master high availability`
(branch `feat/client-metadata-rebuild-ha`). This report presents measured data for this HA rebuild feature across three dimensions — **scale, rebuild latency, and recovery-window read performance** — for code reviewers to evaluate.

> One-line takeaway: under a single-client / single-replica, value=100B configuration, after the master is
> killed with `kill -9` and restarted on the same port, the client automatically resends its local
> key→location table, the master completes metadata rebuild and restores full readability, achieving
> **zero recomputation**. Rebuild latency is about 1.5–2.1 seconds within 1M keys (dominated by
> heartbeat-detection latency), and about 12 seconds at 5M keys (resend + rebuild workload starts to
> dominate). After recovery completes, read latency matches the pre-failure level (no long-term degradation).

---

## 1. Test Methodology (how it was measured, why it is trustworthy)

### 1.1 Test Setup

This uses a **separate-process** live test (not an in-process mock), which most closely resembles a real deployment:

- A real `mooncake_master` process (non-HA mode, `-enable_ha=false`).
- A standalone benchmark client (`ha_scale_bench_main`) that mounts a segment, loads N
  keys, records a baseline, then enters polling; meanwhile an external script `kill -9`s the master and restarts it on the same port.
- After the client observes "reads fail → all readable again", it emits the rebuild latency and recovery-window statistics.

Timeline: `start master → load N keys → print READY_FOR_KILL → kill -9 master →
wait 4s (let the client detect the failure) → restart master on the same port → client reconnects and resends its local table →
master rebuilds → probes turn fully green → emit JSON`.

### 1.2 Key Measurement Design (why it is measured this way)

- **Keys are loaded with BatchPut** (2000 per batch): individual Puts would be dominated by per-RPC overhead and would not accurately measure fill throughput.
- **Rebuild completion is judged with a "sampling probe"**: **uniformly sample 1000 keys** over `[0, N)`; rebuild is deemed complete
  once all of them are readable. Getting all N keys every round would itself take several seconds at the million scale and would pollute the latency measurement.
  Uniform sampling probes every region of the key space, balancing "detecting partial rebuild" with "low overhead".
- **Timing uses `steady_clock`, with a 20ms recovery polling interval**: rebuild-latency resolution is about ±20ms.
- Each scale runs on a **fresh master and a fresh port** (clean state, no cross-interference).

### 1.3 Definitions of the Three Metrics

| Metric | Field | Definition |
|---|---|---|
| **Rebuild latency** | `rebuild_ms` | Wall-clock time from "probe first sees a failure" (detecting the master is down) to "probe first turns fully green" (rebuild complete) |
| **Scale** | `nkeys` sweep | The same procedure repeated at 1k / 10k / 100k / 1M / 5M keys |
| **Recovery-window read latency** | `outage_get_lat_ms` | Average latency of **successful** Gets within the recovery window, compared against `steady_get_lat_ms` (pre-failure steady state) |
| (auxiliary) | `min_avail_pct` | The minimum probe availability within the recovery window (0 means everything was unreadable at some point) |

---

## 2. Test Environment

| Item | Value |
|---|---|
| Machine memory | 3.0 TB (~1.6 TB available) |
| CPU | 640 cores |
| Runtime | Container `shenshuwei-xllm` (image `xllm-dev-a3-arm-cann9`), openEuler / ARM64, glibc 2.38 |
| Transport protocol | TCP (`-protocol=tcp`) |
| Master mode | Non-HA (`-enable_ha=false -enable_metric_reporting=false`) |
| Replica count | replica_num=1 (single replica) |
| Value size | 100 bytes/key |
| Segment size | Auto-estimated at ~1200B/object (9298 MB actual for the 5M scale) |
| client_ttl | 10s (master default; affects failure-detection latency, see §4) |

> Note: the test runs inside the container because the binary is compiled in that container (glibc 2.38); the host
> (glibc 2.34) lacks matching runtime libraries and cannot run it directly.

---

## 3. Results

### 3.1 Summary Table (single run, not averaged over multiple runs)

| nkeys | Fill rate (keys/s) | **Rebuild latency (ms)** | Steady-state read latency (ms) | Recovery-window read latency (ms) | Recovery-window min availability |
|--:|--:|--:|--:|--:|--:|
| 1,000 | 230,585 | 1,546 | 0.043 | 0.040 | 0% |
| 10,000 | 246,441 | 1,559 | 0.041 | 0.043 | 0% |
| 100,000 | 232,930 | 1,791 | 0.042 | 0.046 | 0% |
| 1,000,000 | 206,332 | 2,091 | 0.045 | 0.042 | 0% |
| 5,000,000 | 196,708 | **12,068** | 0.038 | 0.042 | 0% |

Every scale ultimately reports `RESULT=PASS`, the sampling probe recovers 1000/1000, and data is rebuilt with zero recomputation.

### 3.2 Metrics 1 & 2: How Rebuild Latency Scales

- **1k → 1M**: rebuild latency goes from 1.5s → 2.1s, a very gentle increase. This range is dominated **not** by rebuild workload,
  but by **failure-detection latency** (from the master dying to the client's heartbeat deciding to reconnect takes on the order of seconds, and is nearly independent of key count).
- **1M → 5M**: latency jumps from 2.1s to **12s**, clearly superlinear. Here the resend workload (5M entries at 256 per batch
  = about 20k `RebuildMetadata` RPCs) plus the master's per-key rebuild — **the workload itself** —
  starts to dominate and overtakes the fixed detection latency.
- Fill throughput slowly drops from ~230k/s to ~200k/s as scale grows (allocator pressure, larger single-segment capacity).

### 3.3 Metric 3: Recovery-Window Read Latency

- Steady-state read latency is stable at **~0.04ms** (single-threaded sequential Get, local TCP).
- The latency of successful Gets during recovery is likewise **~0.04ms**, **essentially indistinguishable from steady state** — i.e., once rebuild completes,
  readable keys are just as fast to read as before the failure, with no long-term degradation.
- `min_avail_pct=0` indicates that between the master being killed and rebuild completing, there is a window where **all keys are unreadable**
  (as expected — the new master is empty and has not yet received the metadata resent by the client). During rebuild the transition is
  "all-none → all-present", not a gradual recovery.

---

## 4. Important Limitations and Caveats (required reading for reviewers)

1. **`rebuild_ms` includes failure-detection latency.** It measures the end-to-end time from "client detects failure → fully green again",
   which includes the seconds-level latency for the heartbeat to conclude the master is dead. Therefore the
   1.5–2s seen within 1M keys is **not pure rebuild time**; it is largely the floor set by detection latency. To measure "pure resend + rebuild" latency separately,
   a latency-histogram metric on `RebuildMetadata` would need to be added on the master side (there are currently only requests/failures
   counters, no latency statistics) — recommended as a separate follow-up change.

2. **Single-client / single-replica scope.** In this test one client Puts data to its own segment (via the
   `RecordLocalReplica` local-bookkeeping path), with replica_num=1. **Not covered**: the cross-client
   notify bookkeeping path and multi-replica merge rebuild. The **correctness** of these paths is already covered by unit tests
   (tests 4 and 6 in `client_metadata_rebuild_test.cpp`), but their **performance** was not measured at this scale.

3. **Single run, not averaged over multiple runs.** Each scale is run only once, so there is jitter from heartbeat timing (in an earlier run,
   the 100k scale once measured an anomalously low 143ms because it hit a different heartbeat moment). For a formal benchmark, repeating each
   scale 3–5 times and taking the median is recommended.

4. **Recovery-window latency is the latency of single-threaded sequential Gets**, not high-concurrency throughput. Latency/failure rates under concurrent workloads
   require a multi-threaded client to measure.

5. **Per-object memory footprint (a byproduct observation):** each key measured about **~1073 bytes** in the segment
   (only 100B is value; the rest is object metadata + allocator minimum-unit/alignment/fragmentation). The segment
   capacity for scale tests must be estimated accordingly, otherwise `NO_AVAILABLE_HANDLE` occurs (segment full during the data-loading phase).

---

## 5. Reproduction

Test code (included with this PR):
- `mooncake-store/tests/ha_scale_bench_main.cpp` — benchmark client
- `mooncake-store/tests/ha_scale_bench.sh` — driver script (start/kill/restart master, collect JSON)

Run inside the build container:

```bash
# Build
cd build && make ha_scale_bench_main -j32

# Run the full sweep (1k / 10k / 100k / 1M / 5M)
cd /path/to/Mooncake
OUT_DIR=/tmp/ha_scale bash mooncake-store/tests/ha_scale_bench.sh \
    1000 10000 100000 1000000 5000000

# Or a custom single scale (tunable value size / segment size)
VSIZE=100 SEG_MB=10240 bash mooncake-store/tests/ha_scale_bench.sh 5000000
```

Results are written as TSV (`$OUT_DIR/results.tsv`) plus per-scale master/client logs.

---

*Data collected: 2026-07-27. This report and the test scripts were generated with AI assistance; all data comes from real runs.
Before submission, please have a human review every changed line and independently reproduce the results.*
