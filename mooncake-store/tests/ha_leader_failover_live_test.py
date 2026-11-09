#!/usr/bin/env python3
"""Real two-master failover with opt-in client rebuild; never uses cached binaries."""

import argparse
import json
import os
import pathlib
import socket
import subprocess
import time
import urllib.request

p = argparse.ArgumentParser()
p.add_argument("--build-dir", required=True)
p.add_argument("--protocol", default="tcp")
p.add_argument("--npu-device", type=int, default=-1)
p.add_argument("--etcd-bin", required=True)
p.add_argument("--run-dir", required=True)
a = p.parse_args()
root = pathlib.Path(a.run_dir)
root.mkdir(exist_ok=False, parents=True)
root.chmod(0o755)
procs = []
build = pathlib.Path(a.build_dir)
env = dict(
    os.environ, MC_ENABLE_HA_REBUILD="1", MC_STORE_CLUSTER_ID="ha-port-" + root.name
)


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(name, binary, flags):
    args = [str(binary), *(f"--{key}={value}" for key, value in flags.items())]
    f = (root / (name + ".log")).open("w")
    q = subprocess.Popen(args, stdout=f, stderr=subprocess.STDOUT, env=env)
    procs.append((q, f))
    return q


def wait(pred, secs, label):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        v = pred()
        if v:
            return v
        time.sleep(0.1)
    raise RuntimeError("timeout: " + label)


def health(port):
    try:
        with urllib.request.urlopen(
            "http://127.0.0.1:%d/health" % port, timeout=0.5
        ) as r:
            return json.load(r)
    except Exception:
        return {}


def text(name):
    return (root / (name + ".log")).read_text(errors="replace")


def check(condition, diagnostic):
    if not condition:
        raise RuntimeError(diagnostic)


try:
    ep, peer = port(), port()
    endpoint = "127.0.0.1:%d" % ep
    peer_url = "http://127.0.0.1:%d" % peer
    start(
        "etcd",
        a.etcd_bin,
        {
            "data-dir": root / "etcd-data",
            "listen-client-urls": "http://" + endpoint,
            "advertise-client-urls": "http://" + endpoint,
            "listen-peer-urls": peer_url,
            "initial-advertise-peer-urls": peer_url,
            "initial-cluster": "default=" + peer_url,
        },
    )

    def etcd_ready():
        try:
            with urllib.request.urlopen(
                "http://" + endpoint + "/health", timeout=0.5
            ) as r:
                return r.status == 200
        except Exception:
            return False

    wait(etcd_ready, 15, "etcd ready")
    masters = []
    for i in range(2):
        rpc, admin = port(), port()
        q = start(
            "master%d" % i,
            build / "mooncake-store/src/mooncake_master",
            {
                "enable_ha": "true",
                "ha_backend_connstring": endpoint,
                "etcd_endpoints": endpoint,
                "cluster_id": env["MC_STORE_CLUSTER_ID"],
                "enable_oplog": "false",
                "enable_ha_rebuild_gate": "true",
                "rebuild_collect_window_ms": 20000,
                "rebuild_force_serve_timeout_ms": 60000,
                "rpc_address": "127.0.0.1",
                "rpc_port": rpc,
                "metrics_port": admin,
                "enable_metric_reporting": "false",
                "logtostderr": "true",
            },
        )
        masters.append((q, rpc, admin))

    def ready_leader():
        for i, (q, rpc, admin) in enumerate(masters):
            h = health(admin)
            if (
                q.poll() is None
                and h.get("service_ready")
                and h.get("leader_address") == "127.0.0.1:%d" % rpc
            ):
                return (i, h)
        return None

    initial, initial_health = wait(ready_leader, 30, "initial leader")
    (root / "initial-health.json").write_text(json.dumps(initial_health, indent=2))
    client = start(
        "client",
        build / "mooncake-store/tests/ha_recovery_live_main",
        {
            "master": "etcd://" + endpoint,
            "local": "127.0.0.1:%d" % port(),
            "protocol": a.protocol,
            "nkeys": 50,
            "npu_device": a.npu_device,
        },
    )
    wait(
        lambda: "READY_FOR_KILL" in text("client") or client.poll() is not None,
        60,
        "client baseline",
    )
    check("READY_FOR_KILL" in text("client"), text("client")[-6000:])
    if a.npu_device >= 0:
        check("REAL_NPU_HBM" in text("client"), "NPU HBM not allocated")
    masters[initial][0].kill()
    masters[initial][0].wait()
    survivor = 1 - initial
    saw_closed = False
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        h = health(masters[survivor][2])
        if h.get("leader_address") == "127.0.0.1:%d" % masters[survivor][
            1
        ] and not h.get("service_ready"):
            saw_closed = True
        if client.poll() is not None:
            break
        time.sleep(0.1)
    client_log = text("client")
    check(client.poll() == 0, client_log[-6000:])
    check(saw_closed, "promoted leader gate was not observed closed")
    final_health = health(masters[survivor][2])
    check(final_health.get("service_ready"), final_health)
    check(
        int(final_health["view_version"]) > int(initial_health["view_version"]),
        (initial_health, final_health),
    )
    check("RESULT=PASS" in client_log, client_log[-6000:])
    master_log = text("master%d" % survivor)
    for marker in (
        "expected_clients=1",
        "reason=all expected clients rebuilt completed=1/1",
    ):
        check(marker in master_log, master_log[-6000:])
    (root / "final-health.json").write_text(json.dumps(final_health, indent=2))
    report = {
        "result": "PASS",
        "initial_leader": initial,
        "promoted_leader": survivor,
        "initial_view": initial_health["view_version"],
        "promoted_view": final_health["view_version"],
        "observed_closed_gate": saw_closed,
        "real_npu_hbm": a.npu_device >= 0,
        "protocol": a.protocol,
        "npu_device": a.npu_device,
        "keys": 50,
        "read_after_rebuild": True,
        "fresh_write_read_after_rebuild": True,
    }
    (root / "result.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
except Exception as e:
    (root / "result.json").write_text(
        json.dumps({"result": "FAIL", "error": str(e)}, indent=2)
    )
    raise
finally:
    for q, f in reversed(procs):
        if q.poll() is None:
            q.terminate()
            try:
                q.wait(timeout=5)
            except subprocess.TimeoutExpired:
                q.kill()
                q.wait()
        f.close()
