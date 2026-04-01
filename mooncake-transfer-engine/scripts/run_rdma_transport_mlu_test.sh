#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)

TEST_BIN=${TEST_BIN:-"${REPO_ROOT}/../build-te-clean-mlu/tests/rdma_transport_mlu_test"}
DEVICE_NAME=${DEVICE_NAME:-mlx5_0}
MLU_ID=${MLU_ID:-0}
USE_MLU=${USE_MLU:-true}
USE_WILDCARD_LOCATION=${USE_WILDCARD_LOCATION:-true}
LOG_LEVEL=${LOG_LEVEL:-INFO}
TARGET_HINT_PORT=${TARGET_HINT_PORT:-12345}
INITIATOR_HINT_PORT=${INITIATOR_HINT_PORT:-12346}
WAIT_TIMEOUT_SEC=${WAIT_TIMEOUT_SEC:-30}
RUN_DIR=${RUN_DIR:-"/tmp/rdma_transport_mlu_test"}

usage() {
  cat <<EOF
Usage:
  LOCAL_IP=<ip> $0

Optional environment variables:
  TEST_BIN                 Path to rdma_transport_mlu_test binary
  DEVICE_NAME              RDMA device name, default: mlx5_0
  MLU_ID                   MLU device id, default: 0
  USE_MLU                  Whether to use MLU memory, default: true
  USE_WILDCARD_LOCATION    Whether to use wildcard location, default: true
  LOG_LEVEL                MC_LOG_LEVEL value, default: INFO
  TARGET_HINT_PORT         Hint port passed to target local_server_name
  INITIATOR_HINT_PORT      Hint port passed to initiator local_server_name
  WAIT_TIMEOUT_SEC         Wait timeout for target listening endpoint
  RUN_DIR                  Log directory, default: /tmp/rdma_transport_mlu_test

Example:
  LOCAL_IP=10.100.162.76 $0
EOF
}

if [[ "${1-}" == "-h" || "${1-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -x "${TEST_BIN}" ]]; then
  echo "test binary not found or not executable: ${TEST_BIN}" >&2
  exit 1
fi

if [[ -z "${LOCAL_IP:-}" ]]; then
  LOCAL_IP=$(hostname -I 2>/dev/null | awk '{for (i = 1; i <= NF; ++i) if ($i != "127.0.0.1") { print $i; exit }}')
fi

if [[ -z "${LOCAL_IP:-}" ]]; then
  echo "LOCAL_IP is required and could not be auto-detected" >&2
  exit 1
fi

mkdir -p "${RUN_DIR}"
TARGET_LOG="${RUN_DIR}/target.log"
INITIATOR_LOG="${RUN_DIR}/initiator.log"
rm -f "${TARGET_LOG}" "${INITIATOR_LOG}"

TARGET_PID=""

cleanup() {
  if [[ -n "${TARGET_PID}" ]] && kill -0 "${TARGET_PID}" 2>/dev/null; then
    kill "${TARGET_PID}" 2>/dev/null || true
    wait "${TARGET_PID}" 2>/dev/null || true
  fi
}

trap cleanup EXIT

TARGET_SERVER_NAME="${LOCAL_IP}:${TARGET_HINT_PORT}"
INITIATOR_SERVER_NAME="${LOCAL_IP}:${INITIATOR_HINT_PORT}"

echo "[1/4] starting target"
MC_LOG_LEVEL="${LOG_LEVEL}" \
  "${TEST_BIN}" \
  --mode=target \
  --metadata_server=P2PHANDSHAKE \
  --local_server_name="${TARGET_SERVER_NAME}" \
  --device_name="${DEVICE_NAME}" \
  --mlu_id="${MLU_ID}" \
  --use_mlu="${USE_MLU}" \
  --use_wildcard_location="${USE_WILDCARD_LOCATION}" \
  >"${TARGET_LOG}" 2>&1 &
TARGET_PID=$!

echo "[2/4] waiting for target listening endpoint"
TARGET_ENDPOINT=""
for ((i = 0; i < WAIT_TIMEOUT_SEC * 10; ++i)); do
  if ! kill -0 "${TARGET_PID}" 2>/dev/null; then
    echo "target exited unexpectedly" >&2
    cat "${TARGET_LOG}" >&2
    exit 1
  fi

  if grep -q "Transfer Engine RPC using P2P handshake, listening on" "${TARGET_LOG}" 2>/dev/null; then
    TARGET_ENDPOINT=$(sed -n 's/.*listening on \([^ ]*\).*/\1/p' "${TARGET_LOG}" | tail -n1)
    if [[ -n "${TARGET_ENDPOINT}" ]]; then
      break
    fi
  fi
  sleep 0.1
done

if [[ -z "${TARGET_ENDPOINT}" ]]; then
  echo "failed to get target listening endpoint within timeout" >&2
  cat "${TARGET_LOG}" >&2
  exit 1
fi

echo "target endpoint: ${TARGET_ENDPOINT}"

echo "[3/4] running initiator"
set +e
MC_LOG_LEVEL="${LOG_LEVEL}" \
  "${TEST_BIN}" \
  --mode=initiator \
  --metadata_server=P2PHANDSHAKE \
  --local_server_name="${INITIATOR_SERVER_NAME}" \
  --segment_id="${TARGET_ENDPOINT}" \
  --device_name="${DEVICE_NAME}" \
  --mlu_id="${MLU_ID}" \
  --use_mlu="${USE_MLU}" \
  --use_wildcard_location="${USE_WILDCARD_LOCATION}" \
  >"${INITIATOR_LOG}" 2>&1
RC=$?
set -e

echo "[4/4] logs"
echo "target log: ${TARGET_LOG}"
echo "initiator log: ${INITIATOR_LOG}"

cat "${INITIATOR_LOG}"

if [[ ${RC} -ne 0 ]]; then
  echo "initiator failed with exit code ${RC}" >&2
  echo "==== target log ====" >&2
  cat "${TARGET_LOG}" >&2
  echo "==== initiator log ====" >&2
  cat "${INITIATOR_LOG}" >&2
  exit "${RC}"
fi

if ! grep -q "MLU RDMA compare: OK" "${INITIATOR_LOG}"; then
  echo "initiator completed but did not report success marker" >&2
  echo "==== target log ====" >&2
  cat "${TARGET_LOG}" >&2
  echo "==== initiator log ====" >&2
  cat "${INITIATOR_LOG}" >&2
  exit 1
fi

echo "MLU RDMA validation passed"
