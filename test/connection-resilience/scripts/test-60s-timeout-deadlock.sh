#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TEST_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd -- "$SCRIPT_DIR/../../.." && pwd)

TOXIPROXY_URL=${TOXIPROXY_URL:-"http://localhost:8474"}
PROXY_NAME=${PROXY_NAME:-"sink-eventstore"}

# How long the sink connection is unavailable.
OUTAGE_SECONDS=${OUTAGE_SECONDS:-90}

# How long to wait for replication progress after the connection is restored.
RECOVERY_TIMEOUT_SECONDS=${RECOVERY_TIMEOUT_SECONDS:-120}

# Controls the sink write timeout used by TcpEventWriter (replicator.sink.writeTimeoutSeconds).
# This is intentionally defaulted to 30 so this test covers outages longer than the write timeout.
WRITE_TIMEOUT_SECONDS=${WRITE_TIMEOUT_SECONDS:-30}

CHECKPOINT_FILE=${CHECKPOINT_FILE:-"/tmp/replicator-checkpoint"}
REPLICATOR_LOG=${REPLICATOR_LOG:-"/tmp/replicator-test.log"}

echo "════════════════════════════════════════════════════════"
echo "🧪 CONNECTION RESILIENCE TEST - 60s heartbeat"
echo "════════════════════════════════════════════════════════"
echo ""
echo "This test will:"
echo "1. Start replicator (local code)"
echo "2. Start continuous writes to the source"
echo "3. Disable the sink TCP proxy for ${OUTAGE_SECONDS}s"
echo "4. Re-enable the proxy and verify the checkpoint advances within ${RECOVERY_TIMEOUT_SECONDS}s"
echo ""
echo "Parameters:"
echo "  OUTAGE_SECONDS=$OUTAGE_SECONDS"
echo "  RECOVERY_TIMEOUT_SECONDS=$RECOVERY_TIMEOUT_SECONDS"
echo "  WRITE_TIMEOUT_SECONDS=$WRITE_TIMEOUT_SECONDS"
echo ""

cleanup() {
  echo ""
  echo "🧹 Cleaning up..."

  if [[ -n "${CONTINUOUS_PID:-}" ]]; then
    kill "$CONTINUOUS_PID" 2>/dev/null || true
  fi

  if [[ -n "${REPLICATOR_PID:-}" ]]; then
    kill "$REPLICATOR_PID" 2>/dev/null || true
  fi

  curl -s -X POST "${TOXIPROXY_URL}/proxies/${PROXY_NAME}" \
    -H "Content-Type: application/json" \
    -d '{"enabled": true}' > /dev/null || true

  echo "✓ Cleanup complete"
}
trap cleanup EXIT

ensure_toxiproxy_proxy() {
  if ! curl -fsS "${TOXIPROXY_URL}/version" > /dev/null; then
    echo "❌ Toxiproxy is not reachable at ${TOXIPROXY_URL}. Did you run docker-compose up -d?"
    exit 1
  fi

  if ! curl -s "${TOXIPROXY_URL}/proxies" | grep -q "\"${PROXY_NAME}\""; then
    echo "Creating missing toxiproxy proxy: ${PROXY_NAME} (listen 11131 -> upstream sink-eventstore:1113)"
    curl -s -X POST "${TOXIPROXY_URL}/proxies" \
      -H "Content-Type: application/json" \
      -d '{"name":"sink-eventstore","listen":"0.0.0.0:11131","upstream":"sink-eventstore:1113"}' > /dev/null
  fi

  curl -s -X POST "${TOXIPROXY_URL}/proxies/${PROXY_NAME}" \
    -H "Content-Type: application/json" \
    -d '{"enabled": true}' > /dev/null
}

get_checkpoint_pos() {
  if [[ ! -s "${CHECKPOINT_FILE}" ]]; then
    echo ""
    return
  fi

  # Format: eventNumber,eventPosition
  awk -F',' '{print $2}' "${CHECKPOINT_FILE}" | tr -d '[:space:]'
}

wait_for_checkpoint() {
  echo "⏳ Waiting for checkpoint file to be created..."

  for _ in {1..60}; do
    local pos
    pos=$(get_checkpoint_pos)

    if [[ -n "$pos" ]]; then
      echo "✓ Checkpoint file created (${CHECKPOINT_FILE}), current position: $pos"
      return
    fi

    sleep 1
  done

  echo "❌ Checkpoint file was not created within 60s"
  echo "Recent replicator logs:"
  tail -50 "${REPLICATOR_LOG}" || true
  exit 1
}

wait_for_checkpoint_advance() {
  local baseline=$1
  local timeout=$2

  echo "⏳ Waiting up to ${timeout}s for checkpoint to advance beyond ${baseline}..."

  local start
  start=$(date +%s)

  while true; do
    if [[ -n "${REPLICATOR_PID:-}" ]] && ! kill -0 "${REPLICATOR_PID}" 2>/dev/null; then
      echo "❌ Replicator process is not running (PID ${REPLICATOR_PID})"
      echo "Recent replicator logs:"
      tail -80 "${REPLICATOR_LOG}" || true
      exit 1
    fi

    local now
    now=$(date +%s)

    local elapsed=$((now - start))
    if [[ $elapsed -ge $timeout ]]; then
      echo "❌ Checkpoint did not advance within ${timeout}s"
      echo "Last checkpoint file contents:"
      (ls -la "${CHECKPOINT_FILE}" && cat "${CHECKPOINT_FILE}") || true
      echo ""
      echo "Recent replicator logs (interesting lines):"
      grep -E "(Write operation timed out|Writer pipe faulted|Prepare pipe faulted|Replicator cycle crashed|Channel shovel crashed|Waiting for the sink pipe to exhaust)" "${REPLICATOR_LOG}" | tail -80 || tail -80 "${REPLICATOR_LOG}" || true
      exit 1
    fi

    local pos
    pos=$(get_checkpoint_pos)

    if [[ -n "$pos" && "$pos" -gt "$baseline" ]]; then
      echo "✓ Checkpoint advanced: ${baseline} → ${pos} (after ${elapsed}s)"
      return
    fi

    sleep 5
  done
}

ensure_toxiproxy_proxy

# Reset state from previous runs
rm -f "${CHECKPOINT_FILE}" /tmp/replicator-test.pid "${REPLICATOR_LOG}" /tmp/continuous-writes.log

echo "Starting continuous event generation..."
cd "${TEST_DIR}"
./scripts/continuous-writes.sh > /tmp/continuous-writes.log 2>&1 &
CONTINUOUS_PID=$!
echo "✓ Continuous writes started (PID: $CONTINUOUS_PID)"

# Let events accumulate for a short time
sleep 2

# Start replicator
echo "Starting replicator (with WRITE_TIMEOUT_SECONDS=${WRITE_TIMEOUT_SECONDS})..."
cd "${ROOT_DIR}/src/replicator"
REPLICATOR_SINK_WRITETIMEOUTSECONDS="${WRITE_TIMEOUT_SECONDS}" nohup dotnet run --no-build > "${REPLICATOR_LOG}" 2>&1 &
REPLICATOR_PID=$!
echo "${REPLICATOR_PID}" > /tmp/replicator-test.pid
echo "✓ Replicator started (PID: $REPLICATOR_PID)"

# Wait for replication to start
wait_for_checkpoint

# Confirm we're making progress before simulating failure
BASE_POS=$(get_checkpoint_pos)
if [[ -z "$BASE_POS" ]]; then
  echo "❌ Unable to read checkpoint position"
  exit 1
fi

wait_for_checkpoint_advance "$BASE_POS" 60

POS_BEFORE_OUTAGE=$(get_checkpoint_pos)
echo "✓ Checkpoint before outage: ${POS_BEFORE_OUTAGE}"
echo ""

# Trigger outage (proxy disabled), then restore it.
echo "════════════════════════════════════════════════════════"
echo "🔴 DISABLING SINK PROXY FOR ${OUTAGE_SECONDS}s"
echo "════════════════════════════════════════════════════════"

curl -s -X POST "${TOXIPROXY_URL}/proxies/${PROXY_NAME}" \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}' > /dev/null

echo "✓ Proxy disabled"
sleep "${OUTAGE_SECONDS}"

echo "🟢 Re-enabling proxy (allowing reconnection)..."
curl -s -X POST "${TOXIPROXY_URL}/proxies/${PROXY_NAME}" \
  -H "Content-Type: application/json" \
  -d '{"enabled": true}' > /dev/null

echo "✓ Proxy re-enabled"
echo ""

# Success criteria: checkpoint advances again without manual restart.
wait_for_checkpoint_advance "$POS_BEFORE_OUTAGE" "${RECOVERY_TIMEOUT_SECONDS}"

echo ""
echo "════════════════════════════════════════════════════════"
echo "✅ PASS"
echo "════════════════════════════════════════════════════════"
echo "Replicator recovered and checkpoint advanced after a ${OUTAGE_SECONDS}s sink outage without manual restart."
