#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
TEST_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
ROOT_DIR=$(cd -- "$SCRIPT_DIR/../../.." && pwd)

OUTAGE_SECONDS=${OUTAGE_SECONDS:-30}
RECOVERY_TIMEOUT_SECONDS=${RECOVERY_TIMEOUT_SECONDS:-120}

# Failure mode for mock-transform:
#   unavailable   -> always returns 503
#   timeout       -> hangs (tests client timeout handling)
#   intermittent  -> random failures
TRANSFORM_FAILURE_MODE=${TRANSFORM_FAILURE_MODE:-unavailable}

# Keep this low so the "timeout" failure mode doesn't stall for 100s.
TRANSFORM_TIMEOUT_SECONDS=${TRANSFORM_TIMEOUT_SECONDS:-5}

# We verify that, while continuous writes are happening, the checkpoint stops advancing.
STALL_GRACE_SECONDS=${STALL_GRACE_SECONDS:-5}
STALL_WINDOW_SECONDS=${STALL_WINDOW_SECONDS:-10}

CHECKPOINT_FILE=${CHECKPOINT_FILE:-"/tmp/replicator-checkpoint"}
REPLICATOR_LOG=${REPLICATOR_LOG:-"/tmp/replicator-test-http-transform.log"}

cleanup() {
  echo ""
  echo "🧹 Cleaning up..."

  if [[ -n "${CONTINUOUS_PID:-}" ]]; then
    kill "$CONTINUOUS_PID" 2>/dev/null || true
  fi

  if [[ -n "${REPLICATOR_PID:-}" ]]; then
    kill "$REPLICATOR_PID" 2>/dev/null || true
  fi

  # Restore mock-transform to normal operation
  (
    cd "${TEST_DIR}"
    docker-compose stop mock-transform >/dev/null 2>&1 || true
    FAILURE_MODE=none docker-compose up -d mock-transform >/dev/null 2>&1 || true
  )

  echo "✓ Cleanup complete"
}
trap cleanup EXIT

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
  tail -80 "${REPLICATOR_LOG}" || true
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
      tail -120 "${REPLICATOR_LOG}" || true
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
      grep -E "(HTTP transform failed|Transformation request failed|Failed to transform|Replicator cycle crashed|Channel shovel crashed)" "${REPLICATOR_LOG}" | tail -120 || tail -120 "${REPLICATOR_LOG}" || true
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

set_mock_transform_mode() {
  local mode=$1

  cd "${TEST_DIR}"
  docker-compose stop mock-transform >/dev/null
  FAILURE_MODE=$mode docker-compose up -d mock-transform >/dev/null
}

# Reset state from previous runs
rm -f "${CHECKPOINT_FILE}" "${REPLICATOR_LOG}" /tmp/continuous-writes.log

# Ensure transform is in normal mode initially
set_mock_transform_mode none

echo "Starting continuous event generation..."
cd "${TEST_DIR}"
./scripts/continuous-writes.sh > /tmp/continuous-writes.log 2>&1 &
CONTINUOUS_PID=$!
echo "✓ Continuous writes started (PID: $CONTINUOUS_PID)"

sleep 2

echo "Starting replicator (HTTP transform timeout ${TRANSFORM_TIMEOUT_SECONDS}s)..."
cd "${ROOT_DIR}/src/replicator"
REPLICATOR_TRANSFORM_TIMEOUTSECONDS="${TRANSFORM_TIMEOUT_SECONDS}" nohup dotnet run --no-build > "${REPLICATOR_LOG}" 2>&1 &
REPLICATOR_PID=$!
echo "✓ Replicator started (PID: $REPLICATOR_PID)"

wait_for_checkpoint

BASE_POS=$(get_checkpoint_pos)
if [[ -z "$BASE_POS" ]]; then
  echo "❌ Unable to read checkpoint position"
  exit 1
fi

wait_for_checkpoint_advance "$BASE_POS" 60
POS_BEFORE_OUTAGE=$(get_checkpoint_pos)

echo ""
echo "════════════════════════════════════════════════════════"
echo "🔴 TRIGGERING HTTP TRANSFORM FAILURE (${TRANSFORM_FAILURE_MODE}) FOR ${OUTAGE_SECONDS}s"
echo "════════════════════════════════════════════════════════"

set_mock_transform_mode "${TRANSFORM_FAILURE_MODE}"

# Give the replicator time to hit the failing transform and stop making progress.
sleep "${STALL_GRACE_SECONDS}"

POS_STALL_START=$(get_checkpoint_pos)
if [[ -z "$POS_STALL_START" ]]; then
  echo "❌ Unable to read checkpoint position during outage"
  exit 1
fi

echo "Checkpoint at start of stall window: ${POS_STALL_START}"
sleep "${STALL_WINDOW_SECONDS}"
POS_STALL_END=$(get_checkpoint_pos)
echo "Checkpoint at end of stall window:   ${POS_STALL_END}"

if [[ "$POS_STALL_END" != "$POS_STALL_START" ]]; then
  echo "❌ Checkpoint advanced during transform outage (expected it to stall)"
  echo "This indicates events may be getting dropped or bypassing the transform failure path."
  echo "Recent replicator logs:"
  tail -160 "${REPLICATOR_LOG}" || true
  exit 1
fi

echo "✓ Checkpoint stalled during transform outage"

# Keep the service in failure mode for the remainder of the outage window.
remaining=$((OUTAGE_SECONDS - STALL_GRACE_SECONDS - STALL_WINDOW_SECONDS))
if [[ $remaining -gt 0 ]]; then
  sleep "$remaining"
fi

echo ""
echo "🟢 Restoring HTTP transform service to normal operation..."
set_mock_transform_mode none

echo "✓ Transform restored, waiting for checkpoint to advance again"
wait_for_checkpoint_advance "$POS_STALL_END" "${RECOVERY_TIMEOUT_SECONDS}"

echo ""
echo "════════════════════════════════════════════════════════"
echo "✅ PASS"
echo "════════════════════════════════════════════════════════"
echo "Replicator blocked during HTTP transform outage and recovered without dropping events."
