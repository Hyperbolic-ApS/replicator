#!/bin/bash
set -e

echo "════════════════════════════════════════════════════════"
echo "🧪 DEADLOCK REPRODUCTION TEST - 60s Timeout"
echo "════════════════════════════════════════════════════════"
echo ""
echo "This test will:"
echo "1. Start replicator with 60s heartbeat timeout"
echo "2. Begin continuous event writes"
echo "3. Trigger clean TCP closure during active writes"
echo "4. Monitor for deadlock (checkpoint not advancing)"
echo ""

# Cleanup function
cleanup() {
  echo ""
  echo "🧹 Cleaning up..."
  kill $CONTINUOUS_PID 2>/dev/null || true
  kill $REPLICATOR_PID 2>/dev/null || true
  curl -s -X POST "http://localhost:8474/proxies/sink-eventstore" \
    -H "Content-Type: application/json" \
    -d '{"enabled": true}' > /dev/null
  echo "✓ Cleanup complete"
}
trap cleanup EXIT

# Ensure proxy is enabled
curl -s -X POST "http://localhost:8474/proxies/sink-eventstore" \
  -H "Content-Type: application/json" \
  -d '{"enabled": true}' > /dev/null

# Clear checkpoint
rm -f /tmp/replicator-checkpoint
echo "✓ Cleared checkpoint"

# Start replicator
echo "Starting replicator with 60s heartbeat timeout..."
cd ../../src/replicator
nohup dotnet run --no-build > /tmp/replicator-test.log 2>&1 &
REPLICATOR_PID=$!
echo $REPLICATOR_PID > /tmp/replicator-test.pid
echo "✓ Replicator started (PID: $REPLICATOR_PID)"
cd ../../test/connection-resilience

# Wait for replicator to initialize
echo "⏳ Waiting 10 seconds for replicator to initialize..."
sleep 10

# Get initial checkpoint
INITIAL_CHECKPOINT=$(grep "Reached the end" /tmp/replicator-test.log | tail -1 | grep -oE "[0-9]+" | tail -1)
echo "✓ Initial checkpoint: $INITIAL_CHECKPOINT"
echo ""

# Start continuous event generation
echo "Starting continuous event generation..."
./scripts/continuous-writes.sh > /tmp/continuous-writes.log 2>&1 &
CONTINUOUS_PID=$!
echo "✓ Continuous writes started (PID: $CONTINUOUS_PID)"

# Wait for events to accumulate
echo "⏳ Waiting 30 seconds for events to accumulate..."
sleep 30

# Check current position
CURRENT_POS=$(grep "Reached the end" /tmp/replicator-test.log | tail -1 | grep -oE "[0-9]+" | tail -1)
echo "✓ Current checkpoint: $CURRENT_POS (replicated $((CURRENT_POS - INITIAL_CHECKPOINT)) events)"
echo ""

# Trigger clean closure
echo "════════════════════════════════════════════════════════"
echo "🔴 TRIGGERING CLEAN TCP CLOSURE"
echo "════════════════════════════════════════════════════════"
CLOSURE_TIME=$(date +%H:%M:%S)
echo "Time: $CLOSURE_TIME"

curl -s -X POST "http://localhost:8474/proxies/sink-eventstore" \
  -H "Content-Type: application/json" \
  -d '{"enabled": false}' > /dev/null

echo "✓ Proxy disabled - TCP connection closing cleanly"
echo ""
echo "Monitoring for deadlock (checkpoint position should freeze)..."
echo "With 60s timeout, if deadlock occurs, position won't advance for minutes."
echo ""

# Monitor checkpoint position for 2 minutes
MONITOR_START=$(date +%s)
LAST_POS=$CURRENT_POS
STUCK_COUNT=0

for i in {1..24}; do  # 24 x 5s = 2 minutes
  sleep 5
  ELAPSED=$(($(date +%s) - MONITOR_START))
  
  # Get latest checkpoint
  NEW_POS=$(grep "Reached the end" /tmp/replicator-test.log | tail -1 | grep -oE "[0-9]+" | tail -1 || echo "$LAST_POS")
  
  if [ "$NEW_POS" == "$LAST_POS" ]; then
    STUCK_COUNT=$((STUCK_COUNT + 1))
    echo "[$ELAPSED s] ⚠️  Position STUCK at $NEW_POS (stuck for $((STUCK_COUNT * 5))s)"
  else
    echo "[$ELAPSED s] ✓ Position advancing: $LAST_POS → $NEW_POS"
    STUCK_COUNT=0
  fi
  
  LAST_POS=$NEW_POS
  
  # If stuck for more than 30 seconds, we have a deadlock
  if [ $STUCK_COUNT -ge 6 ]; then
    echo ""
    echo "════════════════════════════════════════════════════════"
    echo "🔴 DEADLOCK DETECTED!"
    echo "════════════════════════════════════════════════════════"
    echo "Checkpoint position stuck at $NEW_POS for ${STUCK_COUNT}x5 = $((STUCK_COUNT * 5)) seconds"
    echo ""
    echo "Recent replicator logs:"
    tail -20 /tmp/replicator-test.log | grep -E "(Socket|Connection|Reached|TCP|Write)" || tail -20 /tmp/replicator-test.log
    echo ""
    echo "✅ DEADLOCK REPRODUCED - This matches production behavior!"
    exit 0
  fi
done

echo ""
echo "════════════════════════════════════════════════════════"
echo "✅ NO DEADLOCK DETECTED"
echo "════════════════════════════════════════════════════════"
echo "Checkpoint continued advancing despite connection closure."
echo "Final position: $LAST_POS"
echo ""
echo "This suggests the deadlock may require different conditions."
