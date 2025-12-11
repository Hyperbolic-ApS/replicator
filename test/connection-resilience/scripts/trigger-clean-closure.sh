#!/bin/bash
set -e

TOXIPROXY_URL="http://localhost:8474"
PROXY_NAME="sink-eventstore"
CYCLE_COUNT=${1:-5}
CLOSURE_DURATION=${2:-10}

echo "🔌 Simulating server-initiated clean connection closures..."
echo "   Will cycle $CYCLE_COUNT times, each closure lasting $CLOSURE_DURATION seconds"
echo ""

for ((i=1; i<=CYCLE_COUNT; i++)); do
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "Cycle $i/$CYCLE_COUNT:"
  echo ""
  
  # Disable the proxy (cleanly closes connections)
  echo "  🔴 Disabling proxy (simulating server closing connection)..."
  curl -s -X POST "$TOXIPROXY_URL/proxies/$PROXY_NAME" \
    -H "Content-Type: application/json" \
    -d "{\"enabled\": false}" > /dev/null
  
  echo "  ✓ Proxy disabled - connection closed cleanly"
  echo "  ⏳ Waiting $CLOSURE_DURATION seconds..."
  echo ""
  
  sleep $CLOSURE_DURATION
  
  # Re-enable the proxy
  echo "  🟢 Re-enabling proxy (allowing reconnection)..."
  curl -s -X POST "$TOXIPROXY_URL/proxies/$PROXY_NAME" \
    -H "Content-Type: application/json" \
    -d "{\"enabled\": true}" > /dev/null
  
  echo "  ✓ Proxy re-enabled - connection can be re-established"
  
  if [ $i -lt $CYCLE_COUNT ]; then
    echo "  ⏳ Waiting 30 seconds before next closure..."
    echo ""
    sleep 30
  fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Test complete!"
echo ""
echo "Expected behavior:"
echo "  - Replicator should reconnect after each clean closure"
echo "  - Replication should resume automatically"
echo "  - No manual restart should be needed"
echo ""
echo "If replicator hangs and doesn't resume, this matches production fail-1.log!"
