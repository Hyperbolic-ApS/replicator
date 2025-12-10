#!/bin/bash
set -e

TOXIPROXY_URL="http://localhost:8474"
PROXY_NAME="sink-eventstore"
DURATION=${1:-30}

echo "🔥 Triggering TCP connection failure on sink EventStore for $DURATION seconds..."
echo ""

# Add timeout toxic (simulates connection timeout)
echo "Adding timeout toxic..."
curl -s -X POST "$TOXIPROXY_URL/proxies/$PROXY_NAME/toxics" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"timeout_toxic\",
    \"type\": \"timeout\",
    \"attributes\": {
      \"timeout\": 1000
    }
  }" | jq '.'

echo ""
echo "✓ TCP connection will timeout for $DURATION seconds"
echo "  The replicator should experience write failures..."
echo ""

sleep $DURATION

echo "Removing timeout toxic..."
curl -s -X DELETE "$TOXIPROXY_URL/proxies/$PROXY_NAME/toxics/timeout_toxic" > /dev/null

echo "✓ TCP connection restored"
echo "  The replicator should recover and resume replication..."
