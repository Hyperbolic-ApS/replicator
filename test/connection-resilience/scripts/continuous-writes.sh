#!/bin/bash
STREAM_NAME="continuous-test-stream-$(date +%s)"
COUNT=0

echo "Starting continuous event generation to stream: $STREAM_NAME"
echo "Press Ctrl+C to stop"
echo ""

while true; do
  # Write 10 events at a time
  for i in {1..10}; do
    COUNT=$((COUNT + 1))
    curl -s -X POST "http://localhost:2114/streams/$STREAM_NAME" \
      -H "Content-Type: application/vnd.eventstore.events+json" \
      -H "ES-EventType: ContinuousTestEvent" \
      -H "ES-EventId: $(uuidgen)" \
      -d "[{
        \"eventType\": \"ContinuousTestEvent\",
        \"data\": {
          \"eventNumber\": $COUNT,
          \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
          \"payload\": \"$(head -c 100 /dev/urandom | base64)\"
        }
      }]" > /dev/null
  done
  
  # Short delay between batches (but not long enough for replicator to catch up)
  sleep 0.5
  
  if [ $((COUNT % 100)) -eq 0 ]; then
    echo "Written $COUNT events... ($(date +%H:%M:%S))"
  fi
done
