#!/bin/bash
set -e

echo "Populating source EventStore with test events..."

SOURCE_URL="http://localhost:2114"
STREAM_NAME="test-stream-$(date +%s)"
EVENT_COUNT=${1:-100}

echo "Creating $EVENT_COUNT events in stream: $STREAM_NAME"

for i in $(seq 1 $EVENT_COUNT); do
  EVENT_ID=$(uuidgen)
  EVENT_DATA="{\"eventNumber\": $i, \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"data\": \"Test event $i\"}"
  
  curl -s -X POST \
    "$SOURCE_URL/streams/$STREAM_NAME" \
    -H "Content-Type: application/vnd.eventstore.events+json" \
    -H "ES-EventType: TestEvent" \
    -H "ES-EventId: $EVENT_ID" \
    -d "[{
      \"eventId\": \"$EVENT_ID\",
      \"eventType\": \"TestEvent\",
      \"data\": $EVENT_DATA
    }]" > /dev/null
  
  if [ $((i % 10)) -eq 0 ]; then
    echo "  Created $i/$EVENT_COUNT events..."
  fi
done

echo "✓ Successfully created $EVENT_COUNT events in stream $STREAM_NAME"
echo ""
echo "Stream details:"
echo "  Name: $STREAM_NAME"
echo "  URL: $SOURCE_URL/streams/$STREAM_NAME"
