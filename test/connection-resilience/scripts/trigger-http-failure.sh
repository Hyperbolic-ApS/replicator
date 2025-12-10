#!/bin/bash
set -e

FAILURE_MODE=${1:-unavailable}
DURATION=${2:-30}

echo "🔥 Triggering HTTP transform failure (mode: $FAILURE_MODE) for $DURATION seconds..."
echo ""

# Stop and restart the mock-transform service with failure mode
cd "$(dirname "$0")/.."

echo "Stopping mock-transform service..."
docker-compose stop mock-transform

echo "Starting mock-transform service in failure mode: $FAILURE_MODE"
FAILURE_MODE=$FAILURE_MODE docker-compose up -d mock-transform

echo ""
echo "✓ HTTP transform service will return errors for $DURATION seconds"
echo "  Mode: $FAILURE_MODE"
echo "  The replicator should experience transformation failures..."
echo ""
echo "  Check stats: curl http://localhost:3000/stats"
echo ""

sleep $DURATION

echo "Restoring HTTP transform service to normal operation..."
docker-compose stop mock-transform
FAILURE_MODE=none docker-compose up -d mock-transform

echo "✓ HTTP transform service restored"
echo "  The replicator should recover and resume replication..."
