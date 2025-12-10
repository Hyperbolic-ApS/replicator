# Connection Resilience Test Environment

This test environment is designed to reproduce and debug connection resilience issues in the replicator, specifically:

1. **TCP Connection Drops**: Sink EventStore TCP connections that close and fail to recover
2. **HTTP Transform Failures**: HTTP transformation endpoint becoming unavailable

## Architecture

The test environment mirrors your production setup:

```
┌─────────────┐                    ┌────────────────┐
│  Source     │    gRPC (21141)    │   Replicator   │
│  EventStore │◄───────────────────┤                │
│  (KurrentDB)│                    │  - Reads via   │
└─────────────┘                    │    gRPC        │
                                   │  - Transforms  │
┌─────────────┐                    │    via HTTP    │
│  Mock HTTP  │    HTTP (3000)     │  - Writes via  │
│  Transform  │◄───────────────────┤    TCP         │
└─────────────┘                    └────────────────┘
                                           │ TCP (11131)
                                           ▼
                                   ┌────────────────┐
                                   │  Sink          │
                                   │  EventStore    │
                                   │  (TCP v5)      │
                                   └────────────────┘

All connections go through Toxiproxy for network chaos testing
```

## Components

- **source-esdb**: KurrentDB 25.0 (gRPC source) - port 2114
- **sink-eventstore**: EventStore 5.0.8 (TCP sink) - ports 1113 (TCP), 2113 (HTTP)
- **mock-transform**: Node.js HTTP service with controllable failure modes - port 3000
- **toxiproxy**: Network chaos proxy - port 8474 (API), 21141 (ESDB proxy), 11131 (TCP proxy)

## Prerequisites

- Docker and Docker Compose
- .NET 9.0 SDK
- `jq` (for JSON parsing in scripts)
- `curl`

## Setup

1. **Start the test environment:**

   ```bash
   cd test/connection-resilience
   docker-compose up -d
   ```

   Wait for all services to be healthy (about 30 seconds):

   ```bash
   docker-compose ps
   ```

2. **Build the replicator:**

   ```bash
   cd ../../  # Back to project root
   dotnet build
   ```

3. **Create logs directory:**

   ```bash
   mkdir -p test/connection-resilience/logs
   mkdir -p test/connection-resilience/checkpoint
   ```

## Running Tests

### Test 1: TCP Connection Resilience (Issue from fail-1.log)

This test simulates periodic TCP connection drops to the sink EventStore.

1. **Start the replicator:**

   ```bash
   cd src/replicator
   dotnet run --no-build -- --config ../../test/connection-resilience/config/appsettings.yaml
   ```

2. **In another terminal, populate source with test events:**

   ```bash
   cd test/connection-resilience
   ./scripts/populate-source.sh 200  # Creates 200 test events
   ```

3. **In another terminal, trigger TCP connection failure:**

   ```bash
   cd test/connection-resilience
   ./scripts/trigger-tcp-failure.sh 60  # Fails for 60 seconds
   ```

4. **Observe the behavior:**
   - Watch replicator logs for TCP connection errors
   - Check if replicator recovers after connection is restored
   - Verify events are eventually replicated to sink

5. **Verify sink received events:**

   ```bash
   curl -s http://localhost:2113/streams/\$all | grep "test-stream"
   ```

### Test 2: HTTP Transform Endpoint Failures (Issue from fail-2.log)

This test simulates the HTTP transform endpoint becoming unavailable.

1. **Start the replicator** (if not already running)

2. **Populate source with events:**

   ```bash
   cd test/connection-resilience
   ./scripts/populate-source.sh 100
   ```

3. **Trigger HTTP transform failure:**

   ```bash
   cd test/connection-resilience
   ./scripts/trigger-http-failure.sh unavailable 60  # Returns 503 for 60 seconds
   ```

   Available failure modes:
   - `unavailable`: Always returns 503 Service Unavailable
   - `timeout`: Hangs without responding (tests timeout handling)
   - `intermittent`: Random failures (use FAILURE_RATE=0.5 for 50%)

4. **Check mock transform stats:**

   ```bash
   curl http://localhost:3000/stats
   ```

5. **Observe the behavior:**
   - Watch for retry attempts in logs
   - Check if replicator gets stuck on the same event
   - Verify recovery after transform service is restored

### Test 3: Combined Failure Scenario

Simulate both TCP and HTTP failures simultaneously:

1. Start replicator and populate events
2. Trigger both failures in separate terminals:
   ```bash
   ./scripts/trigger-tcp-failure.sh 90
   ./scripts/trigger-http-failure.sh unavailable 90
   ```
3. Observe how the replicator handles multiple simultaneous failures

## Manual Network Chaos with Toxiproxy

You can manually control network conditions via Toxiproxy API:

### List all proxies:
```bash
curl http://localhost:8474/proxies | jq '.'
```

### Add latency to sink TCP connection:
```bash
curl -X POST http://localhost:8474/proxies/sink-eventstore/toxics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "latency",
    "type": "latency",
    "attributes": {"latency": 1000}
  }'
```

### Simulate bandwidth limit:
```bash
curl -X POST http://localhost:8474/proxies/sink-eventstore/toxics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "bandwidth",
    "type": "bandwidth",
    "attributes": {"rate": 10}
  }'
```

### Reset connection:
```bash
curl -X POST http://localhost:8474/proxies/sink-eventstore/toxics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "reset_peer",
    "type": "reset_peer",
    "attributes": {"timeout": 0}
  }'
```

### Remove toxic:
```bash
curl -X DELETE http://localhost:8474/proxies/sink-eventstore/toxics/latency
```

## Monitoring

### Replicator logs:
```bash
tail -f test/connection-resilience/logs/replicator-*.log
```

### Mock transform service logs:
```bash
docker logs -f test-mock-transform
```

### Check replication progress:

**Source event count:**
```bash
curl -s http://localhost:2114/streams/\$all | jq '.entries | length'
```

**Sink event count:**
```bash
curl -s http://localhost:2113/streams/\$all | jq '.entries | length'
```

### Transform service stats:
```bash
curl http://localhost:3000/stats | jq '.'
```

## Expected Issues to Reproduce

### Issue 1: TCP Connection Not Recovering (fail-1.log)
- **Symptom**: TCP connections close cleanly but replicator doesn't properly reconnect
- **Test**: Run Test 1 and observe if replication resumes after TCP failure
- **Log pattern**: `TcpPackageConnection: connection [...] was closed "cleanly"`

### Issue 2: HTTP Transform Gets Stuck (fail-2.log)
- **Symptom**: After HTTP transform retries are exhausted, replicator restarts but gets stuck on same event
- **Test**: Run Test 2 and observe retry behavior and recovery
- **Log pattern**: `Transformation request failed: Service Unavailable` → retries → restart → same event

## Cleanup

Stop and remove all containers:
```bash
cd test/connection-resilience
docker-compose down -v
```

Clean up logs and checkpoints:
```bash
rm -rf logs/* checkpoint/*
```

## Troubleshooting

### Ports already in use:
If ports are already allocated, stop the main docker-compose:
```bash
cd ../../  # Back to project root
docker-compose down
```

### Services not starting:
Check service health:
```bash
docker-compose ps
docker-compose logs <service-name>
```

### Can't connect to EventStore:
Ensure the services are fully started:
```bash
docker-compose logs source-esdb
docker-compose logs sink-eventstore
```

Wait for "is master" or "leader elected" messages.

## Next Steps

Once issues are reproduced:

1. Document the exact behavior observed
2. Identify root causes in the code
3. Implement fixes with proper:
   - Connection lifecycle management
   - Circuit breaker patterns
   - Exponential backoff
   - Timeout configurations
4. Re-run tests to verify fixes
