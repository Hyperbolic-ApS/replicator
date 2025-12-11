# Test Plan: Clean Connection Closure Investigation

## Hypothesis

**Why production fails but manual restart fixes it:**

When a TCP connection closes cleanly during runtime:
1. The EventStore TCP client reconnects at the transport layer
2. But the replicator's sink pipeline doesn't properly resume after reconnection
3. Events may be stuck in channels or write operations may be blocked
4. Manual restart clears this state by recreating the entire pipeline

## Key Differences: Production vs Our Tests

| Aspect | Our Tests (Working) | Production (Failing) |
|--------|-------------------|---------------------|
| Failure Type | Forced errors (timeout, reset_peer) | Clean closures ([Success] "Socket closed") |
| Trigger | Toxiproxy toxics | Server-initiated (idle timeout?) |
| Exception Thrown | Yes (SocketError) | No (clean Success) |
| Recovery | Automatic ✅ | Hangs ❌ |

## Test Scenarios

### Scenario 1: Clean Closure During Idle Period
**Setup:**
- Start replicator with continuous replication
- Wait for replication to catch up (no active writes)
- Disable Toxiproxy proxy (clean closure)
- Wait 10 seconds
- Re-enable proxy

**Expected (correct) behavior:**
- Connection reconnects
- Replicator resumes processing
- No stuck state

**If it matches production bug:**
- Connection reconnects
- But no new events are processed
- Sink pipeline appears frozen

### Scenario 2: Clean Closure During Active Writes
**Setup:**
- Start continuous event generation (populate-source.sh in loop)
- While events are being written, disable proxy
- Wait 10 seconds
- Re-enable proxy

**Expected (correct) behavior:**
- Active write may fail
- Retry logic kicks in
- Writes resume after reconnection

**If it matches production bug:**
- Write operation hangs
- No exception thrown
- Sink pipeline blocked

### Scenario 3: Repeated Clean Closures (Production Pattern)
**Setup:**
- Simulate production's pattern: multiple clean closures over time
- 5 cycles of: 30s normal operation → 10s closure → reconnect

**Expected (correct) behavior:**
- Replicator survives all cycles
- All events eventually replicated

**If it matches production bug:**
- May fail on first closure, or accumulate problems over multiple cycles

## Test Execution

### Prerequisites
```bash
cd /Users/kasperwelner/Projects/replicator/test/connection-resilience
docker-compose up -d
# Wait for all services healthy
```

### Test 1: Single Clean Closure
```bash
# Terminal 1: Start replicator
cd ../../src/replicator
dotnet run --no-build

# Terminal 2: Populate initial events
cd ../../test/connection-resilience
./scripts/populate-source.sh 50

# Terminal 3: Monitor logs for specific patterns
docker logs -f test-replicator 2>&1 | grep -E "(Socket closed|connected to|Reached the end)"

# Terminal 4: Trigger clean closure after catching up
./scripts/trigger-clean-closure.sh 1 10
```

**Observations to make:**
- [ ] Connection closes cleanly (look for "Socket closed" message)
- [ ] Connection reconnects (look for "TcpPackageConnection: connected to")
- [ ] SSL handshake completes
- [ ] **CRITICAL:** Does "Reached the end of stream" appear after reconnection?
- [ ] Are new events (added during closure) replicated?

### Test 2: Clean Closure During Active Writes
```bash
# Terminal 1: Start replicator
# Terminal 2: Continuous event generation
while true; do
  ./scripts/populate-source.sh 10
  sleep 2
done

# Terminal 3: Trigger closure during active writes
sleep 10  # Let some events accumulate
./scripts/trigger-clean-closure.sh 1 15
```

**Observations to make:**
- [ ] Are writes actively happening when closure occurs?
- [ ] Do write operations timeout or hang?
- [ ] Are there any error messages in logs?
- [ ] Does replication resume after reconnection?

### Test 3: Multiple Cycles (Production Simulation)
```bash
# Terminal 1: Start replicator with verbose logging
REPLICATOR_DEBUG=true dotnet run --no-build

# Terminal 2: Continuous slow event generation
while true; do
  ./scripts/populate-source.sh 5
  sleep 5
done

# Terminal 3: Repeated clean closures
./scripts/trigger-clean-closure.sh 5 10
```

**Observations to make:**
- [ ] Does it survive all 5 cycles?
- [ ] Is there degradation over multiple cycles?
- [ ] Are all events eventually replicated?
- [ ] Check final checkpoint position

## Success Criteria

### If Test PASSES (doesn't reproduce bug):
- Replicator recovers from all clean closures
- All events are eventually replicated
- No manual restart needed

→ **Next step:** Investigate what's different in production environment:
- Network/firewall configuration?
- EventStore server version or configuration?
- Different connection parameters?
- Longer idle periods in production?

### If Test FAILS (reproduces bug):
- Replicator hangs after clean closure
- Events stop being processed
- Sink pipeline appears frozen
- Manual restart fixes it

→ **Next step:** We've reproduced it! Now investigate:
1. TCP client connection state after clean closure
2. Channel state (are events stuck in sink channel?)
3. Write operation state (is a write blocked/hanging?)
4. Add diagnostic logging to identify exact point of failure

## Diagnostic Questions to Answer

If bug is reproduced:

1. **Connection State:**
   - Is `IEventStoreConnection.ConnectAsync()` called again after closure?
   - Does the connection report as "Connected" after reconnection?
   
2. **Channel State:**
   - Are events accumulating in the sink channel? (Check `sinkChannel.Reader.Count`)
   - Is the prepare→sink channel working?
   
3. **Pipeline State:**
   - Is the sink pipe "shovel" task still running?
   - Is it waiting on channel reads?
   - Is it blocked on a write operation?

4. **Writer State:**
   - What is the state of the last write operation?
   - Is there a pending write that never completed?
   - Does the TCP client have any pending operations?

## Code Areas to Investigate

If bug is reproduced:

1. **`TcpEventWriter.WriteEvent()`** - Does it properly handle connection state?
2. **`IEventStoreConnection`** reconnection behavior after clean closures
3. **`SinkPipe.Send()`** retry logic and exception handling
4. **Channel shoveling** in `Replicator.Replicate()` - could it deadlock?

## Expected Log Patterns

### Normal (Working):
```
Socket closed
TcpPackageConnection: connected to [...]
[SSL handshake]
Reached the end of the stream at [position]
[continues processing new events]
```

### Bug (Stuck):
```
Socket closed
TcpPackageConnection: connected to [...]
[SSL handshake]
[...silence... no more activity...]
```

The key differentiator: **"Reached the end of the stream"** message after reconnection.
This comes from the *reader* (gRPC), not the writer (TCP sink). If it doesn't appear,
it suggests the sink pipeline is blocking the reader channel.
