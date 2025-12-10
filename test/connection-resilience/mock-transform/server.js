const http = require('http');

const PORT = process.env.PORT || 3000;
const FAILURE_MODE = process.env.FAILURE_MODE || 'none';
const FAILURE_RATE = parseFloat(process.env.FAILURE_RATE || '0.0');
const DELAY_MS = parseInt(process.env.DELAY_MS || '0');

let requestCount = 0;
let failureCount = 0;

function shouldFail() {
  if (FAILURE_MODE === 'none') return false;
  if (FAILURE_MODE === 'unavailable') return true;
  if (FAILURE_MODE === 'intermittent') return Math.random() < FAILURE_RATE;
  return false;
}

function log(message, data = {}) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    message,
    ...data
  }));
}

const server = http.createServer(async (req, res) => {
  requestCount++;
  
  // Health check endpoint
  if (req.url === '/health' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ 
      status: 'healthy',
      mode: FAILURE_MODE,
      requests: requestCount,
      failures: failureCount
    }));
    return;
  }

  // Stats endpoint
  if (req.url === '/stats' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      requestCount,
      failureCount,
      successCount: requestCount - failureCount,
      failureMode: FAILURE_MODE,
      failureRate: FAILURE_RATE,
      delayMs: DELAY_MS
    }));
    return;
  }

  // Transform endpoint
  if (req.method === 'POST') {
    // Simulate delay if configured
    if (DELAY_MS > 0) {
      await new Promise(resolve => setTimeout(resolve, DELAY_MS));
    }

    // Check if we should fail this request
    if (shouldFail()) {
      failureCount++;
      
      if (FAILURE_MODE === 'timeout') {
        log('Simulating timeout - hanging request', { requestId: requestCount });
        // Don't respond - let it timeout
        return;
      }
      
      log('Returning 503 Service Unavailable', { 
        requestId: requestCount,
        mode: FAILURE_MODE 
      });
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Service Unavailable' }));
      return;
    }

    // Normal transformation
    let body = '';
    req.on('data', chunk => {
      body += chunk.toString();
    });

    req.on('end', () => {
      try {
        const event = JSON.parse(body);
        
        log('Transforming event', {
          requestId: requestCount,
          eventType: event.EventType,
          streamName: event.StreamName
        });

        // Simple passthrough transformation (you can modify this)
        const transformed = {
          EventType: event.EventType,
          StreamName: event.StreamName,
          Payload: event.Payload,
          Metadata: event.Metadata
        };

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(transformed));
      } catch (error) {
        log('Error parsing event', { 
          requestId: requestCount,
          error: error.message 
        });
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid JSON' }));
      }
    });
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(PORT, () => {
  log('Mock transform service started', { 
    port: PORT,
    failureMode: FAILURE_MODE,
    failureRate: FAILURE_RATE,
    delayMs: DELAY_MS
  });
});

process.on('SIGTERM', () => {
  log('Shutting down gracefully');
  server.close(() => {
    process.exit(0);
  });
});
