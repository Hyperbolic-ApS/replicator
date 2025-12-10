# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

Kurrent Replicator is an event replication tool that copies events in real-time from one EventStoreDB or KurrentDB instance/cluster to another. It supports filtering, transforming events, and propagating stream metadata and deletions.

### Supported Protocols
- **Readers and Writers**: KurrentDB gRPC, EventStoreDB gRPC (v20+), EventStore TCP (v4+)
- **Writers only**: Kafka, MongoDB, HTTP

## Build and Development Commands

### Building

```bash
# Build all projects
dotnet build

# Build Docker image (amd64)
docker build .

# Build Docker image for ARM64 (Apple Silicon)
docker build --build-arg RUNTIME=linux-arm64 .
```

### Testing

```bash
# Run all tests
dotnet test

# Tests use TUnit framework and require Docker for test containers (EventStoreDB)
# Test results are output to test-results/{TargetFramework}/
```

### Local Development

```bash
# Start dependencies (EventStoreDB, KurrentDB, MongoDB, Kafka)
docker-compose up

# Run the application (from src/replicator directory)
cd src/replicator
dotnet run

# Application exposes:
# - HTTP API and UI on http://localhost:5000
# - Prometheus metrics on /metrics endpoint
```

### Frontend Development

The web UI is built with Vue 3 and Vite:

```bash
cd src/replicator/ClientApp

# Install dependencies
yarn install

# Development mode
yarn dev

# Build for production
yarn build
```

## Architecture

### Three-Stage Pipeline

The replicator uses a three-stage pipeline architecture with `System.Threading.Channels` for async communication:

```
┌────────┐    ┌─────────┐    ┌──────┐
│ Reader │ -> │ Prepare │ -> │ Sink │
└────────┘    └─────────┘    └──────┘
     |             |             |
     v             v             v
  IEventReader  Filter &    IEventWriter
  (protocol)   Transform   (protocol)
```

1. **Reader Pipe** (`Kurrent.Replicator/Read/ReaderPipe.cs`):
   - Reads events from source via `IEventReader` implementations
   - Manages checkpoint position tracking
   - Pushes events to Prepare channel

2. **Prepare Pipe** (`Kurrent.Replicator/Prepare/PreparePipe.cs`):
   - Applies event filters (drop events based on criteria)
   - Executes event transformations (JavaScript-based or built-in)
   - Uses GreenPipes for retry logic and concurrency control
   - Pushes transformed events to Sink channel

3. **Sink Pipe** (`Kurrent.Replicator/Sink/SinkPipe.cs`):
   - Writes events to destination via `IEventWriter` implementations
   - Supports partitioning for parallel writes
   - Updates checkpoint after successful writes

### Protocol System

Each protocol is implemented as a separate module with a configurator:

- **Kurrent.Replicator.EventStore**: TCP reader/writer for EventStore v4+
- **Kurrent.Replicator.KurrentDb**: gRPC reader/writer for KurrentDB/EventStoreDB v20+
- **Kurrent.Replicator.Kafka**: Kafka writer with topic routing
- **Kurrent.Replicator.Mongo**: MongoDB writer
- **Kurrent.Replicator.Http**: HTTP writer
- **Kurrent.Replicator.Js**: JavaScript transform and partitioner support (Jint engine)

Protocol registration happens via `IConfigurator` instances in DI:
```csharp
services.AddSingleton<IConfigurator, TcpConfigurator>();
services.AddSingleton<IConfigurator, GrpcConfigurator>();
```

The `Factory` class (`Kurrent.Replicator/Factories.cs`) resolves the appropriate reader/writer based on the protocol string in configuration.

### Configuration System

Configuration is loaded from multiple sources in this order (later sources override earlier):
1. `config/appsettings.yaml` (mounted in Docker or local file)
2. Environment variables (prefixed with `REPLICATOR_`)

Key configuration structure (`src/replicator/Settings/ReplicatorSettings.cs`):
```yaml
replicator:
  reader:
    protocol: tcp|grpc  # Reader protocol
    connectionString: "..."
    pageSize: 1024  # Events per read batch
  sink:
    protocol: tcp|grpc|kafka|mongo|http
    connectionString: "..."
    partitionCount: 1  # Parallel write partitions
    bufferSize: 1000  # Channel buffer size
  transform:
    type: js|default
    config: ./transform.js  # Path to JS file
    bufferSize: 1  # Transform pipeline buffer
  filters: []  # Event filters
  checkpoint:
    type: file|mongo
    path: "./checkpoint"
    checkpointAfter: 1000  # Events between checkpoints
```

### Checkpoint System

The checkpoint system tracks replication progress:

- **File-based**: Stores position in a local file (`FileCheckpointStore`)
- **MongoDB-based**: Stores position in MongoDB for multi-instance deployments (`MongoCheckpointStore`)
- Checkpoints are written asynchronously after N events (configurable)
- **Checkpoint Seeder**: Can initialize from another source (e.g., "chaser" reads from EventStore's checkpoint)

### Event Filtering and Transformation

**Filters** (`src/replicator/Settings/Filters.cs`):
- Return `true` to keep event, `false` to drop
- Built-in filters: stream name patterns, event type patterns
- Custom filters can be implemented via `FilterEvent` delegate

**Transformers** (`src/replicator/Settings/Transformers.cs`):
- JavaScript transformers use Jint engine (`Kurrent.Replicator.Js`)
- Transform function signature: `function transform(original) { return {...}; }`
- Can modify stream name, event type, data, and metadata
- Returning `undefined` drops the event
- Example in `src/replicator/config/transform.js`

**Partitioners** (for parallel sink processing):
- Default: Hash by stream name
- JavaScript partitioners: Custom logic to assign partition keys
- Used when `sink.partitionCount > 1`

### Key Dependencies

- **GreenPipes**: Pipeline composition with retry, concurrency, and partitioning middleware
- **Jint**: JavaScript interpreter for transforms and partitioners
- **Serilog**: Structured logging (JSON in production, text in development)
- **prometheus-net**: Metrics exposed at `/metrics` endpoint
- **Ubiquitous.Metrics**: Abstraction over Prometheus metrics
- **YamlDotNet**: YAML configuration parsing

## Code Conventions

Based on `.editorconfig`:

- **Braces**: End-of-line style (not new line)
- **Namespaces**: File-scoped (`namespace Foo;` instead of `namespace Foo { }`)
- **Expression bodies**: Preferred for properties, accessors, methods where appropriate
- **Var usage**: Preferred for type-obvious declarations
- **Max line length**: 200 characters
- **Naming**: 
  - PascalCase for types, public members
  - camelCase for parameters, local variables
  - `_camelCase` for private fields
- **Null checking**: Use `is not null` pattern
- **Blank lines**: Required before control transfer statements, around methods

## Project Structure

```
src/
├── Kurrent.Replicator/           # Core pipeline logic
│   ├── Read/                     # Reader pipe
│   ├── Prepare/                  # Filter & transform pipe
│   └── Sink/                     # Writer pipe
├── Kurrent.Replicator.Shared/    # Interfaces & contracts
│   ├── Contracts/                # Event data structures
│   └── Pipeline/                 # Pipeline abstractions
├── Kurrent.Replicator.EventStore/    # TCP protocol implementation
├── Kurrent.Replicator.KurrentDb/     # gRPC protocol implementation
├── Kurrent.Replicator.Kafka/         # Kafka sink
├── Kurrent.Replicator.Mongo/         # MongoDB sink & checkpoint store
├── Kurrent.Replicator.Http/          # HTTP sink
├── Kurrent.Replicator.Js/            # JavaScript transform engine
└── replicator/                       # ASP.NET Core host application
    ├── ClientApp/                    # Vue.js frontend
    ├── HttpApi/                      # REST API controllers
    ├── Settings/                     # Configuration models
    └── config/                       # Sample configurations

test/
└── Kurrent.Replicator.Tests/    # TUnit tests with Testcontainers
```

## Deployment

### Docker
The application is containerized with multi-stage builds:
- Base: .NET 9.0 SDK for building
- Runtime: .NET 9.0 ASP.NET runtime
- Frontend built during Docker build (Node.js 22 + Yarn)
- Exposes port 5000

### Kubernetes/Helm
Helm charts are available in `charts/replicator/` for Kubernetes deployment.
Example values in `example/values.yml`.

### Environment Variables
- `REPLICATOR_DEBUG=true`: Enable debug logging
- `ALLOWED_HOSTS`: Allowed host headers (default: `*`)
- Configuration can be overridden via environment variables (e.g., `REPLICATOR_READER__CONNECTIONSTRING`)

## Development Notes

- The main application entry point is `src/replicator/Program.cs`
- Background replication runs via `ReplicatorService` (ASP.NET Core hosted service)
- The application can run continuously (`RunContinuously: true`) or stop after catching up
- On failure, it can restart automatically (`RestartOnFailure: true`) after a configurable delay
- Metrics are reported every 5 seconds by default to Prometheus

## Testing Notes

- Tests use TUnit framework (not xUnit/NUnit)
- Testcontainers are used to spin up EventStoreDB instances
- Test results are output in TRX format to `test-results/` directory
- Tests require Docker to be running locally
