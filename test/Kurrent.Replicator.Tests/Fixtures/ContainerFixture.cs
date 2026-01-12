using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using EventStore.Client;
using EventStore.ClientAPI;
using Testcontainers.EventStoreDb;

namespace Kurrent.Replicator.Tests.Fixtures;

public class ContainerFixture {
    EventStoreDbContainer _kurrentDbContainer;
    EventStoreDbContainer _eventStoreContainer;
    public DirectoryInfo  V5DataPath { get; private set; }

    public async Task StartContainers() {
        V5DataPath           = Directory.CreateTempSubdirectory();
        _kurrentDbContainer  = BuildV23Container();
        _eventStoreContainer = BuildV23ContainerForTcp(V5DataPath);

        await _kurrentDbContainer.StartAsync();
        await _eventStoreContainer.StartAsync();
        await Task.Delay(TimeSpan.FromSeconds(2)); // give it some time to spin up
    }

    public async Task StopContainers() {
        await Task.WhenAll(_kurrentDbContainer.StopAsync(), _eventStoreContainer.StopAsync());
        await _eventStoreContainer.DisposeAsync();
        await _kurrentDbContainer.DisposeAsync();
    }

    public IEventStoreConnection GetV5Client() {
        var port             = _eventStoreContainer.GetMappedPublicPort(1113);
        var connectionString = $"ConnectTo=tcp://admin:changeit@localhost:{port}; HeartBeatTimeout=500; UseSslConnection=false;";
        var client           = ConfigureEventStoreTcp(connectionString);

        return client;
    }

    public EventStoreClient GetKurrentClient() {
        var connectionString = _kurrentDbContainer.GetConnectionString();
        var settings         = EventStoreClientSettings.Create(connectionString);

        return new(settings);
    }

    static EventStoreDbContainer BuildV23Container() => new EventStoreDbBuilder()
        .WithImage("eventstore/eventstore:24.10")
        .WithEnvironment("EVENTSTORE_RUN_PROJECTIONS", "None")
        .WithEnvironment("EVENTSTORE_START_STANDARD_PROJECTIONS", "false")
        .WithEnvironment("EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", bool.TrueString)
        .Build();

    static EventStoreDbContainer BuildV23ContainerForTcp(DirectoryInfo data) => new EventStoreDbBuilder()
        .WithImage("eventstore/eventstore:23.10.1-bookworm-slim")
        .WithEnvironment("EVENTSTORE_CLUSTER_SIZE", "1")
        .WithEnvironment("EVENTSTORE_RUN_PROJECTIONS", "None")
        .WithEnvironment("EVENTSTORE_START_STANDARD_PROJECTIONS", "false")
        .WithEnvironment("EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", bool.TrueString)
        .WithEnvironment("EVENTSTORE_ENABLE_EXTERNAL_TCP", bool.TrueString)
        .WithEnvironment("EVENTSTORE_INSECURE", bool.TrueString)
        .WithEnvironment("EVENTSTORE_EXT_TCP_PORT", "1113")
        .WithPortBinding(1113, true)
        .WithBindMount(data.FullName, "/var/lib/eventstore")
        .Build();

    static IEventStoreConnection ConfigureEventStoreTcp(string connectionString) {
        var builder = ConnectionSettings.Create()
            .KeepReconnecting()
            .KeepRetrying();

        var connection = EventStoreConnection.Create(connectionString, builder);

        return connection;
    }
}
