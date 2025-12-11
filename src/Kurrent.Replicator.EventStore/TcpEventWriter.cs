using Kurrent.Replicator.Shared.Contracts;
using Kurrent.Replicator.Shared.Logging;
using Kurrent.Replicator.Shared.Observe;
using Ubiquitous.Metrics;
using StreamMetadata = EventStore.ClientAPI.StreamMetadata;

// ReSharper disable SuggestBaseTypeForParameter
namespace Kurrent.Replicator.EventStore;

public class TcpEventWriter(IEventStoreConnection connection, int writeTimeoutSeconds = 30) : IEventWriter {
    static readonly ILog Log = LogProvider.GetCurrentClassLogger();

    public Task Start() => connection.ConnectAsync();

    public async Task<long> WriteEvent(BaseProposedEvent proposedEvent, CancellationToken cancellationToken) {
        // Create a timeout cancellation token to prevent indefinite hangs
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(TimeSpan.FromSeconds(writeTimeoutSeconds));
        
        try {
            var task = proposedEvent switch {
                ProposedEvent p             => Append(p, timeoutCts.Token),
                ProposedMetaEvent meta      => SetMeta(meta, timeoutCts.Token),
                ProposedDeleteStream delete => Delete(delete, timeoutCts.Token),
                IgnoredEvent _              => Task.FromResult(-1L),
                _                           => throw new InvalidOperationException("Unknown proposed event type")
            };

            return await Metrics.Measure(() => task, ReplicationMetrics.WritesHistogram, ReplicationMetrics.WriteErrorsCount);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested) {
            // Write operation timed out (not user cancellation)
            Log.Error(
                "Write operation timed out after {Timeout}s for event {EventId} to stream {Stream}",
                writeTimeoutSeconds,
                proposedEvent.EventDetails.EventId,
                proposedEvent.EventDetails.Stream
            );
            throw new TimeoutException($"Write operation timed out after {writeTimeoutSeconds} seconds");
        }

        async Task<long> Append(ProposedEvent p, CancellationToken ct) {
            if (Log.IsDebugEnabled()) {
                Log.Debug(
                    "TCP: Write event with id {Id} of type {Type} to {Stream} with original position {Position}",
                    proposedEvent.EventDetails.EventId,
                    proposedEvent.EventDetails.EventType,
                    proposedEvent.EventDetails.Stream,
                    proposedEvent.SourceLogPosition.EventPosition
                );
            }

            var result = await connection.AppendToStreamAsync(p.EventDetails.Stream, ExpectedVersion.Any, Map(p))
                .WaitAsync(ct)
                .ConfigureAwait(false);

            return result.LogPosition.CommitPosition;
        }

        async Task<long> SetMeta(ProposedMetaEvent meta, CancellationToken ct) {
            if (Log.IsDebugEnabled())
                Log.Debug(
                    "TCP: Setting metadata to {Stream} with original position {Position}",
                    proposedEvent.EventDetails.Stream,
                    proposedEvent.SourceLogPosition.EventPosition
                );

            var result = await connection.SetStreamMetadataAsync(
                    meta.EventDetails.Stream,
                    ExpectedVersion.Any,
                    StreamMetadata.Create(
                        meta.Data.MaxCount,
                        meta.Data.MaxAge,
                        meta.Data.TruncateBefore,
                        meta.Data.CacheControl,
                        new(
                            meta.Data.StreamAcl?.ReadRoles,
                            meta.Data.StreamAcl?.WriteRoles,
                            meta.Data.StreamAcl?.DeleteRoles,
                            meta.Data.StreamAcl?.MetaReadRoles,
                            meta.Data.StreamAcl?.MetaWriteRoles
                        )
                    )
                )
                .WaitAsync(ct)
                .ConfigureAwait(false);

            return result.LogPosition.CommitPosition;
        }

        async Task<long> Delete(ProposedDeleteStream delete, CancellationToken ct) {
            if (Log.IsDebugEnabled()) {
                Log.Debug(
                    "TCP: Deleting stream {Stream} with original position {Position}",
                    proposedEvent.EventDetails.Stream,
                    proposedEvent.SourceLogPosition.EventPosition
                );
            }

            var result = await connection.DeleteStreamAsync(delete.EventDetails.Stream, ExpectedVersion.Any)
                .WaitAsync(ct)
                .ConfigureAwait(false);

            return result.LogPosition.CommitPosition;
        }

        static EventData Map(ProposedEvent evt) => new(
            evt.EventDetails.EventId,
            evt.EventDetails.EventType,
            evt.EventDetails.ContentType == ContentTypes.Json,
            evt.Data,
            evt.Metadata
        );
    }
}
