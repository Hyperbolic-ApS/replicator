using System.Threading.Channels;
using Kurrent.Replicator.Shared;
using Kurrent.Replicator.Shared.Logging;
using Kurrent.Replicator.Shared.Observe;
using Kurrent.Replicator.Prepare;
using Kurrent.Replicator.Read;
using Kurrent.Replicator.Sink;
using Ubiquitous.Metrics;

namespace Kurrent.Replicator;

public static class Replicator {
    static readonly ILog Log = LogProvider.GetCurrentClassLogger();

    public static async Task Replicate(
            IEventReader           reader,
            IEventWriter           writer,
            SinkPipeOptions        sinkPipeOptions,
            PreparePipelineOptions preparePipeOptions,
            ICheckpointSeeder      checkpointSeeder,
            ICheckpointStore       checkpointStore,
            ReplicatorOptions      replicatorOptions,
            CancellationToken      stoppingToken
        ) {
        ReplicationMetrics.SetCapacity(preparePipeOptions.BufferSize, sinkPipeOptions.BufferSize);

        var reporter = Task.Run(Report, stoppingToken);

        await writer.Start();
        await checkpointSeeder.Seed(stoppingToken);

        var stopping = false;

        while (!stopping && !stoppingToken.IsCancellationRequested) {
            using var cycleCts       = new CancellationTokenSource();
            using var cycleLinkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, cycleCts.Token);

            var prepareChannel = Channel.CreateBounded<PrepareContext>(preparePipeOptions.BufferSize);
            var sinkChannel    = Channel.CreateBounded<SinkContext>(sinkPipeOptions.BufferSize);

            var readerPipe = new ReaderPipe(
                reader,
                checkpointStore,
                ctx => prepareChannel.Writer.WriteAsync(ctx, ctx.CancellationToken)
            );

            var preparePipe = new PreparePipe(
                preparePipeOptions.Filter,
                preparePipeOptions.Transform,
                ctx => sinkChannel.Writer.WriteAsync(ctx, ctx.CancellationToken)
            );

            var sinkPipe = new SinkPipe(writer, sinkPipeOptions, checkpointStore);

            var prepareTask = CreateChannelShovel(
                "Prepare",
                prepareChannel,
                preparePipe.Send,
                ReplicationMetrics.PrepareChannelSize,
                cycleLinkedCts.Token
            );

            var writerTask = CreateChannelShovel(
                "Writer",
                sinkChannel,
                sinkPipe.Send,
                ReplicationMetrics.SinkChannelSize,
                cycleLinkedCts.Token
            );

            try {
                ReplicationStatus.Start();

                var readerTask = readerPipe.Start(cycleLinkedCts.Token);

                var completed = await Task.WhenAny(readerTask, prepareTask, writerTask).ConfigureAwait(false);

                if (completed != readerTask) {
                    // A pipeline stage stopped early; treat as a failure so we can restart.
                    await completed.ConfigureAwait(false);
                }

                await readerTask.ConfigureAwait(false);

                stopping = cycleLinkedCts.IsCancellationRequested || !replicatorOptions.RunContinuously;

                if (stopping) {
                    Log.Info("Replicator stopping");
                }

                ReplicationStatus.Stop();

                // Stop producing new items for this cycle and let the pipeline drain.
                prepareChannel.Writer.TryComplete();

                while (!prepareTask.IsCompleted) {
                    await checkpointStore.Flush(CancellationToken.None).ConfigureAwait(false);

                    if (prepareTask.IsFaulted) {
                        throw new Exception("Prepare pipe faulted", prepareTask.Exception);
                    }

                    if (writerTask.IsFaulted) {
                        throw new Exception("Writer pipe faulted", writerTask.Exception);
                    }

                    await Task.Delay(1000, CancellationToken.None).ConfigureAwait(false);
                }

                await prepareTask.ConfigureAwait(false);

                sinkChannel.Writer.TryComplete();

                while (sinkChannel.Reader.Count > 0) {
                    await checkpointStore.Flush(CancellationToken.None).ConfigureAwait(false);

                    if (writerTask.IsFaulted) {
                        throw new Exception("Writer pipe faulted", writerTask.Exception);
                    }

                    Log.Info("Waiting for the sink pipe to exhaust ({Left} left)...", sinkChannel.Reader.Count);
                    await Task.Delay(1000, CancellationToken.None).ConfigureAwait(false);
                }

                await writerTask.ConfigureAwait(false);

                await Flush().ConfigureAwait(false);
            } catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) {
                // stopping now
                stopping = true;
            } catch (OperationCanceledException) when (cycleLinkedCts.IsCancellationRequested) {
                // cycle cancelled (restart unless host is stopping)
            } catch (Exception e) {
                Log.Error(e, "Replicator cycle crashed");

                // Cancel the cycle so blocked channel writes can unblock and we can restart cleanly.
                cycleCts.Cancel();
            } finally {
                ReplicationStatus.Stop();

                prepareChannel.Writer.TryComplete();
                sinkChannel.Writer.TryComplete();

                try {
                    await Task.WhenAll(prepareTask, writerTask).ConfigureAwait(false);
                } catch (OperationCanceledException) { } catch (Exception e) {
                    Log.Error(e, "Error stopping pending tasks");
                }

                await Flush().ConfigureAwait(false);
            }

            if (stopping) {
                break;
            }

            Log.Info("Will restart in {0} sec", replicatorOptions.RestartDelay.TotalSeconds);

            if (replicatorOptions.RestartDelay != TimeSpan.Zero) {
                try {
                    await Task.Delay(replicatorOptions.RestartDelay, stoppingToken);
                } catch (OperationCanceledException) {
                    break;
                }
            }
        }

        Log.Info("Replicator stopped");

        return;

        async Task Flush() {
            Log.Info("Storing the last known checkpoint");
            await checkpointStore.Flush(CancellationToken.None).ConfigureAwait(false);
        }

        static Task CreateChannelShovel<T>(
                string            name,
                Channel<T>        channel,
                Func<T, Task>     send,
                IGaugeMetric      size,
                CancellationToken token
            )
            => Task.Run(() => channel.Shovel(send, () => Log.Info($"{name} started"), () => Log.Info($"{name} stopped"), size, token), token);

        async Task Report() {
            try {
                while (!stoppingToken.IsCancellationRequested) {
                    var position = await reader.GetLastPosition(stoppingToken).ConfigureAwait(false);

                    if (position.HasValue) {
                        ReplicationMetrics.LastSourcePosition.Set(position.Value);
                    }

                    await Task.Delay(replicatorOptions.ReportMetricsFrequency, stoppingToken).ConfigureAwait(false);
                }
            } catch (OperationCanceledException) {
                // it's ok
            }

            Log.Info("Reporting stopped");
        }
    }
}

static class ChannelExtensions {
    static readonly ILog Log = LogProvider.GetCurrentClassLogger();

    public static async Task Shovel<T>(
            this Channel<T>   channel,
            Func<T, Task>     send,
            Action            beforeStart,
            Action            afterStop,
            IGaugeMetric      channelSizeGauge,
            CancellationToken token
        ) {
        beforeStart();

        try {
            while (!token.IsCancellationRequested         &&
                   !channel.Reader.Completion.IsCompleted &&
                   await channel.Reader.WaitToReadAsync(token).ConfigureAwait(false)) {
                await foreach (var ctx in channel.Reader.ReadAllAsync(token).ConfigureAwait(false)) {
                    await send(ctx).ConfigureAwait(false);
                    channelSizeGauge.Set(channel.Reader.Count);
                }
            }
        } catch (OperationCanceledException) {
            // it's ok
        } catch (Exception e) {
            Log.Error(e, "Channel shovel crashed");

            throw;
        } finally {
            afterStop();
        }
    }
}
