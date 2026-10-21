using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using TelemetryCore.Contracts;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Dlq;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.Worker.Ingestion;

/// <summary>
/// Single consume loop (§6): reads Kafka and hands each record to the bounded channel of its partition; a full channel
/// makes the loop wait (backpressure). Partition processors are created on assign and drained on revoke/stop.
/// </summary>
public sealed partial class IngestionService : BackgroundService
{
    /// <summary>Upper bound for draining processors on revoke (D10) and on shutdown.</summary>
    internal static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(15);

    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan ConsumeErrorBackoff = TimeSpan.FromMilliseconds(500);

    private readonly WorkerOptions _options;
    private readonly IBatchCommitter _committer;
    private readonly IDedupeHint _dedupe;
    private readonly IDlqPublisher _dlq;
    private readonly IProcessingObserver _observer;
    private readonly ChannelDepthRegistry _channels;
    private readonly VehicleStateCache _cache;
    private readonly IngestionStatus _status;
    private readonly TimeProvider _clock;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<IngestionService> _log;

    // Touched only by the consume thread (loop + rebalance handlers, which librdkafka runs inside Consume/Close).
    private readonly Dictionary<int, PartitionWorker> _workers = new();
    private IOffsetSink? _offsetSink;

    public IngestionService(IOptions<WorkerOptions> options, IBatchCommitter committer, IDedupeHint dedupe, IDlqPublisher dlq,
        IProcessingObserver observer, ChannelDepthRegistry channels, VehicleStateCache cache, IngestionStatus status, TimeProvider clock,
        ILoggerFactory loggerFactory)
    {
        _options = options.Value;
        _committer = committer;
        _dedupe = dedupe;
        _dlq = dlq;
        _observer = observer;
        _channels = channels;
        _cache = cache;
        _status = status;
        _clock = clock;
        _loggerFactory = loggerFactory;
        _log = loggerFactory.CreateLogger<IngestionService>();
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        // Consume() blocks for up to 100 ms; keep it off the host startup path.
        Task.Run(() => RunAsync(stoppingToken), CancellationToken.None);

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        try
        {
            await ConsumeLoopAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
        {
            LogLoopCrashed(_log, ex); // readiness turns red
            throw;
        }
        finally
        {
            _status.SetRunning(false);
        }
    }

    private async Task ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        using var consumer = BuildConsumer();
        _offsetSink = new ConsumerOffsetSink(consumer, _options.Topic, _loggerFactory.CreateLogger<ConsumerOffsetSink>());
        consumer.Subscribe(_options.Topic);
        LogStarted(_log, _options.Topic, _options.GroupId);
        _status.SetRunning(true);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? result;
                try
                {
                    result = consumer.Consume(ConsumeTimeout);
                }
                catch (ConsumeException ex)
                {
                    LogConsumeError(_log, ex, ex.Error.Reason);
                    await Task.Delay(ConsumeErrorBackoff, _clock, stoppingToken).ConfigureAwait(false); // avoid a log flood
                    continue;
                }

                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }

                if (!_workers.TryGetValue(result.Partition.Value, out var worker))
                {
                    // Not ours any more; the offset is not stored, so the owner replays it.
                    LogUnknownPartition(_log, result.Partition.Value, result.Offset.Value);
                    continue;
                }

                try
                {
                    _observer.OnConsumed();
                }
                catch (Exception ex)
                {
                    LogObserverFailed(_log, ex);
                }

                var msg = new PartitionMessage(result.Partition.Value, result.Offset.Value, result.Message.Key, result.Message.Value,
                    ParentContext(result.Message.Headers), _clock.GetUtcNow());
                await worker.Channel.Writer.WriteAsync(msg, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Stop requested while waiting on a full channel; that message was not handed over and will be replayed.
        }
        finally
        {
            await StopAllAsync(consumer).ConfigureAwait(false);
        }
    }

    private IConsumer<string, byte[]> BuildConsumer()
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.GroupId,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1000,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            // Deliberate (D11): a DB outage longer than this stalls the loop on a full channel and triggers a rebalance.
            MaxPollIntervalMs = 300_000,
        };

        return new ConsumerBuilder<string, byte[]>(config)
            .SetErrorHandler((_, e) => LogKafkaError(_log, e.Code, e.Reason, e.IsFatal))
            .SetPartitionsAssignedHandler((_, partitions) => OnAssigned(partitions))
            .SetPartitionsRevokedHandler((c, partitions) => OnRevoked(c, partitions, lost: false))
            .SetPartitionsLostHandler((c, partitions) => OnRevoked(c, partitions, lost: true))
            .Build();
    }

    private void OnAssigned(List<TopicPartition> partitions)
    {
        foreach (var tp in partitions)
        {
            var partition = tp.Partition.Value;
            if (_workers.ContainsKey(partition))
            {
                continue;
            }

            // Never trust memory from an earlier ownership of this partition: reload from the DB (D10).
            _cache.EvictPartition(partition);
            var channel = Channel.CreateBounded<PartitionMessage>(new BoundedChannelOptions(_options.ChannelCapacity)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = true,
                SingleWriter = true,
            });
            var abort = new CancellationTokenSource();
            var processor = new PartitionProcessor(partition, channel.Reader, _committer, _dedupe, _dlq, _offsetSink!, _observer, _clock,
                _options, _loggerFactory.CreateLogger<PartitionProcessor>());
            var task = Task.Run(() => processor.RunAsync(abort.Token), CancellationToken.None);
            _workers[partition] = new PartitionWorker(channel, task, abort);
            _channels.Register(partition, channel.Reader);
        }

        LogAssigned(_log, partitions.Count, _workers.Count);
    }

    /// <summary>
    /// Runs on the consume thread inside Consume()/Close(). Drains the processors of the revoked partitions before
    /// returning, because offsets of unassigned partitions cannot be stored. The only blocking wait in the worker (D10).
    /// </summary>
    private void OnRevoked(IConsumer<string, byte[]> consumer, List<TopicPartitionOffset> partitions, bool lost)
    {
        var drained = new List<(int Partition, PartitionWorker Worker)>(partitions.Count);
        foreach (var tpo in partitions)
        {
            if (_workers.Remove(tpo.Partition.Value, out var worker))
            {
                _channels.Unregister(tpo.Partition.Value);
                worker.Channel.Writer.TryComplete();
                drained.Add((tpo.Partition.Value, worker));
            }
        }

        if (drained.Count > 0)
        {
            var tasks = new Task[drained.Count];
            for (var i = 0; i < drained.Count; i++)
            {
                tasks[i] = drained[i].Worker.Task;
            }

            if (!Task.WaitAll(tasks, DrainTimeout))
            {
                LogDrainTimeout(_log, drained.Count);
                foreach (var (_, worker) in drained)
                {
                    worker.Abort.Cancel();
                }
            }
        }

        if (!lost)
        {
            CommitStored(consumer);
        }

        foreach (var (partition, worker) in drained)
        {
            _cache.EvictPartition(partition);
            if (worker.Task.IsCompleted)
            {
                worker.Abort.Dispose();
            }
        }

        LogRevoked(_log, partitions.Count, lost, _workers.Count);
    }

    /// <summary>Graceful stop (§7): stop consuming → drain channels → flush last batches → commit offsets → close.</summary>
    private async Task StopAllAsync(IConsumer<string, byte[]> consumer)
    {
        var tasks = new List<Task>(_workers.Count);
        foreach (var worker in _workers.Values)
        {
            worker.Channel.Writer.TryComplete();
            tasks.Add(worker.Task);
        }

        try
        {
            await Task.WhenAll(tasks).WaitAsync(DrainTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            LogDrainTimeout(_log, tasks.Count);
            foreach (var worker in _workers.Values)
            {
                worker.Abort.Cancel();
            }
        }

        foreach (var worker in _workers.Values)
        {
            if (worker.Task.IsCompleted)
            {
                worker.Abort.Dispose();
            }
        }

        foreach (var partition in _workers.Keys)
        {
            _channels.Unregister(partition);
        }

        _workers.Clear();
        CommitStored(consumer);
        try
        {
            consumer.Close(); // leaves the group; the revoke handler finds nothing left to drain
        }
        catch (KafkaException ex)
        {
            LogKafkaError(_log, ex.Error.Code, ex.Error.Reason, ex.Error.IsFatal);
        }

        LogStopped(_log);
    }

    private void CommitStored(IConsumer<string, byte[]> consumer)
    {
        try
        {
            consumer.Commit();
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_NoOffset)
        {
            // Nothing new stored since the last commit.
        }
        catch (KafkaException ex)
        {
            LogCommitFailed(_log, ex, ex.Error.Reason);
        }
    }

    private static ActivityContext ParentContext(Headers? headers)
    {
        if (headers is not null && headers.TryGetLastBytes(TelemetryHeaders.TraceParent, out var bytes) && bytes is { Length: > 0 }
            && ActivityContext.TryParse(Encoding.ASCII.GetString(bytes), null, out var ctx))
        {
            return ctx;
        }

        return default;
    }

    private sealed record PartitionWorker(Channel<PartitionMessage> Channel, Task Task, CancellationTokenSource Abort);

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming {Topic} as group {GroupId}")]
    private static partial void LogStarted(ILogger log, string topic, string groupId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Assigned {Count} partitions; now owning {Total}")]
    private static partial void LogAssigned(ILogger log, int count, int total);

    [LoggerMessage(Level = LogLevel.Information, Message = "Revoked {Count} partitions (lost: {Lost}); now owning {Total}")]
    private static partial void LogRevoked(ILogger log, int count, bool lost, int total);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Count} processors did not drain in time; aborting them (their batches will be replayed)")]
    private static partial void LogDrainTimeout(ILogger log, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Record for unassigned partition {Partition} at offset {Offset} skipped")]
    private static partial void LogUnknownPartition(ILogger log, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Consume error: {Reason}")]
    private static partial void LogConsumeError(ILogger log, Exception ex, string reason);

    [LoggerMessage(Level = LogLevel.Error, Message = "Offset commit failed: {Reason}")]
    private static partial void LogCommitFailed(ILogger log, Exception ex, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kafka error {Code}: {Reason} (fatal: {IsFatal})")]
    private static partial void LogKafkaError(ILogger log, ErrorCode code, string reason, bool isFatal);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Processing observer failed in the consume loop; consuming continues")]
    private static partial void LogObserverFailed(ILogger log, Exception ex);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Consume loop crashed")]
    private static partial void LogLoopCrashed(ILogger log, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Ingestion stopped; offsets committed and consumer closed")]
    private static partial void LogStopped(ILogger log);
}
