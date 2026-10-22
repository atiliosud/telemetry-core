using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using TelemetryCore.Domain;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Dlq;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.Worker.Ingestion;

/// <summary>
/// Sequential processor of one partition (R3). Collects up to <see cref="WorkerOptions.BatchSize"/> messages or
/// <see cref="WorkerOptions.FlushInterval"/> since the first one, validates them (invalid ones go to the DLQ),
/// commits the valid ones in one DB transaction and only then stores the next offset (D9).
/// A failed commit keeps the batch and retries it until it succeeds or <c>ct</c> is cancelled (D11).
/// The dedupe hint (D1) is checked before the commit (known events are skipped and counted as duplicates) and
/// marked only after it; a failing hint never blocks the commit or the offset.
/// </summary>
public sealed partial class PartitionProcessor
{
    internal static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);

    private readonly int _partition;
    private readonly ChannelReader<PartitionMessage> _reader;
    private readonly IBatchCommitter _committer;
    private readonly IDedupeHint _dedupe;
    private readonly IDlqPublisher _dlq;
    private readonly IOffsetSink _offsets;
    private readonly IProcessingObserver _observer;
    private readonly TimeProvider _clock;
    private readonly WorkerOptions _options;
    private readonly ILogger _log;
    private readonly Tracing.LinkCollector _links = new();
    private readonly IFaultInjector _faults;
    private readonly Action? _onFatal;

    public PartitionProcessor(int partition, ChannelReader<PartitionMessage> reader, IBatchCommitter committer, IDedupeHint dedupe,
        IDlqPublisher dlq, IOffsetSink offsets, IProcessingObserver observer, TimeProvider clock, WorkerOptions o, ILogger log, IFaultInjector? faults = null, Action? onFatal = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(o.BatchSize, 1);
        _partition = partition;
        _reader = reader;
        _committer = committer;
        _dedupe = dedupe;
        _dlq = dlq;
        _offsets = offsets;
        _observer = observer;
        _clock = clock;
        _options = o;
        _log = log;
        _faults = faults ?? new NoopFaultInjector();
        _onFatal = onFatal;
    }

    /// <summary>Runs until the channel completes (remaining messages are flushed) or <paramref name="ct"/> is cancelled (nothing more is stored).</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        var buffer = ArrayPool<TelemetryEvent>.Shared.Rent(_options.BatchSize);
        var offsets = ArrayPool<long>.Shared.Rent(_options.BatchSize); // Kafka offset of each buffered event, for logs
        var batch = new Batch(buffer, offsets);
        try
        {
            var completed = false;
            while (!completed && await _reader.WaitToReadAsync(ct).ConfigureAwait(false))
            {
                if (!_reader.TryRead(out var first))
                {
                    continue;
                }

                var count = 0;
                var messages = 0;
                count = await AddAsync(first, batch, count, ct).ConfigureAwait(false);
                messages++;
                var lastOffset = first.Offset;

                using (var flushTimer = new CancellationTokenSource(_options.FlushInterval, _clock))
                using (var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, flushTimer.Token))
                {
                    while (messages < _options.BatchSize)
                    {
                        if (_reader.TryRead(out var next))
                        {
                            count = await AddAsync(next, batch, count, ct).ConfigureAwait(false);
                            messages++;
                            lastOffset = next.Offset;
                            continue;
                        }

                        if (flushTimer.IsCancellationRequested)
                        {
                            break;
                        }

                        try
                        {
                            if (!await _reader.WaitToReadAsync(linked.Token).ConfigureAwait(false))
                            {
                                completed = true; // channel completed: flush what we have and return
                                break;
                            }
                        }
                        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                        {
                            break; // flush interval elapsed
                        }
                    }
                }

                await FlushAsync(batch, count, messages, lastOffset, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            LogAborted(_log, _partition);
        }
        catch (SimulatedCrashException ex)
        {
            // Test hook (SPEC §16): die after the DB commit, before the offset is stored. The batch is replayed by the next owner.
            LogSimulatedCrash(_log, ex, _partition);
            _onFatal?.Invoke();
        }
        finally
        {
            ArrayPool<TelemetryEvent>.Shared.Return(buffer, clearArray: true); // drop string references
            ArrayPool<long>.Shared.Return(offsets);
        }
    }

    private async ValueTask<int> AddAsync(PartitionMessage msg, Batch batch, int count, CancellationToken ct)
    {
        _links.Add(msg.ParentContext);
        if (EventValidator.TryParse(msg.Value, _clock.GetUtcNow(), out var evt, out var reason)
            && !IsKeyMismatch(msg.Key, evt.VehicleId, out reason))
        {
            batch.Events[count] = evt;
            batch.Offsets[count] = msg.Offset;
            return count + 1;
        }

        // D17: the DLQ write is awaited before the offset of this batch can be stored.
        while (true)
        {
            try
            {
                await _dlq.PublishAsync(msg, reason!, ct).ConfigureAwait(false);
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                LogDlqFailed(_log, ex, _partition, msg.Offset);
                await Task.Delay(RetryDelay, _clock, ct).ConfigureAwait(false);
            }
        }

        LogSentToDlq(_log, msg.Key, _partition, msg.Offset, reason!);

        // Outside the retry loop: an observer failure must never republish the message.
        try
        {
            _observer.OnDlq(reason!);
        }
        catch (Exception ex)
        {
            LogObserverFailed(_log, ex, _partition);
        }

        return count;
    }

    /// <summary>D29: the Kafka key decides the partition; a different payload vehicle id would split one vehicle over two partitions.</summary>
    private static bool IsKeyMismatch(string? key, string vehicleId, out string? reason)
    {
        if (key is not null && !string.Equals(key, vehicleId, StringComparison.Ordinal))
        {
            reason = DlqReasons.KeyMismatch;
            return true;
        }

        reason = null;
        return false;
    }

    private async Task FlushAsync(Batch batch, int count, int messages, long lastOffset, CancellationToken ct)
    {
        if (count == 0)
        {
            _links.Clear();
            _offsets.Store(_partition, lastOffset + 1); // only invalid messages, all already in the DLQ
            return;
        }

        var started = _clock.GetTimestamp();
        using (var span = _links.StartBatch(_partition, messages))
        {
            _links.Clear();
            await CommitBatchAsync(batch, count, lastOffset, span, ct).ConfigureAwait(false);
        }

        RecordBatchDuration(_clock.GetElapsedTime(started).TotalSeconds);
        _offsets.Store(_partition, lastOffset + 1);
    }

    private async Task CommitBatchAsync(Batch batch, int count, long lastOffset, Activity? span, CancellationToken ct)
    {
        var buffer = batch.Events;
        var valid = count;
        count = await DropKnownAsync(batch, count, ct).ConfigureAwait(false);
        var knownByHint = valid - count;
        if (count == 0)
        {
            LogAllKnown(_log, _partition, knownByHint, lastOffset);
            span?.SetTag("duplicates", knownByHint);
            NotifyBatch(0, knownByHint, 0, 0, 0);
        }
        else
        {
            var events = new ReadOnlyMemory<TelemetryEvent>(buffer, 0, count);
            var attempt = 0;
            PersistResult result;
            while (true)
            {
                try
                {
                    result = await _committer.CommitAsync(events, _partition, ct).ConfigureAwait(false);
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
                {
                    attempt++;
                    LogCommitFailed(_log, ex, _partition, count, attempt);
                    await Task.Delay(RetryDelay, _clock, ct).ConfigureAwait(false);
                }
            }

            await _faults.AfterDbCommitAsync(_partition, ct).ConfigureAwait(false);
            var duplicates = knownByHint + result.Duplicates;
            LogBatchCommitted(_log, _partition, count, result.Inserted, duplicates, lastOffset);
            span?.SetTag("inserted", result.Inserted);
            span?.SetTag("duplicates", duplicates);
            span?.SetTag("commit.attempts", attempt + 1);
            NotifyBatch(result.Inserted, duplicates, result.OutOfOrder, result.GpsJumps, result.Alerts.Count);
            RecordLatencies(buffer, count);

            // D1: keys only for committed events. Every event of the batch is in the DB now (new or duplicate).
            await MarkKnownAsync(buffer, count, ct).ConfigureAwait(false);
        }
    }

    private void RecordBatchDuration(double seconds)
    {
        try
        {
            _observer.RecordBatchDuration(seconds);
        }
        catch (Exception ex)
        {
            LogObserverFailed(_log, ex, _partition);
        }
    }

    /// <summary>Now - event timestamp for every committed event of the batch (the generator stamps send time, D16).</summary>
    private void RecordLatencies(TelemetryEvent[] buffer, int count)
    {
        try
        {
            var now = _clock.GetUtcNow();
            for (var i = 0; i < count; i++)
            {
                _observer.RecordLatency((now - buffer[i].TimestampUtc).TotalSeconds);
            }
        }
        catch (Exception ex)
        {
            LogObserverFailed(_log, ex, _partition);
        }
    }

    /// <summary>The batch is already committed: an observer failure is logged and must not stop the offset from being stored.</summary>
    private void NotifyBatch(int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts)
    {
        try
        {
            _observer.OnBatchCommitted(_partition, inserted, duplicates, outOfOrder, gpsJumps, alerts);
        }
        catch (Exception ex)
        {
            LogObserverFailed(_log, ex, _partition);
        }
    }

    /// <summary>Removes events the hint already knows as committed; keeps batch order. Hint failures are ignored (the DB key decides).</summary>
    private async ValueTask<int> DropKnownAsync(Batch batch, int count, CancellationToken ct)
    {
        var buffer = batch.Events;
        var ids = ArrayPool<Guid>.Shared.Rent(count);
        try
        {
            for (var i = 0; i < count; i++)
            {
                ids[i] = buffer[i].EventId;
            }

            bool[] known;
            try
            {
                known = await _dedupe.AreKnownAsync(new ReadOnlyMemory<Guid>(ids, 0, count), ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                LogHintFailed(_log, ex, _partition);
                return count;
            }

            var kept = 0;
            for (var i = 0; i < count; i++)
            {
                if (i < known.Length && known[i])
                {
                    LogDuplicate(_log, buffer[i].VehicleId, _partition, batch.Offsets[i], buffer[i].EventId);
                    continue;
                }

                batch.Offsets[kept] = batch.Offsets[i];
                buffer[kept++] = buffer[i];
            }

            return kept;
        }
        finally
        {
            ArrayPool<Guid>.Shared.Return(ids, clearArray: true);
        }
    }

    private async ValueTask MarkKnownAsync(TelemetryEvent[] buffer, int count, CancellationToken ct)
    {
        var ids = ArrayPool<Guid>.Shared.Rent(count);
        try
        {
            for (var i = 0; i < count; i++)
            {
                ids[i] = buffer[i].EventId;
            }

            await _dedupe.MarkAsync(new ReadOnlyMemory<Guid>(ids, 0, count), ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            LogHintFailed(_log, ex, _partition);
        }
        finally
        {
            ArrayPool<Guid>.Shared.Return(ids, clearArray: true);
        }
    }

    /// <summary>Buffers of one partition processor: events and the Kafka offset of each, index-aligned.</summary>
    private sealed record Batch(TelemetryEvent[] Events, long[] Offsets);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message for vehicle {vehicleId} at partition {partition} offset {offset} sent to the DLQ: {reason}")]
    private static partial void LogSentToDlq(ILogger log, string? vehicleId, int partition, long offset, string reason);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Duplicate event {eventId} for vehicle {vehicleId} at partition {partition} offset {offset} skipped (dedupe hint)")]
    private static partial void LogDuplicate(ILogger log, string vehicleId, int partition, long offset, Guid eventId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Partition {Partition}: committed {Count} events ({Inserted} new; {Duplicates} duplicates incl. hint hits) up to offset {LastOffset}")]
    private static partial void LogBatchCommitted(ILogger log, int partition, int count, int inserted, int duplicates, long lastOffset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Partition {Partition}: all {Count} events already known by the hint, up to offset {LastOffset}")]
    private static partial void LogAllKnown(ILogger log, int partition, int count, long lastOffset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Partition {Partition}: batch of {Count} events failed to commit (attempt {Attempt}); keeping it and retrying")]
    private static partial void LogCommitFailed(ILogger log, Exception ex, int partition, int count, int attempt);

    [LoggerMessage(Level = LogLevel.Error, Message = "Partition {Partition}: DLQ publish failed for offset {Offset}; retrying")]
    private static partial void LogDlqFailed(ILogger log, Exception ex, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Partition}: dedupe hint failed; falling back to the DB key")]
    private static partial void LogHintFailed(ILogger log, Exception ex, int partition);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Partition}: processing observer failed; processing continues")]
    private static partial void LogObserverFailed(ILogger log, Exception ex, int partition);

    [LoggerMessage(Level = LogLevel.Critical, Message = "Partition {Partition}: simulated crash after the DB commit; the offset is not stored")]
    private static partial void LogSimulatedCrash(ILogger log, Exception ex, int partition);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Partition}: processor aborted; the unstored batch will be replayed")]
    private static partial void LogAborted(ILogger log, int partition);
}
