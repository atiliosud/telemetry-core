using System.Buffers;
using System.Threading.Channels;
using TelemetryCore.Domain;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Dlq;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.Worker.Ingestion;

/// <summary>
/// Sequential processor of one partition (R3). Collects up to <see cref="WorkerOptions.BatchSize"/> messages or
/// <see cref="WorkerOptions.FlushInterval"/> since the first one, validates them (invalid ones go to the DLQ),
/// commits the valid ones in one DB transaction and only then stores the next offset (D9).
/// A failed commit keeps the batch and retries it until it succeeds or <c>ct</c> is cancelled (D11).
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
    private readonly TimeProvider _clock;
    private readonly WorkerOptions _options;
    private readonly ILogger _log;

    public PartitionProcessor(int partition, ChannelReader<PartitionMessage> reader, IBatchCommitter committer, IDedupeHint dedupe,
        IDlqPublisher dlq, IOffsetSink offsets, TimeProvider clock, WorkerOptions o, ILogger log)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(o.BatchSize, 1);
        _partition = partition;
        _reader = reader;
        _committer = committer;
        _dedupe = dedupe;
        _dlq = dlq;
        _offsets = offsets;
        _clock = clock;
        _options = o;
        _log = log;
    }

    /// <summary>Runs until the channel completes (remaining messages are flushed) or <paramref name="ct"/> is cancelled (nothing more is stored).</summary>
    public async Task RunAsync(CancellationToken ct)
    {
        var buffer = ArrayPool<TelemetryEvent>.Shared.Rent(_options.BatchSize);
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
                count = await AddAsync(first, buffer, count, ct).ConfigureAwait(false);
                messages++;
                var lastOffset = first.Offset;

                using (var flushTimer = new CancellationTokenSource(_options.FlushInterval, _clock))
                using (var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, flushTimer.Token))
                {
                    while (messages < _options.BatchSize)
                    {
                        if (_reader.TryRead(out var next))
                        {
                            count = await AddAsync(next, buffer, count, ct).ConfigureAwait(false);
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

                await FlushAsync(buffer, count, lastOffset, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            LogAborted(_log, _partition);
        }
        finally
        {
            ArrayPool<TelemetryEvent>.Shared.Return(buffer, clearArray: true); // drop string references
        }
    }

    private async ValueTask<int> AddAsync(PartitionMessage msg, TelemetryEvent[] buffer, int count, CancellationToken ct)
    {
        if (EventValidator.TryParse(msg.Value, _clock.GetUtcNow(), out var evt, out var reason))
        {
            buffer[count] = evt;
            return count + 1;
        }

        // D17: the DLQ write is awaited before the offset of this batch can be stored.
        while (true)
        {
            try
            {
                await _dlq.PublishAsync(msg, reason!, ct).ConfigureAwait(false);
                return count;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                LogDlqFailed(_log, ex, _partition, msg.Offset);
                await Task.Delay(RetryDelay, _clock, ct).ConfigureAwait(false);
            }
        }
    }

    private async Task FlushAsync(TelemetryEvent[] buffer, int count, long lastOffset, CancellationToken ct)
    {
        if (count > 0)
        {
            count = await DropKnownAsync(buffer, count, ct).ConfigureAwait(false);
        }

        if (count > 0)
        {
            var events = new ReadOnlyMemory<TelemetryEvent>(buffer, 0, count);
            var attempt = 0;
            while (true)
            {
                try
                {
                    var result = await _committer.CommitAsync(events, _partition, ct).ConfigureAwait(false);
                    LogBatchCommitted(_log, _partition, count, result.Inserted, result.Duplicates, lastOffset);
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
                {
                    attempt++;
                    LogCommitFailed(_log, ex, _partition, count, attempt);
                    await Task.Delay(RetryDelay, _clock, ct).ConfigureAwait(false);
                }
            }

            await MarkKnownAsync(buffer, count, ct).ConfigureAwait(false);
        }

        _offsets.Store(_partition, lastOffset + 1);
    }

    /// <summary>Removes events the hint already knows as committed; keeps batch order. Hint failures are ignored (the DB key decides).</summary>
    private async ValueTask<int> DropKnownAsync(TelemetryEvent[] buffer, int count, CancellationToken ct)
    {
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
                    continue;
                }

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

    [LoggerMessage(Level = LogLevel.Debug, Message = "Partition {Partition}: committed {Count} events ({Inserted} new, {Duplicates} duplicates) up to offset {LastOffset}")]
    private static partial void LogBatchCommitted(ILogger log, int partition, int count, int inserted, int duplicates, long lastOffset);

    [LoggerMessage(Level = LogLevel.Error, Message = "Partition {Partition}: batch of {Count} events failed to commit (attempt {Attempt}); keeping it and retrying")]
    private static partial void LogCommitFailed(ILogger log, Exception ex, int partition, int count, int attempt);

    [LoggerMessage(Level = LogLevel.Error, Message = "Partition {Partition}: DLQ publish failed for offset {Offset}; retrying")]
    private static partial void LogDlqFailed(ILogger log, Exception ex, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Partition}: dedupe hint failed; falling back to the DB key")]
    private static partial void LogHintFailed(ILogger log, Exception ex, int partition);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Partition {Partition}: processor aborted; the unstored batch will be replayed")]
    private static partial void LogAborted(ILogger log, int partition);
}
