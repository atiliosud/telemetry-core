using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Dlq;
using TelemetryCore.Worker.Ingestion;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.Worker.Tests;

public sealed class PartitionProcessorTests
{
    private const int Partition = 3;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 10, 21, 12, 0, 0, TimeSpan.Zero));
    private readonly Channel<PartitionMessage> _channel = Channel.CreateBounded<PartitionMessage>(
        new BoundedChannelOptions(1000) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = true });
    private readonly List<string> _log = new();
    private readonly FakeCommitter _committer;
    private readonly FakeDlq _dlq;
    private readonly FakeOffsets _offsets;
    private readonly WorkerOptions _options = new();
    private readonly RecordingObserver _observer = new();
    private long _nextOffset;

    public PartitionProcessorTests()
    {
        _committer = new FakeCommitter(_log);
        _dlq = new FakeDlq(_log);
        _offsets = new FakeOffsets(_log);
    }

    [Fact]
    public async Task FlushesAt500Events()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(500);

        await WaitUntilAsync(() => _committer.Calls.Count == 1);
        _committer.Calls[0].Should().HaveCount(500);
        await WaitUntilAsync(() => _offsets.Stored.Count == 1);
        _offsets.Stored[0].Should().Be((Partition, 500L));

        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);
        _committer.Calls.Should().HaveCount(1, "no clock time passed, so only the size trigger fired");
    }

    [Fact]
    public async Task FlushesAfter100msWithFewerEvents()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(10);
        await WaitUntilAsync(() => _channel.Reader.Count == 0); // the processor took them, so the flush timer is armed
        await Task.Delay(50);

        _clock.Advance(TimeSpan.FromMilliseconds(99));
        await Task.Delay(100);
        _committer.Calls.Should().BeEmpty("the flush interval has not elapsed yet");

        _clock.Advance(TimeSpan.FromMilliseconds(1));
        await WaitUntilAsync(() => _committer.Calls.Count == 1);
        _committer.Calls[0].Should().HaveCount(10);
        await WaitUntilAsync(() => _offsets.Stored.Count == 1);
        _offsets.Stored[0].Should().Be((Partition, 10L));

        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);
    }

    [Fact]
    public async Task StoresOffsetOnlyAfterCommitSucceeds()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _committer.Gate = gate.Task;
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(500);
        await WaitUntilAsync(() => _committer.Calls.Count == 1);
        await Task.Delay(100);
        _offsets.Stored.Should().BeEmpty("the batch transaction has not committed yet");

        gate.SetResult();
        await WaitUntilAsync(() => _offsets.Stored.Count == 1);
        _offsets.Stored[0].Should().Be((Partition, 500L));

        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);
    }

    [Fact]
    public async Task CommitFailure_RetriesSameBatch_NoOffsetStored()
    {
        _committer.FailuresBeforeSuccess = 1;
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(500);
        await WaitUntilAsync(() => _committer.Calls.Count == 1);
        await Task.Delay(50);
        _offsets.Stored.Should().BeEmpty("the first commit failed");

        // The retry waits 1 s on the injected clock.
        await WaitUntilAsync(() => _committer.Calls.Count == 2, () => _clock.Advance(TimeSpan.FromMilliseconds(250)));
        _committer.Calls[1].Should().Equal(_committer.Calls[0]);
        await WaitUntilAsync(() => _offsets.Stored.Count == 1);
        _offsets.Stored.Should().Equal((Partition, 500L));

        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);
        _committer.Calls.Should().HaveCount(2);
        _offsets.Stored.Should().HaveCount(1);
    }

    [Fact]
    public async Task ChannelCompleted_FlushesRemainder()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(10);
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _committer.Calls.Should().ContainSingle().Which.Should().HaveCount(10);
        _offsets.Stored.Should().Equal((Partition, 10L));
    }

    [Fact]
    public async Task NoEmptyFlushes()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        for (var i = 0; i < 10; i++)
        {
            _clock.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(10);
        }

        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);
        _committer.Calls.Should().BeEmpty();
        _offsets.Stored.Should().BeEmpty();
    }

    [Fact]
    public async Task InvalidOnlyBatch_PublishesDlqThenStoresOffset()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteAsync(Encoding.UTF8.GetBytes("not json"));
        await WriteAsync(null);
        await WriteAsync(Encoding.UTF8.GetBytes("{\"lat\":\"abc\"}"));
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _committer.Calls.Should().BeEmpty();
        _dlq.Published.Select(p => p.Reason).Should().Equal(DlqReasons.InvalidJson, DlqReasons.EmptyPayload, DlqReasons.InvalidJson);
        _offsets.Stored.Should().Equal((Partition, 3L));
        _log.Should().Equal("dlq:0", "dlq:1", "dlq:2", "offset:3");
    }

    [Fact]
    public async Task MixedBatch_CommitsValidEvents_DlqBeforeOffset()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(2);
        await WriteAsync(Encoding.UTF8.GetBytes("garbage"));
        await WriteValidAsync(1);
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _committer.Calls.Should().ContainSingle().Which.Select(e => e.Seq).Should().Equal(0L, 1L, 3L);
        _log.Should().Equal("dlq:2", "commit:3", "offset:4");
    }

    [Fact]
    public async Task KnownByHint_SkipsCommitButStoresOffset()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token, new AllKnownHint());

        await WriteValidAsync(5);
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _committer.Calls.Should().BeEmpty("every event was already committed according to the hint");
        _offsets.Stored.Should().Equal((Partition, 5L));
    }

    [Fact]
    public async Task KnownByHint_SkippedAndCountedAsDuplicate()
    {
        _committer.DuplicatesPerCall = 1; // the DB also reports one duplicate of its own
        using var cts = new CancellationTokenSource();
        var hint = new RecordingHint(_log) { KnownIndexes = new HashSet<int> { 1, 3 } };
        var run = StartProcessor(cts.Token, hint);

        await WriteValidAsync(5);
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _committer.Calls.Should().ContainSingle().Which.Select(e => e.Seq).Should().Equal(0L, 2L, 4L);
        _observer.Batches.Should().ContainSingle().Which.Should().Be((Partition, 2, 3, 0, 0, 0),
            "2 known by the hint + 1 reported by the DB = 3 duplicates; inserted = 3 committed - 1 DB duplicate");
        _offsets.Stored.Should().Equal((Partition, 5L));
    }

    [Fact]
    public async Task KnownByHint_AllKnown_StillReportsDuplicates()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token, new AllKnownHint());

        await WriteValidAsync(4);
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _committer.Calls.Should().BeEmpty();
        _observer.Batches.Should().ContainSingle().Which.Should().Be((Partition, 0, 4, 0, 0, 0));
    }

    [Fact]
    public async Task HintFailure_StillCommitsAll()
    {
        using var cts = new CancellationTokenSource();
        var hint = new RecordingHint(_log) { Throw = true };
        var run = StartProcessor(cts.Token, hint);

        await WriteValidAsync(5);
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _committer.Calls.Should().ContainSingle().Which.Should().HaveCount(5);
        _offsets.Stored.Should().Equal(new[] { (Partition, 5L) }, "a failing hint never blocks the offset");
        _observer.Batches.Should().ContainSingle().Which.Should().Be((Partition, 5, 0, 0, 0, 0));
    }

    [Fact]
    public async Task MarkCalledOnlyAfterCommit()
    {
        using var cts = new CancellationTokenSource();
        var hint = new RecordingHint(_log);
        var run = StartProcessor(cts.Token, hint);

        await WriteValidAsync(3);
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _log.Should().Equal("commit:3", "mark:3", "offset:3");
        hint.Marked.Should().Equal(_committer.Calls[0].Select(e => e.EventId));
    }

    [Fact]
    public async Task MarkNeverCalled_WhenCommitFails()
    {
        _committer.FailuresBeforeSuccess = int.MaxValue;
        using var cts = new CancellationTokenSource();
        var hint = new RecordingHint(_log);
        var run = StartProcessor(cts.Token, hint);

        await WriteValidAsync(3);
        _channel.Writer.Complete();
        await WaitUntilAsync(() => _committer.Calls.Count >= 2, () => _clock.Advance(TimeSpan.FromMilliseconds(250)));

        cts.Cancel();
        await run.WaitAsync(Timeout);
        hint.Marked.Should().BeEmpty("D1: a key is set only for committed events");
        _offsets.Stored.Should().BeEmpty();
        _observer.Batches.Should().BeEmpty();
    }

    [Fact]
    public async Task DlqPublish_IsReportedWithReason()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteAsync(Encoding.UTF8.GetBytes("not json"));
        await WriteAsync(Array.Empty<byte>());
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _observer.DlqReasons.Should().Equal(DlqReasons.InvalidJson, DlqReasons.EmptyPayload);
    }

    [Fact]
    public async Task Cancelled_WhileCommitFails_ReturnsWithoutStoringOffset()
    {
        _committer.FailuresBeforeSuccess = int.MaxValue;
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(3);
        _channel.Writer.Complete();
        await WaitUntilAsync(() => _committer.Calls.Count >= 1);

        cts.Cancel();
        await run.WaitAsync(Timeout);
        _offsets.Stored.Should().BeEmpty();
    }

    [Fact]
    public async Task ThrowingObserver_ProcessorKeepsGoing_OffsetsStored_DlqPublishedOnce()
    {
        using var cts = new CancellationTokenSource();
        var observer = new ThrowingObserver();
        var run = StartProcessor(cts.Token, observer: observer);

        await WriteValidAsync(2);
        await WriteAsync(Encoding.UTF8.GetBytes("not json"));
        await WriteValidAsync(1);
        await WaitUntilAsync(() => _channel.Reader.Count == 0); // all four taken, so they share one batch
        await Task.Delay(50);
        await WaitUntilAsync(() => _offsets.Stored.Count == 1, () => _clock.Advance(TimeSpan.FromMilliseconds(100)));

        await WriteValidAsync(3); // a second batch proves the processor survived the throwing observer
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        run.IsCompletedSuccessfully.Should().BeTrue();
        _dlq.Published.Should().ContainSingle("an observer failure must not republish the DLQ message");
        _committer.Calls.Should().HaveCount(2);
        _offsets.Stored.Should().Equal((Partition, 4L), (Partition, 7L));
        observer.BatchCalls.Should().Be(2);
        observer.DlqCalls.Should().Be(1);
    }

    [Fact]
    public async Task ObserverDlq_CalledOnlyAfterSuccessfulPublish()
    {
        _dlq.FailuresBeforeSuccess = 2;
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteAsync(Encoding.UTF8.GetBytes("not json"));
        _channel.Writer.Complete();
        await WaitUntilAsync(() => run.IsCompleted, () => _clock.Advance(TimeSpan.FromMilliseconds(250)));

        _dlq.Attempts.Should().Be(3);
        _observer.DlqReasons.Should().Equal(DlqReasons.InvalidJson);
        _offsets.Stored.Should().Equal((Partition, 1L));
    }

    [Fact]
    public async Task Flush_EmitsBatchSpanWithDistinctProducerLinks_AndRecordsTimings()
    {
        var spans = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == Tracing.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                lock (spans)
                {
                    spans.Add(a);
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var producers = Enumerable.Range(0, 40)
            .Select(_ => new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded))
            .ToArray();
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);
        // 45 events: 40 distinct producer contexts, the first 5 repeated, so links are de-duplicated and capped at 32.
        for (var i = 0; i < 45; i++)
        {
            await WriteValidAsync(1, producers[i % 40]);
        }

        _clock.Advance(TimeSpan.FromSeconds(2)); // every event is now 2 s old
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        Activity batch;
        lock (spans)
        {
            batch = spans.Should().ContainSingle(a => a.OperationName == "batch.process" && Equals(a.GetTagItem("partition"), Partition)).Subject;
        }

        batch.GetTagItem("batch.size").Should().Be(45);
        batch.Links.Should().HaveCount(Tracing.MaxLinks);
        batch.Links.Select(l => l.Context.SpanId).Should().OnlyHaveUniqueItems();
        batch.Links.Select(l => l.Context).Should().BeSubsetOf(producers);
        batch.Parent.Should().BeNull("a batch has many producers, so they are links, not a parent (D14)");
        _observer.BatchDurations.Should().ContainSingle();
        _observer.Latencies.Should().HaveCount(45).And.OnlyContain(l => l >= 2.0);
    }

    [Fact]
    public async Task KeyDifferentFromVehicleId_GoesToDlq_OffsetStillAdvances()
    {
        using var cts = new CancellationTokenSource();
        var run = StartProcessor(cts.Token);

        await WriteValidAsync(1);
        await WriteValidAsync(1, key: "OTHER-VEHICLE");
        await WriteValidAsync(1, key: null); // no key: nothing to compare, accepted
        _channel.Writer.Complete();
        await run.WaitAsync(Timeout);

        _dlq.Published.Should().ContainSingle().Which.Reason.Should().Be(DlqReasons.KeyMismatch);
        _dlq.Published[0].Msg.Offset.Should().Be(1);
        _committer.Calls.Should().ContainSingle().Which.Select(e => e.Seq).Should().Equal(0L, 2L);
        _offsets.Stored.Should().Equal((Partition, 3L));
        _log.Should().Equal("dlq:1", "commit:2", "offset:3");
    }

    private Task StartProcessor(CancellationToken ct, IDedupeHint? hint = null, IProcessingObserver? observer = null)
    {
        var processor = new PartitionProcessor(Partition, _channel.Reader, _committer, hint ?? new NoopDedupeHint(), _dlq, _offsets,
            observer ?? _observer, _clock, _options, NullLogger.Instance);
        return Task.Run(() => processor.RunAsync(ct));
    }

    private const string VehicleKey = "use-vehicle-id";

    private async Task WriteValidAsync(int count, ActivityContext parent = default, string? key = VehicleKey)
    {
        for (var i = 0; i < count; i++)
        {
            var seq = _nextOffset;
            var dto = new TelemetryEventDto
            {
                EventId = Guid.NewGuid(),
                VehicleId = "VH-" + (seq % 7),
                Seq = seq,
                TimestampUtc = _clock.GetUtcNow(),
                Lat = 40.0,
                Lon = -3.0,
                SpeedKmh = 50,
                Ignition = true,
            };
            await WriteAsync(JsonSerializer.SerializeToUtf8Bytes(dto, TelemetryJsonContext.Default.TelemetryEventDto), parent,
                key == VehicleKey ? dto.VehicleId : key);
        }
    }

    private ValueTask WriteAsync(byte[]? value, ActivityContext parent = default, string? key = "k")
    {
        var offset = _nextOffset++;
        return _channel.Writer.WriteAsync(new PartitionMessage(Partition, offset, key, value, parent, _clock.GetUtcNow()));
    }

    private static async Task WaitUntilAsync(Func<bool> condition, Action? onEachPoll = null)
    {
        var sw = Stopwatch.StartNew();
        while (!condition())
        {
            if (sw.Elapsed > Timeout)
            {
                throw new TimeoutException("Condition not met in time.");
            }

            onEachPoll?.Invoke();
            await Task.Delay(10);
        }
    }

    private sealed class FakeCommitter : IBatchCommitter
    {
        private readonly List<string> _log;
        private readonly List<TelemetryEvent[]> _calls = new();

        public FakeCommitter(List<string> log) => _log = log;

        public Task? Gate { get; set; }

        public int FailuresBeforeSuccess { get; set; }

        public int DuplicatesPerCall { get; set; }

        public IReadOnlyList<TelemetryEvent[]> Calls
        {
            get
            {
                lock (_calls)
                {
                    return _calls.ToArray();
                }
            }
        }

        public async Task<PersistResult> CommitAsync(ReadOnlyMemory<TelemetryEvent> events, int partition, CancellationToken ct)
        {
            partition.Should().Be(Partition);
            lock (_calls)
            {
                _calls.Add(events.ToArray()); // copy: the buffer is pooled
            }

            if (Gate is not null)
            {
                await Gate.WaitAsync(ct);
            }

            lock (_calls)
            {
                if (FailuresBeforeSuccess > 0)
                {
                    FailuresBeforeSuccess--;
                    throw new InvalidOperationException("simulated DB failure");
                }
            }

            lock (_log)
            {
                _log.Add("commit:" + events.Length);
            }

            return new PersistResult(events.Length - DuplicatesPerCall, DuplicatesPerCall, 0, 0, Array.Empty<SpeedingAlertMessage>());
        }
    }

    private sealed class FakeDlq : IDlqPublisher
    {
        private readonly List<string> _log;

        public FakeDlq(List<string> log) => _log = log;

        public List<(PartitionMessage Msg, string Reason)> Published { get; } = new();

        public int FailuresBeforeSuccess { get; set; }

        public int Attempts { get; private set; }

        public Task PublishAsync(PartitionMessage msg, string reason, CancellationToken ct)
        {
            lock (_log)
            {
                Attempts++;
                if (FailuresBeforeSuccess > 0)
                {
                    FailuresBeforeSuccess--;
                    throw new InvalidOperationException("simulated broker failure");
                }

                Published.Add((msg, reason));
                _log.Add("dlq:" + msg.Offset);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class FakeOffsets : IOffsetSink
    {
        private readonly List<string> _log;
        private readonly List<(int, long)> _stored = new();

        public FakeOffsets(List<string> log) => _log = log;

        public IReadOnlyList<(int Partition, long NextOffset)> Stored
        {
            get
            {
                lock (_stored)
                {
                    return _stored.ToArray();
                }
            }
        }

        public void Store(int partition, long nextOffset)
        {
            lock (_stored)
            {
                _stored.Add((partition, nextOffset));
            }

            lock (_log)
            {
                _log.Add("offset:" + nextOffset);
            }
        }
    }

    private sealed class AllKnownHint : IDedupeHint
    {
        public ValueTask<bool[]> AreKnownAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct)
        {
            var known = new bool[ids.Length];
            Array.Fill(known, true);
            return ValueTask.FromResult(known);
        }

        public ValueTask MarkAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct) => ValueTask.CompletedTask;
    }

    /// <summary>Hint that knows the events at the given batch positions, can fail, and records what was marked.</summary>
    private sealed class RecordingHint : IDedupeHint
    {
        private readonly List<string> _log;
        private readonly List<Guid> _marked = new();

        public RecordingHint(List<string> log) => _log = log;

        public HashSet<int> KnownIndexes { get; init; } = new();

        public bool Throw { get; init; }

        public IReadOnlyList<Guid> Marked
        {
            get
            {
                lock (_marked)
                {
                    return _marked.ToArray();
                }
            }
        }

        public ValueTask<bool[]> AreKnownAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct)
        {
            if (Throw)
            {
                throw new InvalidOperationException("simulated Redis failure");
            }

            var known = new bool[ids.Length];
            for (var i = 0; i < known.Length; i++)
            {
                known[i] = KnownIndexes.Contains(i);
            }

            return ValueTask.FromResult(known);
        }

        public ValueTask MarkAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct)
        {
            if (Throw)
            {
                throw new InvalidOperationException("simulated Redis failure");
            }

            lock (_marked)
            {
                _marked.AddRange(ids.ToArray());
            }

            lock (_log)
            {
                _log.Add("mark:" + ids.Length);
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingObserver : IProcessingObserver
    {
        private int _batchCalls;
        private int _dlqCalls;

        public int BatchCalls => Volatile.Read(ref _batchCalls);

        public int DlqCalls => Volatile.Read(ref _dlqCalls);

        public void OnBatchCommitted(int partition, int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts)
        {
            Interlocked.Increment(ref _batchCalls);
            throw new InvalidOperationException("observer failure");
        }

        public void OnDlq(string reason)
        {
            Interlocked.Increment(ref _dlqCalls);
            throw new InvalidOperationException("observer failure");
        }

        public void OnRedisFallback() => throw new InvalidOperationException("observer failure");
    }

    private sealed class RecordingObserver : IProcessingObserver
    {
        private readonly List<(int, int, int, int, int, int)> _batches = new();
        private readonly List<string> _dlq = new();

        public IReadOnlyList<(int Partition, int Inserted, int Duplicates, int OutOfOrder, int GpsJumps, int Alerts)> Batches
        {
            get
            {
                lock (_batches)
                {
                    return _batches.ToArray();
                }
            }
        }

        public IReadOnlyList<string> DlqReasons
        {
            get
            {
                lock (_dlq)
                {
                    return _dlq.ToArray();
                }
            }
        }

        public void OnBatchCommitted(int partition, int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts)
        {
            lock (_batches)
            {
                _batches.Add((partition, inserted, duplicates, outOfOrder, gpsJumps, alerts));
            }
        }

        public void OnDlq(string reason)
        {
            lock (_dlq)
            {
                _dlq.Add(reason);
            }
        }

        public void OnRedisFallback()
        {
        }

        public IReadOnlyList<double> BatchDurations
        {
            get
            {
                lock (_timings)
                {
                    return _timings.Where(t => t.Batch).Select(t => t.Seconds).ToArray();
                }
            }
        }

        public IReadOnlyList<double> Latencies
        {
            get
            {
                lock (_timings)
                {
                    return _timings.Where(t => !t.Batch).Select(t => t.Seconds).ToArray();
                }
            }
        }

        public void RecordBatchDuration(double seconds)
        {
            lock (_timings)
            {
                _timings.Add((true, seconds));
            }
        }

        public void RecordLatency(double seconds)
        {
            lock (_timings)
            {
                _timings.Add((false, seconds));
            }
        }

        private readonly List<(bool Batch, double Seconds)> _timings = new();
    }
}
