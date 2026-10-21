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

    private Task StartProcessor(CancellationToken ct, IDedupeHint? hint = null)
    {
        var processor = new PartitionProcessor(Partition, _channel.Reader, _committer, hint ?? new NoopDedupeHint(), _dlq, _offsets,
            _clock, _options, NullLogger.Instance);
        return Task.Run(() => processor.RunAsync(ct));
    }

    private async Task WriteValidAsync(int count)
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
            await WriteAsync(JsonSerializer.SerializeToUtf8Bytes(dto, TelemetryJsonContext.Default.TelemetryEventDto));
        }
    }

    private ValueTask WriteAsync(byte[]? value)
    {
        var offset = _nextOffset++;
        return _channel.Writer.WriteAsync(new PartitionMessage(Partition, offset, "k", value, default(ActivityContext), _clock.GetUtcNow()));
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

            return new PersistResult(events.Length, 0, 0, 0, Array.Empty<SpeedingAlertMessage>());
        }
    }

    private sealed class FakeDlq : IDlqPublisher
    {
        private readonly List<string> _log;

        public FakeDlq(List<string> log) => _log = log;

        public List<(PartitionMessage Msg, string Reason)> Published { get; } = new();

        public Task PublishAsync(PartitionMessage msg, string reason, CancellationToken ct)
        {
            lock (_log)
            {
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
}
