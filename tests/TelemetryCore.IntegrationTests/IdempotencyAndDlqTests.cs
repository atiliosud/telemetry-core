using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;
using TelemetryCore.IntegrationTests.Fixtures;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.IntegrationTests;

[Collection(TelemetryStackCollection.Name)]
public sealed class IdempotencyAndDlqTests
{
    private static readonly TimeSpan ProcessingTimeout = TimeSpan.FromSeconds(60);
    private readonly TelemetryStackFixture _stack;

    public IdempotencyAndDlqTests(TelemetryStackFixture stack) => _stack = stack;

    [Fact]
    public async Task SameEventPublishedTwice_OneRowOneStateChange()
    {
        var ctx = await NewContextAsync();
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var e1 = Event(vehicle, 1, DateTimeOffset.UtcNow.AddMinutes(-5), 40.0, 50);

        await using var worker = ctx.StartWorker(_stack);
        await worker.StartAsync();
        await _stack.ProduceAsync(new[] { e1 }, ctx.Topic);
        await WaitForStateAsync(vehicle, 1);
        // D1: the key appears only after the commit.
        await TelemetryStackFixture.WaitUntilAsync(() => _stack.RedisKeyExistsAsync(RedisDedupeHint.Key(e1.EventId!.Value)), ProcessingTimeout);
        var before = await _stack.GetStateAsync(vehicle);

        await _stack.ProduceAsync(new[] { e1 }, ctx.Topic);
        await TelemetryStackFixture.WaitUntilAsync(() => Task.FromResult(ctx.Observer.Duplicates >= 1), ProcessingTimeout);
        await worker.StopAsync();

        ctx.Observer.Duplicates.Should().Be(1);
        ctx.Observer.Inserted.Should().Be(1);
        (await _stack.CountRowsAsync("processed_events", vehicle)).Should().Be(1);
        (await _stack.GetStateAsync(vehicle)).Should().BeEquivalentTo(before, "the duplicate must not touch the state");
    }

    [Fact]
    public async Task DuplicateAfterRedisKeyLost_RejectedByDatabase()
    {
        var ctx = await NewContextAsync();
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var e1 = Event(vehicle, 1, DateTimeOffset.UtcNow.AddMinutes(-5), 40.0, 50);

        await using var worker = ctx.StartWorker(_stack);
        await worker.StartAsync();
        await _stack.ProduceAsync(new[] { e1 }, ctx.Topic);
        await WaitForStateAsync(vehicle, 1);
        await TelemetryStackFixture.WaitUntilAsync(() => _stack.RedisKeyExistsAsync(RedisDedupeHint.Key(e1.EventId!.Value)), ProcessingTimeout);
        var before = await _stack.GetStateAsync(vehicle);

        await _stack.FlushRedisAsync();
        await _stack.ProduceAsync(new[] { e1 }, ctx.Topic);
        await TelemetryStackFixture.WaitUntilAsync(() => Task.FromResult(ctx.Observer.Duplicates >= 1), ProcessingTimeout);
        await worker.StopAsync();

        ctx.Observer.Duplicates.Should().Be(1, "the DB key catches what the hint no longer knows");
        (await _stack.CountRowsAsync("processed_events", vehicle)).Should().Be(1);
        (await _stack.GetStateAsync(vehicle)).Should().BeEquivalentTo(before);
    }

    [Fact]
    public async Task OutOfOrder_StoredButStateUnchanged()
    {
        var ctx = await NewContextAsync();
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
        var events = new[] { 1, 2, 3, 5, 4 }.Select(seq => Event(vehicle, seq, t0.AddSeconds(seq), 40.0 + seq * 0.0001, 50)).ToArray();

        await using var worker = ctx.StartWorker(_stack);
        await worker.StartAsync();
        await _stack.ProduceAsync(events, ctx.Topic);
        await TelemetryStackFixture.WaitUntilAsync(async () => await _stack.CountRowsAsync("processed_events", vehicle) == 5, ProcessingTimeout);
        await WaitForStateAsync(vehicle, 5);
        await worker.StopAsync();

        (await OutOfOrderSeqsAsync(vehicle)).Should().Equal(4L);
        var state = await _stack.GetStateAsync(vehicle);
        state!.LastSeq.Should().Be(5);
        state.Lat.Should().BeApproximately(40.0005, 1e-9, "the late seq 4 must not move the vehicle back");
        ctx.Observer.OutOfOrder.Should().Be(1);
    }

    [Fact]
    public async Task InvalidEvents_GoToDlqWithReason()
    {
        var ctx = await NewContextAsync();
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
        var badLat = Event(vehicle, 1, t0, 95.0, 50);
        var badSpeed = Event(vehicle, 2, t0.AddSeconds(1), 40.0, 400);

        await using var worker = ctx.StartWorker(_stack);
        await worker.StartAsync();
        await _stack.ProduceAsync(new[] { badLat, badSpeed }, ctx.Topic);
        var dlq = await WaitForDlqAsync(ctx.DlqTopic, 2);
        await worker.StopAsync();

        dlq.Select(Reason).Should().Equal(DlqReasons.LatOutOfRange, DlqReasons.SpeedOutOfRange);
        dlq.Should().AllSatisfy(m => m.Message.Key.Should().Be(vehicle));
        dlq[0].Message.Value.Should().Equal(Serialize(badLat), "the DLQ keeps the original bytes");
        var sourcePartitions = dlq.Select(m => int.Parse(Header(m, TelemetryHeaders.SourcePartition))).Distinct().ToList();
        sourcePartitions.Should().ContainSingle();
        dlq.Select(m => long.Parse(Header(m, TelemetryHeaders.SourceOffset))).Should().Equal(0L, 1L);
        (await _stack.CountRowsAsync("processed_events", vehicle)).Should().Be(0);
        ctx.Observer.DlqReasons.Should().BeEquivalentTo(new[] { DlqReasons.LatOutOfRange, DlqReasons.SpeedOutOfRange });
    }

    [Fact]
    public async Task PoisonPayloads_GoToDlq_AndPartitionKeepsFlowing()
    {
        var ctx = await NewContextAsync();
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var nullKeyVehicle = TelemetryStackFixture.UniqueName("nokey"); // not a prefix match of vehicle
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
        var latAsText = Encoding.UTF8.GetString(Serialize(Event(vehicle, 1, t0, 40.0, 50)))
            .Replace("\"lat\":40", "\"lat\":\"abc\"", StringComparison.Ordinal);
        latAsText.Should().Contain("\"lat\":\"abc\"");
        var tooLarge = Encoding.UTF8.GetBytes("{\"padding\":\"" + new string('x', 5 * 1024) + "\"}");
        var valid = Event(vehicle, 2, t0.AddSeconds(1), 40.0001, 50);

        await using var worker = ctx.StartWorker(_stack);
        await worker.StartAsync();
        // Same key -> same partition: the valid event is queued behind every poison message.
        await _stack.ProduceRawAsync(vehicle, "\u0001\u0002 not json at all"u8.ToArray(), ctx.Topic);
        await _stack.ProduceRawAsync(vehicle, Encoding.UTF8.GetBytes(latAsText), ctx.Topic);
        await _stack.ProduceRawAsync(null, Serialize(Event(nullKeyVehicle, 1, t0, 41.0, 50)), ctx.Topic);
        await _stack.ProduceRawAsync(vehicle, Array.Empty<byte>(), ctx.Topic);
        await _stack.ProduceRawAsync(vehicle, tooLarge, ctx.Topic);
        await _stack.ProduceAsync(new[] { valid }, ctx.Topic);

        await WaitForStateAsync(vehicle, 2);
        await WaitForStateAsync(nullKeyVehicle, 1);
        var dlq = await WaitForDlqAsync(ctx.DlqTopic, 4);
        await worker.StopAsync();

        dlq.Select(Reason).Should().Equal(DlqReasons.InvalidJson, DlqReasons.InvalidJson, DlqReasons.EmptyPayload, DlqReasons.PayloadTooLarge);
        dlq.Should().AllSatisfy(m => m.Message.Key.Should().Be(vehicle));
        dlq[2].Message.Value.Should().BeEmpty();
        dlq[3].Message.Value.Should().Equal(tooLarge);
        (await _stack.CountRowsAsync("processed_events", vehicle)).Should().Be(1, "the valid event behind the poison messages is persisted");
        (await _stack.CountRowsAsync("processed_events", nullKeyVehicle)).Should().Be(1, "a null key does not make a valid payload invalid");
    }

    [Fact]
    public async Task Restart_HydratesStateFromDatabase()
    {
        var ctx = await NewContextAsync();
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-10);
        // seq 1..8 at 50 km/h, then a speeding streak of 2 (seq 9, 10) at 125 km/h.
        var first = Enumerable.Range(1, 10)
            .Select(seq => Event(vehicle, seq, t0.AddSeconds(seq), 40.0 + seq * 0.0001, seq >= 9 ? 125 : 50))
            .ToArray();

        await using (var worker1 = ctx.StartWorker(_stack))
        {
            await worker1.StartAsync();
            await _stack.ProduceAsync(first, ctx.Topic);
            await WaitForStateAsync(vehicle, 10);
            await worker1.StopAsync();
        }

        var persisted = await _stack.GetStateAsync(vehicle);
        persisted!.OverspeedStreak.Should().Be(2);
        persisted.Speeding.Should().BeFalse();

        // A fresh host (empty in-memory cache), same consumer group.
        await using var worker2 = ctx.StartWorker(_stack);
        await worker2.StartAsync();
        var next = Event(vehicle, 11, t0.AddSeconds(11), 40.0011, 125);
        await _stack.ProduceAsync(new[] { next }, ctx.Topic);
        await WaitForStateAsync(vehicle, 11);
        await worker2.StopAsync();

        var state = await _stack.GetStateAsync(vehicle);
        state!.Speeding.Should().BeTrue("the streak continues from the persisted value (2 + 1 = 3)");
        state.OverspeedStreak.Should().Be(3);
        state.DistanceKmToday.Should().BeApproximately(
            persisted.DistanceKmToday + Geo.HaversineKm(persisted.Lat, persisted.Lon, 40.0011, -3.0), 1e-9);
        (await OutOfOrderSeqsAsync(vehicle)).Should().BeEmpty("seq 11 follows the persisted last_seq 10");
        (await _stack.CountRowsAsync("alerts", vehicle)).Should().Be(1);
        (await _stack.CountRowsAsync("processed_events", vehicle)).Should().Be(11);
    }

    [Fact]
    public async Task RedisHint_MarkThenKnown_WithOneDayTtl()
    {
        using var hint = RedisDedupeHint.Connect(_stack.RedisConnectionString, ResiliencePipelines.CreateRedis(), () => { },
            NullLogger.Instance);
        var marked = Guid.NewGuid();
        var other = Guid.NewGuid();

        await hint.MarkAsync(new[] { marked }, CancellationToken.None);
        var known = await hint.AreKnownAsync(new[] { marked, other }, CancellationToken.None);

        known.Should().Equal(true, false);
        var ttl = await _stack.RedisTimeToLiveAsync("dedupe:" + marked.ToString("N"));
        ttl.Should().NotBeNull();
        ttl!.Value.Should().BeCloseTo(TimeSpan.FromSeconds(86400), TimeSpan.FromMinutes(1));
    }

    private async Task<TestContext> NewContextAsync()
    {
        var ctx = new TestContext(TelemetryStackFixture.UniqueName("raw"), TelemetryStackFixture.UniqueName("dlq"),
            TelemetryStackFixture.UniqueName("group"));
        await _stack.CreateTopicAsync(ctx.Topic, 12);
        await _stack.CreateTopicAsync(ctx.DlqTopic, 3);
        return ctx;
    }

    private async Task WaitForStateAsync(string vehicle, long lastSeq) =>
        await TelemetryStackFixture.WaitUntilAsync(async () => (await _stack.GetStateAsync(vehicle))?.LastSeq == lastSeq, ProcessingTimeout);

    private async Task<List<ConsumeResult<string?, byte[]?>>> WaitForDlqAsync(string topic, int expected)
    {
        await TelemetryStackFixture.WaitUntilAsync(async () => (await _stack.GetHighWatermarksAsync(topic)).Values.Sum() >= expected,
            ProcessingTimeout);
        var all = await _stack.ConsumeAllAsync(topic);
        all.Should().HaveCount(expected);
        return all.OrderBy(m => long.Parse(Header(m, TelemetryHeaders.SourceOffset))).ToList();
    }

    private async Task<List<long>> OutOfOrderSeqsAsync(string vehicle)
    {
        await using var conn = new NpgsqlConnection(_stack.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT seq FROM processed_events WHERE vehicle_id = @id AND out_of_order ORDER BY seq", conn);
        cmd.Parameters.AddWithValue("id", vehicle);
        var result = new List<long>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            result.Add(reader.GetInt64(0));
        }

        return result;
    }

    private static string Reason(ConsumeResult<string?, byte[]?> m) => Header(m, TelemetryHeaders.Reason);

    private static string Header(ConsumeResult<string?, byte[]?> m, string name) =>
        Encoding.UTF8.GetString(m.Message.Headers.GetLastBytes(name));

    private static byte[] Serialize(TelemetryEventDto dto) =>
        JsonSerializer.SerializeToUtf8Bytes(dto, TelemetryJsonContext.Default.TelemetryEventDto);

    private static TelemetryEventDto Event(string vehicle, long seq, DateTimeOffset ts, double lat, double speed) => new()
    {
        EventId = Guid.NewGuid(),
        VehicleId = vehicle,
        Seq = seq,
        TimestampUtc = ts,
        Lat = lat,
        Lon = -3.0,
        SpeedKmh = speed,
        Ignition = true,
    };

    private sealed record TestContext(string Topic, string DlqTopic, string Group)
    {
        public RecordingObserver Observer { get; } = new();

        public WorkerHarness StartWorker(TelemetryStackFixture stack) =>
            new(stack, Topic, Group,
                new Dictionary<string, string?>
                {
                    ["Worker:RedisConnectionString"] = stack.RedisConnectionString,
                    ["Worker:DlqTopic"] = DlqTopic,
                },
                services => services.AddSingleton<IProcessingObserver>(Observer));
    }

    private sealed class RecordingObserver : IProcessingObserver
    {
        private readonly List<string> _dlq = new();
        private int _inserted;
        private int _duplicates;
        private int _outOfOrder;

        public int Inserted => Volatile.Read(ref _inserted);

        public int Duplicates => Volatile.Read(ref _duplicates);

        public int OutOfOrder => Volatile.Read(ref _outOfOrder);

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
            Interlocked.Add(ref _inserted, inserted);
            Interlocked.Add(ref _duplicates, duplicates);
            Interlocked.Add(ref _outOfOrder, outOfOrder);
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
    }
}
