using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;
using TelemetryCore.IntegrationTests.Fixtures;
using TelemetryCore.Worker.Ingestion;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Persistence;
using TelemetryCore.Worker.Resilience;
using Testcontainers.Redis;
using Xunit.Abstractions;

namespace TelemetryCore.IntegrationTests;

/// <summary>
/// SPEC §7 / §12 / §16 failure scenarios against real containers: crash after the DB commit, Postgres outage (breaker),
/// Redis outage (DB key decides, D1) and a cooperative rebalance between two workers (D10).
/// </summary>
[Collection(TelemetryStackCollection.Name)]
public sealed class ResilienceTests
{
    private static readonly TimeSpan ProcessingTimeout = TimeSpan.FromSeconds(120);
    private readonly TelemetryStackFixture _stack;
    private readonly ITestOutputHelper _output;

    public ResilienceTests(TelemetryStackFixture stack, ITestOutputHelper output)
    {
        _stack = stack;
        _output = output;
    }

    [Fact]
    public async Task CrashAfterDbCommit_RestartedWorker_FinalCountsEqualUnique()
    {
        var sw = Stopwatch.StartNew();
        var (topic, group, prefix) = await NewRunAsync();
        var load = Load.Create(prefix, vehicles: 48, eventsPerVehicle: 100, duplicates: 200); // 5,000 messages
        await _stack.ProduceManyAsync(load.Messages, topic);

        // A short session timeout: the crashed member never leaves the group, so its partitions return after this.
        var settings = new Dictionary<string, string?> { ["Worker:SessionTimeoutMs"] = "6000" };
        var injector = new CrashOnNthCommit(3);
        long rowsAtCrash;
        await using (var first = new WorkerHarness(_stack, topic, group, settings, s => s.AddSingleton<IFaultInjector>(injector)))
        {
            await first.StartAsync();
            await TelemetryStackFixture.WaitUntilAsync(() => Task.FromResult(injector.Crashed), ProcessingTimeout);
            await first.CrashAsync();
            rowsAtCrash = await _stack.CountRowsAsync("processed_events", prefix);
        }

        _output.WriteLine($"Crashed on partition {injector.CrashedPartition}; rows at crash {rowsAtCrash} of {load.Unique}");
        _stack.CountSeqInversionsInTopic(topic, 12).Should().Be(0, "the producer must keep per-vehicle order in the log");
        rowsAtCrash.Should().BeLessThan(load.Unique, "the worker crashed mid-run");

        var observer = new CountingObserver();
        await using (var second = new WorkerHarness(_stack, topic, group, settings, s => s.AddSingleton<IProcessingObserver>(observer)))
        {
            await second.StartAsync();
            await WaitForRowsAsync(prefix, load.Unique);
            await WaitForLastSeqAsync(prefix, load);
            await second.StopAsync();
        }

        (await _stack.CountRowsAsync("processed_events", prefix)).Should().Be(load.Unique);
        observer.Duplicates.Should().BeGreaterThan(0, "the batch committed before the crash had no stored offset and was replayed");
        await AssertStateAsync(prefix, load);
        _output.WriteLine($"Duration {sw.Elapsed.TotalSeconds:F1} s");
    }

    [Fact]
    public async Task PostgresPaused20s_BreakerOpens_NoDataLost()
    {
        var sw = Stopwatch.StartNew();
        var (topic, group, prefix) = await NewRunAsync();
        var load = Load.Create(prefix, vehicles: 48, eventsPerVehicle: 100, duplicates: 200);
        var half = load.Messages.Count / 2;
        var observer = new CountingObserver();

        // Short command / cancellation timeouts: against a paused server the attempts fail within the outage, so the breaker can open.
        var cs = new NpgsqlConnectionStringBuilder(_stack.WorkerConnectionString) { CommandTimeout = 2, CancellationTimeout = 500, Timeout = 2 };
        var dbSettings = new Dictionary<string, string?> { ["Worker:PostgresConnectionString"] = cs.ConnectionString };
        await using var worker = new WorkerHarness(_stack, topic, group, dbSettings, s => s.AddSingleton<IProcessingObserver>(observer));
        await worker.StartAsync();
        var tracker = worker.Services.GetRequiredService<CircuitStateTracker>();
        await _stack.ProduceManyAsync(load.Messages.Take(half), topic);
        var firstUnique = load.Messages.Take(half).Select(m => m.EventId).Distinct().LongCount();
        await WaitForRowsAsync(prefix, firstUnique);

        var reachedOpen = false;
        long insertedAtPause;
        long insertedBeforeUnpause;
        await _stack.PausePostgresAsync();
        try
        {
            insertedAtPause = observer.Inserted;
            await _stack.ProduceManyAsync(load.Messages.Skip(half), topic);
            var paused = Stopwatch.StartNew();
            while (paused.Elapsed < TimeSpan.FromSeconds(20))
            {
                reachedOpen |= tracker.State == CircuitStateTracker.Open;
                await Task.Delay(100);
            }

            insertedBeforeUnpause = observer.Inserted;
        }
        finally
        {
            await _stack.UnpausePostgresAsync();
        }

        var recovery = Stopwatch.StartNew();
        await TelemetryStackFixture.WaitUntilAsync(async () =>
        {
            reachedOpen |= tracker.State == CircuitStateTracker.Open; // hung attempts may only fail (and open it) after the pause
            return await _stack.CountRowsAsync("processed_events", prefix) == load.Unique;
        }, TimeSpan.FromSeconds(90));
        _output.WriteLine($"Recovered to {load.Unique} rows {recovery.Elapsed.TotalSeconds:F1} s after unpause");
        await WaitForLastSeqAsync(prefix, load);
        await worker.StopAsync();

        reachedOpen.Should().BeTrue("the DB circuit breaker must open during the 20 s outage episode");
        insertedBeforeUnpause.Should().Be(insertedAtPause, "nothing can be committed while Postgres is paused");
        (await _stack.CountRowsAsync("processed_events", prefix)).Should().Be(load.Unique);
        await AssertStateAsync(prefix, load);
        _output.WriteLine($"Duration {sw.Elapsed.TotalSeconds:F1} s");
    }

    [Fact]
    public async Task RedisStopped_ProcessingContinuesWithDbConstraint()
    {
        var sw = Stopwatch.StartNew();
        var (topic, group, prefix) = await NewRunAsync();
        var load = Load.Create(prefix, vehicles: 20, eventsPerVehicle: 50, duplicates: 100);
        var observer = new CountingObserver();

        // A dedicated Redis (same image as the shared one): the shared container stays up for the other tests.
        await using var redis = new RedisBuilder("redis:7.4-alpine").Build();
        await redis.StartAsync();
        var settings = new Dictionary<string, string?> { ["Worker:RedisConnectionString"] = redis.GetConnectionString() };
        await using var worker = new WorkerHarness(_stack, topic, group, settings, s => s.AddSingleton<IProcessingObserver>(observer));
        await worker.StartAsync();
        await redis.StopAsync();

        await _stack.ProduceManyAsync(load.Messages, topic);
        await WaitForRowsAsync(prefix, load.Unique);
        await TelemetryStackFixture.WaitUntilAsync(() => Task.FromResult(observer.Inserted + observer.Duplicates == load.Messages.Count),
            ProcessingTimeout);
        await worker.StopAsync();

        (await _stack.CountRowsAsync("processed_events", prefix)).Should().Be(load.Unique);
        observer.Duplicates.Should().Be(load.Duplicates, "with Redis down every duplicate is caught by the DB key");
        ((long)observer.Inserted).Should().Be(load.Unique);
        observer.RedisFallbacks.Should().BeGreaterThan(0);
        await AssertStateAsync(prefix, load);
        _output.WriteLine($"Duration {sw.Elapsed.TotalSeconds:F1} s; redis fallbacks {observer.RedisFallbacks}");
    }

    [Fact]
    public async Task Rebalance_TwoWorkers_NoLossNoDoubleCount()
    {
        var sw = Stopwatch.StartNew();
        var (topic, group, prefix) = await NewRunAsync();
        var load = Load.Create(prefix, vehicles: 200, eventsPerVehicle: 100, duplicates: 1000, speedingEvery: 10);
        await _stack.ProduceManyAsync(load.Messages, topic);

        // Small, slowed-down batches keep the run long enough for a second member to join and the first to leave mid-run.
        var settings = new Dictionary<string, string?> { ["Worker:BatchSize"] = "100", ["Worker:ChannelCapacity"] = "300", ["Logging:LogLevel:TelemetryCore.Worker.Ingestion"] = "Information" };
        var observer1 = new CountingObserver();
        var observer2 = new CountingObserver();
        await using var first = SlowWorker(topic, group, settings, observer1);
        await using var second = SlowWorker(topic, group, settings, observer2);

        await first.StartAsync();
        await WaitForRowsAsync(prefix, load.Unique / 10);
        await second.StartAsync();
        await TelemetryStackFixture.WaitUntilAsync(() => Task.FromResult(observer2.Inserted >= 500), ProcessingTimeout);
        await first.StopAsync();
        var rowsWhenFirstLeft = await _stack.CountRowsAsync("processed_events", prefix);
        _output.WriteLine($"First worker left at {rowsWhenFirstLeft} of {load.Unique} rows (first inserted {observer1.Inserted}, second {observer2.Inserted})");

        await WaitForRowsAsync(prefix, load.Unique);
        await WaitForLastSeqAsync(prefix, load);
        await TelemetryStackFixture.WaitUntilAsync(
            () => Task.FromResult(observer1.Inserted + observer2.Inserted + observer1.Duplicates + observer2.Duplicates >= load.Messages.Count),
            ProcessingTimeout);
        await Task.Delay(500); // a late replay would show up now
        await second.StopAsync();

        rowsWhenFirstLeft.Should().BeLessThan(load.Unique, "the first worker left mid-run");
        observer1.Inserted.Should().BeGreaterThan(0);
        observer2.Inserted.Should().BeGreaterThan(0);
        (await _stack.CountRowsAsync("processed_events", prefix)).Should().Be(load.Unique);
        ((long)observer1.Inserted + observer2.Inserted).Should().Be(load.Unique, "no unique event is counted as inserted twice");
        ((long)observer1.Duplicates + observer2.Duplicates).Should().BeGreaterThanOrEqualTo(load.Duplicates);
        (await _stack.CountRowsAsync("alerts", prefix)).Should().Be(load.SpeedingVehicles, "one SPEEDING alert per speeding vehicle, never two");
        (await ScalarAsync("SELECT count(DISTINCT vehicle_id) FROM alerts WHERE vehicle_id LIKE @p || '%'", prefix))
            .Should().Be(load.SpeedingVehicles);
        await AssertStateAsync(prefix, load);
        _output.WriteLine($"Duration {sw.Elapsed.TotalSeconds:F1} s");
    }

    private WorkerHarness SlowWorker(string topic, string group, Dictionary<string, string?> settings, CountingObserver observer) =>
        new(_stack, topic, group, settings, services =>
        {
            services.AddSingleton<IProcessingObserver>(observer);
            services.AddSingleton<IBatchCommitter>(sp =>
                new SlowCommitter(ActivatorUtilities.CreateInstance<BatchCommitter>(sp), TimeSpan.FromMilliseconds(100)));
        });

    private async Task<(string Topic, string Group, string Prefix)> NewRunAsync()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        await _stack.CreateTopicAsync(topic, 12);
        return (topic, TelemetryStackFixture.UniqueName("group"), TelemetryStackFixture.UniqueName("veh"));
    }

    private Task WaitForRowsAsync(string prefix, long expected) =>
        TelemetryStackFixture.WaitUntilAsync(async () => await _stack.CountRowsAsync("processed_events", prefix) >= expected, ProcessingTimeout);

    private Task WaitForLastSeqAsync(string prefix, Load load) =>
        TelemetryStackFixture.WaitUntilAsync(async () =>
            await ScalarAsync("SELECT count(*) FROM vehicle_state WHERE vehicle_id LIKE @p || '%' AND last_seq = " + load.EventsPerVehicle, prefix)
            == load.Vehicles, ProcessingTimeout);

    /// <summary>Per-vehicle last_seq == max seq, no false out-of-order rows, and no event id stored twice.</summary>
    private async Task AssertStateAsync(string prefix, Load load)
    {
        (await ScalarAsync("SELECT count(*) FROM vehicle_state WHERE vehicle_id LIKE @p || '%'", prefix)).Should().Be(load.Vehicles);
        (await ScalarAsync(
                "SELECT count(*) FROM vehicle_state s WHERE s.vehicle_id LIKE @p || '%' AND s.last_seq = " +
                "(SELECT max(e.seq) FROM processed_events e WHERE e.vehicle_id = s.vehicle_id)", prefix))
            .Should().Be(load.Vehicles, "last_seq of every vehicle equals its max seq");
        (await ScalarAsync("SELECT count(*) FROM processed_events WHERE vehicle_id LIKE @p || '%' AND out_of_order", prefix))
            .Should().Be(0, "the load is in order per vehicle (idempotent producer), so no event may be flagged out-of-order");
        (await ScalarAsync("SELECT count(*) - count(DISTINCT (vehicle_id, seq)) FROM processed_events WHERE vehicle_id LIKE @p || '%'", prefix))
            .Should().Be(0, "no (vehicle, seq) stored twice");
    }

    private async Task<long> ScalarAsync(string sql, string prefix)
    {
        await using var conn = new NpgsqlConnection(_stack.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("p", prefix);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    /// <summary>
    /// In-order tracks per vehicle (seq 1..N), interleaved across vehicles, with <c>duplicates</c> exact re-sends of
    /// random earlier events placed right after a later one of the same vehicle. Every <c>speedingEvery</c>-th vehicle
    /// drives at 130 km/h (one SPEEDING alert each); the rest at 50 km/h.
    /// </summary>
    private sealed record Load(List<TelemetryEventDto> Messages, int Vehicles, int EventsPerVehicle, int Duplicates, int SpeedingVehicles)
    {
        public long Unique => (long)Vehicles * EventsPerVehicle;

        public static Load Create(string prefix, int vehicles, int eventsPerVehicle, int duplicates, int speedingEvery = 0)
        {
            var rng = new Random(42);
            var t0 = DateTimeOffset.UtcNow.AddMinutes(-30);
            var originals = new List<TelemetryEventDto>(vehicles * eventsPerVehicle);
            for (var seq = 1; seq <= eventsPerVehicle; seq++)
            {
                for (var v = 0; v < vehicles; v++)
                {
                    var speeding = speedingEvery > 0 && v % speedingEvery == 0;
                    originals.Add(new TelemetryEventDto
                    {
                        EventId = Guid.NewGuid(),
                        VehicleId = prefix + "-" + v,
                        Seq = seq,
                        TimestampUtc = t0.AddSeconds(seq * 10),
                        Lat = 40.0 + seq * 0.0001,
                        Lon = -3.0 + v * 0.01,
                        SpeedKmh = speeding ? 130 : 50,
                        Ignition = true,
                    });
                }
            }

            var dupAfter = new Dictionary<int, List<TelemetryEventDto>>();
            for (var i = 0; i < duplicates; i++)
            {
                var source = rng.Next(originals.Count);
                var at = Math.Min(originals.Count - 1, source + rng.Next(0, vehicles * 3)); // same or a later position
                if (!dupAfter.TryGetValue(at, out var list))
                {
                    dupAfter[at] = list = new List<TelemetryEventDto>();
                }

                list.Add(originals[source]);
            }

            var messages = new List<TelemetryEventDto>(originals.Count + duplicates);
            for (var i = 0; i < originals.Count; i++)
            {
                messages.Add(originals[i]);
                if (dupAfter.TryGetValue(i, out var dups))
                {
                    messages.AddRange(dups);
                }
            }

            var speedingVehicles = speedingEvery > 0 ? (vehicles + speedingEvery - 1) / speedingEvery : 0;
            return new Load(messages, vehicles, eventsPerVehicle, duplicates, speedingVehicles);
        }
    }

    /// <summary>Throws <see cref="SimulatedCrashException"/> on the Nth successful DB commit (any partition).</summary>
    private sealed class CrashOnNthCommit : IFaultInjector
    {
        private readonly int _n;
        private int _calls;
        private volatile bool _crashed;

        public CrashOnNthCommit(int n) => _n = n;

        public bool Crashed => _crashed;

        public int CrashedPartition { get; private set; } = -1;

        public ValueTask AfterDbCommitAsync(int partition, CancellationToken ct)
        {
            if (Interlocked.Increment(ref _calls) == _n)
            {
                CrashedPartition = partition;
                _crashed = true;
                throw new SimulatedCrashException($"Simulated crash after DB commit #{_n} on partition {partition}");
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class SlowCommitter : IBatchCommitter
    {
        private readonly IBatchCommitter _inner;
        private readonly TimeSpan _delay;

        public SlowCommitter(IBatchCommitter inner, TimeSpan delay)
        {
            _inner = inner;
            _delay = delay;
        }

        public async Task<PersistResult> CommitAsync(ReadOnlyMemory<TelemetryEvent> events, int partition, CancellationToken ct)
        {
            await Task.Delay(_delay, ct);
            return await _inner.CommitAsync(events, partition, ct);
        }
    }

    private sealed class CountingObserver : IProcessingObserver
    {
        private int _inserted;
        private int _duplicates;
        private int _redisFallbacks;

        public int Inserted => Volatile.Read(ref _inserted);

        public int Duplicates => Volatile.Read(ref _duplicates);

        public int RedisFallbacks => Volatile.Read(ref _redisFallbacks);

        public void OnBatchCommitted(int partition, int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts)
        {
            Interlocked.Add(ref _inserted, inserted);
            Interlocked.Add(ref _duplicates, duplicates);
        }

        public void OnDlq(string reason)
        {
        }

        public void OnRedisFallback() => Interlocked.Increment(ref _redisFallbacks);
    }
}
