using FluentAssertions;
using Npgsql;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;
using TelemetryCore.IntegrationTests.Fixtures;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.IntegrationTests;

[Collection(TelemetryStackCollection.Name)]
public sealed class PipelineTests
{
    private static readonly TimeSpan ProcessingTimeout = TimeSpan.FromSeconds(60);
    private readonly TelemetryStackFixture _stack;

    public PipelineTests(TelemetryStackFixture stack) => _stack = stack;

    [Fact]
    public async Task ProcessesAllEvents_StateReflectsLastInOrder()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        var prefix = TelemetryStackFixture.UniqueName("veh");
        await _stack.CreateTopicAsync(topic, 12);
        var vehicles = new[] { prefix + "-a", prefix + "-b", prefix + "-c" };

        await using var worker = new WorkerHarness(_stack, topic, TelemetryStackFixture.UniqueName("group"));
        await worker.StartAsync();
        await _stack.ProduceAsync(Track(vehicles, 50), topic);

        await TelemetryStackFixture.WaitUntilAsync(
            async () => await _stack.CountRowsAsync("processed_events", prefix) == 150, ProcessingTimeout);
        await TelemetryStackFixture.WaitUntilAsync(async () =>
        {
            foreach (var v in vehicles)
            {
                if ((await _stack.GetStateAsync(v))?.LastSeq != 50)
                {
                    return false;
                }
            }

            return true;
        }, ProcessingTimeout);
        await worker.StopAsync();

        (await _stack.CountRowsAsync("vehicle_state", prefix)).Should().Be(3);
        foreach (var v in vehicles)
        {
            var state = await _stack.GetStateAsync(v);
            state!.LastSeq.Should().Be(50);
            state.DistanceKmToday.Should().BeGreaterThan(0);
            state.Lat.Should().BeApproximately(40.0 + 50 * 0.0001, 1e-9);
        }
    }

    [Fact]
    public async Task SameEventTwiceInOneBatch_InsertsOnce()
    {
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
        var e1 = new TelemetryEvent(Guid.NewGuid(), vehicle, 1, t0, 40.0, -3.0, 50, true);
        var e2 = new TelemetryEvent(Guid.NewGuid(), vehicle, 2, t0.AddSeconds(1), 40.0001, -3.0, 55, true);

        await using var dataSource = NpgsqlDataSource.Create(_stack.WorkerConnectionString);
        var committer = new BatchCommitter(dataSource, new TelemetryRepository(), new VehicleStateCache());

        var result = await committer.CommitAsync(new[] { e1, e1, e2 }, partition: 0, CancellationToken.None);

        result.Inserted.Should().Be(2);
        result.Duplicates.Should().Be(1);
        result.OutOfOrder.Should().Be(0, "the repeated event must not be applied a second time");
        (await _stack.CountRowsAsync("processed_events", vehicle)).Should().Be(2);
        var state = await _stack.GetStateAsync(vehicle);
        state!.LastSeq.Should().Be(2);
        state.DistanceKmToday.Should().BeApproximately(Geo.HaversineKm(40.0, -3.0, 40.0001, -3.0), 1e-9);

        // Replaying the whole batch changes nothing (D1).
        var replay = await committer.CommitAsync(new[] { e1, e2 }, partition: 0, CancellationToken.None);
        replay.Inserted.Should().Be(0);
        replay.Duplicates.Should().Be(2);
        (await _stack.GetStateAsync(vehicle))!.DistanceKmToday.Should().Be(state.DistanceKmToday);
    }

    [Fact]
    public async Task SpeedingAlert_WrittenWithOutboxRowInSameBatch()
    {
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
        var events = new TelemetryEvent[4];
        for (var i = 0; i < events.Length; i++)
        {
            events[i] = new TelemetryEvent(Guid.NewGuid(), vehicle, i + 1, t0.AddSeconds(i), 40.0 + i * 0.0001, -3.0, 130, true);
        }

        await using var dataSource = NpgsqlDataSource.Create(_stack.WorkerConnectionString);
        var committer = new BatchCommitter(dataSource, new TelemetryRepository(), new VehicleStateCache());

        var result = await committer.CommitAsync(events, partition: 0, CancellationToken.None);

        result.Alerts.Should().ContainSingle().Which.AlertId.Should().Be(events[2].EventId, "alert id = id of the raising event (D8)");
        (await _stack.CountRowsAsync("alerts", vehicle)).Should().Be(1);
        (await _stack.CountRowsAsync("outbox", vehicle)).Should().Be(1);
        (await _stack.GetStateAsync(vehicle))!.Speeding.Should().BeTrue();
    }

    [Fact]
    public async Task OffsetsCommittedAfterRun()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        var prefix = TelemetryStackFixture.UniqueName("veh");
        var group = TelemetryStackFixture.UniqueName("group");
        await _stack.CreateTopicAsync(topic, 12);
        var vehicles = new string[10];
        for (var i = 0; i < vehicles.Length; i++)
        {
            vehicles[i] = prefix + "-" + i;
        }

        await _stack.ProduceAsync(Track(vehicles, 10), topic);
        await _stack.ProduceRawAsync(vehicles[0], "not json"u8.ToArray(), topic); // invalid messages also move the offset

        await using (var worker = new WorkerHarness(_stack, topic, group))
        {
            await worker.StartAsync();
            await TelemetryStackFixture.WaitUntilAsync(
                async () => await _stack.CountRowsAsync("processed_events", prefix) == 100, ProcessingTimeout);
            await worker.StopAsync();
        }

        var highs = await _stack.GetHighWatermarksAsync(topic);
        var committed = await _stack.GetCommittedOffsetsAsync(group, topic, highs.Keys);
        foreach (var (partition, high) in highs)
        {
            if (high > 0)
            {
                committed[partition].Should().Be(high, $"partition {partition} was fully processed");
            }
        }
    }

    /// <summary>Each vehicle moves north 0.0001 deg (~11 m) per second, seq 1..count, in order.</summary>
    private static IEnumerable<TelemetryEventDto> Track(IReadOnlyList<string> vehicles, int count)
    {
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-10);
        for (var seq = 1; seq <= count; seq++)
        {
            foreach (var v in vehicles)
            {
                yield return new TelemetryEventDto
                {
                    EventId = Guid.NewGuid(),
                    VehicleId = v,
                    Seq = seq,
                    TimestampUtc = t0.AddSeconds(seq),
                    Lat = 40.0 + seq * 0.0001,
                    Lon = -3.0,
                    SpeedKmh = 40,
                    Ignition = true,
                };
            }
        }
    }
}
