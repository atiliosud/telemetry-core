using FluentAssertions;
using Npgsql;
using TelemetryCore.Domain;
using TelemetryCore.IntegrationTests.Fixtures;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.IntegrationTests;

[Collection(TelemetryStackCollection.Name)]
public sealed class BatchCommitterTests
{
    private readonly TelemetryStackFixture _stack;

    public BatchCommitterTests(TelemetryStackFixture stack) => _stack = stack;

    [Fact]
    public async Task AmbiguousCommit_EvictsCache_NextBatchUsesDatabaseState()
    {
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
        // Zig-zag path, so "skipping" e3 would give a different distance than going through it.
        var e1 = Evt(vehicle, 1, t0, 40.0000, -3.0000);
        var e2 = Evt(vehicle, 2, t0.AddSeconds(1), 40.0001, -3.0000);
        var e3 = Evt(vehicle, 3, t0.AddSeconds(2), 40.0001, -3.0002);
        var e4 = Evt(vehicle, 4, t0.AddSeconds(3), 40.0002, -3.0000);

        await using var dataSource = NpgsqlDataSource.Create(_stack.WorkerConnectionString);
        var cache = new VehicleStateCache();
        var repository = new TelemetryRepository();
        var committer = new BatchCommitter(dataSource, repository, cache);
        var ambiguous = new BatchCommitter(dataSource, repository, cache)
        {
            // Postgres commits, but the client sees an error (e.g. connection dropped while reading the reply).
            CommitTransactionAsync = async (tx, ct) =>
            {
                await tx.CommitAsync(ct);
                throw new NpgsqlException("simulated connection loss after COMMIT");
            },
        };

        await committer.CommitAsync(new[] { e1, e2 }, 0, CancellationToken.None);

        var act = () => ambiguous.CommitAsync(new[] { e3 }, 0, CancellationToken.None);
        await act.Should().ThrowAsync<NpgsqlException>();
        cache.TryGet(vehicle, out _).Should().BeFalse("state of a batch with an unknown outcome must not stay cached");

        var retry = await committer.CommitAsync(new[] { e3 }, 0, CancellationToken.None);
        retry.Inserted.Should().Be(0, "the ambiguous commit did persist e3");

        var next = await committer.CommitAsync(new[] { e4 }, 0, CancellationToken.None);
        next.OutOfOrder.Should().Be(0);

        var expected = Geo.HaversineKm(e1.Lat, e1.Lon, e2.Lat, e2.Lon)
                       + Geo.HaversineKm(e2.Lat, e2.Lon, e3.Lat, e3.Lon)
                       + Geo.HaversineKm(e3.Lat, e3.Lon, e4.Lat, e4.Lon);
        var state = await _stack.GetStateAsync(vehicle);
        state!.LastSeq.Should().Be(4);
        state.DistanceKmToday.Should().BeApproximately(expected, 1e-9);
        cache.TryGet(vehicle, out var cached).Should().BeTrue();
        cached.DistanceKmToday.Should().BeApproximately(expected, 1e-9);
    }

    [Fact]
    public async Task EmptyCache_HydratesFromDatabase_AndContinues()
    {
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var ts = DateTimeOffset.UtcNow.AddMinutes(-5);
        var previous = ts.AddSeconds(-1);
        await using (var conn = new NpgsqlConnection(_stack.AdminConnectionString))
        {
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand(
                "INSERT INTO vehicle_state (vehicle_id, last_seq, last_event_utc, lat, lon, speed_kmh, ignition, distance_km_today, " +
                "speeding, updated_utc, overspeed_streak, distance_date) " +
                "VALUES (@id, 10, @prev, 40.0, -3.0, 130, true, 5.0, false, now(), 2, @date)", conn);
            cmd.Parameters.AddWithValue("id", vehicle);
            cmd.Parameters.AddWithValue("prev", previous);
            cmd.Parameters.AddWithValue("date", DateOnly.FromDateTime(previous.UtcDateTime));
            await cmd.ExecuteNonQueryAsync();
        }

        await using var dataSource = NpgsqlDataSource.Create(_stack.WorkerConnectionString);
        var committer = new BatchCommitter(dataSource, new TelemetryRepository(), new VehicleStateCache());
        var evt = new TelemetryEvent(Guid.NewGuid(), vehicle, 11, ts, 40.0001, -3.0, 130, true);

        var result = await committer.CommitAsync(new[] { evt }, 0, CancellationToken.None);

        result.OutOfOrder.Should().Be(0, "seq 11 continues from the persisted last_seq 10");
        result.Alerts.Should().ContainSingle("the persisted streak of 2 plus this event reaches 3");
        var state = await _stack.GetStateAsync(vehicle);
        state!.LastSeq.Should().Be(11);
        state.OverspeedStreak.Should().Be(3);
        state.Speeding.Should().BeTrue();
        var expectedDistance = DateOnly.FromDateTime(ts.UtcDateTime) == DateOnly.FromDateTime(previous.UtcDateTime)
            ? 5.0 + Geo.HaversineKm(40.0, -3.0, 40.0001, -3.0)
            : Geo.HaversineKm(40.0, -3.0, 40.0001, -3.0); // crossed UTC midnight: daily reset (D6)
        state.DistanceKmToday.Should().BeApproximately(expectedDistance, 1e-9);
    }

    private static TelemetryEvent Evt(string vehicle, long seq, DateTimeOffset ts, double lat, double lon) =>
        new(Guid.NewGuid(), vehicle, seq, ts, lat, lon, 50, true);
}
