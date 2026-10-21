using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using TelemetryCore.Contracts;
using TelemetryCore.IntegrationTests.Fixtures;
using TelemetryCore.Worker;
using TelemetryCore.Worker.Outbox;

namespace TelemetryCore.IntegrationTests;

[Collection(TelemetryStackCollection.Name)]
public sealed class OutboxTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(60);
    private readonly TelemetryStackFixture _stack;

    public OutboxTests(TelemetryStackFixture stack) => _stack = stack;

    [Fact]
    public async Task SpeedingVehicle_RaisesOneAlert_PublishedToAlertsTopic()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        await _stack.CreateTopicAsync(topic, 12);
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-5);
        var speeds = new[] { 121f, 125f, 130f, 135f, 100f };
        var events = new List<TelemetryEventDto>();
        for (var i = 0; i < speeds.Length; i++)
        {
            events.Add(new TelemetryEventDto
            {
                EventId = Guid.NewGuid(), VehicleId = vehicle, Seq = i + 1, TimestampUtc = t0.AddSeconds(i),
                Lat = 40.0 + i * 0.0001, Lon = -3.0, SpeedKmh = speeds[i], Ignition = true,
            });
        }

        await using var worker = new WorkerHarness(_stack, topic, TelemetryStackFixture.UniqueName("group"));
        await worker.StartAsync();
        await _stack.ProduceAsync(events, topic);

        await TelemetryStackFixture.WaitUntilAsync(async () => await PendingAsync(vehicle) == 0 && await _stack.CountRowsAsync("outbox", vehicle) == 1, Timeout);
        await worker.StopAsync();

        (await _stack.CountRowsAsync("alerts", vehicle)).Should().Be(1);
        (await AlertIdAsync(vehicle)).Should().Be(events[2].EventId!.Value, "alert id = id of the 3rd (raising) event");
        var messages = (await _stack.ConsumeAllAsync(Topics.Alerts)).Where(m => m.Message.Key == vehicle).ToList();
        messages.Should().ContainSingle();
        var alert = JsonSerializer.Deserialize(messages[0].Message.Value!, TelemetryJsonContext.Default.SpeedingAlertMessage)!;
        alert.Type.Should().Be("SPEEDING");
        alert.AlertId.Should().Be(events[2].EventId!.Value);
        alert.VehicleId.Should().Be(vehicle);
    }

    [Fact]
    public async Task KafkaDown_OutboxRowsStayPending_ThenPublish()
    {
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        var alertId = Guid.NewGuid();
        var payload = JsonSerializer.Serialize(
            new SpeedingAlertMessage(alertId, vehicle, "SPEEDING", DateTimeOffset.UtcNow, 3, 40, -3, 130), TelemetryJsonContext.Default.SpeedingAlertMessage);
        await using (var admin = new NpgsqlConnection(_stack.AdminConnectionString))
        {
            await admin.OpenAsync();
            await using var insert = new NpgsqlCommand("INSERT INTO outbox (topic, key, payload) VALUES (@t, @k, @p::jsonb)", admin);
            insert.Parameters.AddWithValue("t", Topics.Alerts);
            insert.Parameters.AddWithValue("k", vehicle);
            insert.Parameters.AddWithValue("p", payload);
            await insert.ExecuteNonQueryAsync();
        }

        await using (var down = Start("127.0.0.1:1"))
        {
            await TelemetryStackFixture.WaitUntilAsync(() => Task.FromResult(down.SendFailures > 0), TimeSpan.FromSeconds(30));
            await down.StopAsync(CancellationToken.None);
        }

        (await PendingAsync(vehicle)).Should().Be(1, "Kafka was unreachable");

        await using (var up = Start(_stack.BootstrapServers))
        {
            await TelemetryStackFixture.WaitUntilAsync(async () => await PendingAsync(vehicle) == 0, Timeout);
            await up.StopAsync(CancellationToken.None);
        }

        var messages = (await _stack.ConsumeAllAsync(Topics.Alerts)).Where(m => m.Message.Key == vehicle).ToList();
        messages.Should().ContainSingle();
    }

    [Fact]
    public async Task PoisonRow_StaysPending_AndDoesNotBlockValidRows()
    {
        var prefix = TelemetryStackFixture.UniqueName("veh");
        var poison = prefix + "-poison";
        var good = new[] { prefix + "-a", prefix + "-b" };
        await InsertOutboxAsync("bad topic!", poison); // lowest id: first in every poll
        foreach (var key in good)
        {
            await InsertOutboxAsync(Topics.Alerts, key);
        }

        await using var publisher = Start(_stack.BootstrapServers);
        foreach (var key in good)
        {
            await TelemetryStackFixture.WaitUntilAsync(async () => await PendingAsync(key) == 0, Timeout);
        }

        await publisher.StopAsync(CancellationToken.None);
        (await PendingAsync(poison)).Should().Be(1, "the poison row is never marked");
        var keys = (await _stack.ConsumeAllAsync(Topics.Alerts)).Select(m => m.Message.Key).ToList();
        keys.Should().Contain(good);
    }

    private async Task InsertOutboxAsync(string topic, string key)
    {
        var payload = JsonSerializer.Serialize(
            new SpeedingAlertMessage(Guid.NewGuid(), key, "SPEEDING", DateTimeOffset.UtcNow, 3, 40, -3, 130), TelemetryJsonContext.Default.SpeedingAlertMessage);
        await using var admin = new NpgsqlConnection(_stack.AdminConnectionString);
        await admin.OpenAsync();
        await using var insert = new NpgsqlCommand("INSERT INTO outbox (topic, key, payload) VALUES (@t, @k, @p::jsonb)", admin);
        insert.Parameters.AddWithValue("t", topic);
        insert.Parameters.AddWithValue("k", key);
        insert.Parameters.AddWithValue("p", payload);
        await insert.ExecuteNonQueryAsync();
    }

    private StartedPublisher Start(string bootstrap)
    {
        var options = new WorkerOptions { BootstrapServers = bootstrap, OutboxPollInterval = TimeSpan.FromMilliseconds(100) };
        var publisher = new OutboxPublisher(options, NpgsqlDataSource.Create(_stack.WorkerConnectionString), NullLogger<OutboxPublisher>.Instance);
        publisher.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
        return new StartedPublisher(publisher);
    }

    private async Task<long> PendingAsync(string vehicle)
    {
        await using var conn = new NpgsqlConnection(_stack.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT count(*) FROM outbox WHERE key = @k AND published_utc IS NULL", conn);
        cmd.Parameters.AddWithValue("k", vehicle);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private async Task<Guid> AlertIdAsync(string vehicle)
    {
        await using var conn = new NpgsqlConnection(_stack.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand("SELECT alert_id FROM alerts WHERE vehicle_id = @k", conn);
        cmd.Parameters.AddWithValue("k", vehicle);
        return (Guid)(await cmd.ExecuteScalarAsync())!;
    }

    private sealed class StartedPublisher : IAsyncDisposable
    {
        private readonly OutboxPublisher _publisher;
        public StartedPublisher(OutboxPublisher publisher) => _publisher = publisher;
        public long SendFailures => _publisher.SendFailures;
        public Task StopAsync(CancellationToken ct) => _publisher.StopAsync(ct);
        public ValueTask DisposeAsync()
        {
            _publisher.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
