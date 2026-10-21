using System.Net;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using TelemetryCore.Contracts;
using TelemetryCore.IntegrationTests.Fixtures;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.IntegrationTests;

[Collection(TelemetryStackCollection.Name)]
public sealed class WorkerEndpointTests
{
    private static readonly TimeSpan ProcessingTimeout = TimeSpan.FromSeconds(60);
    private readonly TelemetryStackFixture _stack;

    public WorkerEndpointTests(TelemetryStackFixture stack) => _stack = stack;

    [Fact]
    public async Task Metrics_ExposeSpecNames()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        var dlq = TelemetryStackFixture.UniqueName("dlq");
        var vehicle = TelemetryStackFixture.UniqueName("veh");
        await _stack.CreateTopicAsync(topic, 12);
        await _stack.CreateTopicAsync(dlq, 3);

        await using var worker = new WorkerHarness(_stack, topic, TelemetryStackFixture.UniqueName("group"),
            new Dictionary<string, string?> { ["Worker:DlqTopic"] = dlq });
        await worker.StartAsync();
        await _stack.ProduceAsync(new[]
        {
            new TelemetryEventDto
            {
                EventId = Guid.NewGuid(), VehicleId = vehicle, Seq = 1, TimestampUtc = DateTimeOffset.UtcNow.AddMinutes(-1),
                Lat = 40, Lon = -3, SpeedKmh = 50, Ignition = true,
            },
        }, topic);
        await _stack.ProduceRawAsync(vehicle, Encoding.UTF8.GetBytes("not json"), topic);

        using var http = new HttpClient { BaseAddress = worker.BaseAddress };
        string text = string.Empty;
        await TelemetryStackFixture.WaitUntilAsync(async () =>
        {
            text = await http.GetStringAsync("/metrics");
            return Value(text, "telemetry_events_processed_total") >= 1 && Value(text, "telemetry_dlq_total") >= 1;
        }, ProcessingTimeout);

        foreach (var name in new[]
                 {
                     "telemetry_events_consumed_total", "telemetry_events_processed_total", "telemetry_duplicates_total",
                     "telemetry_out_of_order_total", "telemetry_dlq_total", "telemetry_gps_jumps_total", "telemetry_alerts_raised_total",
                     "telemetry_redis_fallback_total", "telemetry_circuit_breaker_state",
                 })
        {
            text.Should().MatchRegex("(?m)^" + name + "[{ ]", $"{name} must be exported under its SPEC §10 name");
        }

        text.Should().Contain("reason=\"invalid_json\"");
        text.Should().Contain("type=\"SPEEDING\"");
        Value(text, "telemetry_events_consumed_total").Should().BeGreaterThanOrEqualTo(2);
        Value(text, "telemetry_circuit_breaker_state").Should().Be(0);
    }

    [Fact]
    public async Task Health_LiveAndReady_ReadyFailsWhenCircuitOpen()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        await _stack.CreateTopicAsync(topic, 12);
        await using var worker = new WorkerHarness(_stack, topic, TelemetryStackFixture.UniqueName("group"));
        await worker.StartAsync();
        using var http = new HttpClient { BaseAddress = worker.BaseAddress };

        (await http.GetAsync("/health/live")).StatusCode.Should().Be(HttpStatusCode.OK);
        await TelemetryStackFixture.WaitUntilAsync(async () => (await http.GetAsync("/health/ready")).StatusCode == HttpStatusCode.OK,
            ProcessingTimeout);

        var tracker = worker.Services.GetRequiredService<CircuitStateTracker>();
        tracker.Set(1);
        (await http.GetAsync("/health/ready")).StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
        (await http.GetAsync("/health/live")).StatusCode.Should().Be(HttpStatusCode.OK, "liveness does not depend on dependencies");

        tracker.Set(0);
        (await http.GetAsync("/health/ready")).StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task Health_ReadyFailsWhenDatabaseUnreachable()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        await _stack.CreateTopicAsync(topic, 12);
        await using var worker = new WorkerHarness(_stack, topic, TelemetryStackFixture.UniqueName("group"),
            new Dictionary<string, string?>
            {
                ["Worker:PostgresConnectionString"] = "Host=127.0.0.1;Port=1;Database=telemetry;Username=x;Password=y;Timeout=1",
            });
        await worker.StartAsync();
        using var http = new HttpClient { BaseAddress = worker.BaseAddress };

        (await http.GetAsync("/health/live")).StatusCode.Should().Be(HttpStatusCode.OK);
        (await http.GetAsync("/health/ready")).StatusCode.Should().Be(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Sum of all series of <paramref name="name"/> in a Prometheus text exposition (0 when absent).</summary>
    private static double Value(string text, string name)
    {
        double sum = 0;
        foreach (var line in text.Split('\n'))
        {
            if (!line.StartsWith(name, StringComparison.Ordinal) || line.Length <= name.Length || (line[name.Length] != '{' && line[name.Length] != ' '))
            {
                continue;
            }

            var rest = line[name.Length] == '{' ? line[(line.IndexOf('}', StringComparison.Ordinal) + 1)..] : line[name.Length..];
            var parts = rest.Trim().Split(' ');
            sum += double.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture);
        }

        return sum;
    }
}
