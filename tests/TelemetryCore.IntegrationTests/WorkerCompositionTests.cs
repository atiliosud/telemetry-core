using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Trace;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Persistence;
using WorkerProgram = TelemetryCore.Worker.Program;

namespace TelemetryCore.IntegrationTests;

/// <summary>Production composition root, without starting the host (no containers needed).</summary>
public sealed class WorkerCompositionTests
{
    [Fact]
    public async Task BuildApp_WiresResilientCommitterAndMetricsObserver()
    {
        var app = WorkerProgram.BuildApp(Array.Empty<string>(), configure: cfg => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Worker:PostgresConnectionString"] = "Host=127.0.0.1;Port=1;Database=x;Username=x;Password=x",
            ["Worker:BootstrapServers"] = "127.0.0.1:1",
        }));
        await using var _ = app;

        app.Services.GetRequiredService<IBatchCommitter>().Should().BeOfType<ResilientBatchCommitter>(
            "every batch commit must go through the DB pipeline (SPEC §7)");
        app.Services.GetRequiredService<IProcessingObserver>().Should().BeOfType<TelemetryMetrics>();
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData("http://127.0.0.1:4317", true)]
    public async Task BuildApp_TracesOnlyWithOtlpEndpoint_AndLogsThroughSerilog(string? endpoint, bool tracing)
    {
        var app = WorkerProgram.BuildApp(Array.Empty<string>(), configure: cfg => cfg.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Worker:PostgresConnectionString"] = "Host=127.0.0.1;Port=1;Database=x;Username=x;Password=x",
            ["Worker:BootstrapServers"] = "127.0.0.1:1",
            ["OTEL_EXPORTER_OTLP_ENDPOINT"] = endpoint,
        }));
        await using var _ = app;

        (app.Services.GetService<TracerProvider>() is not null).Should().Be(tracing);
        app.Services.GetServices<ILoggerProvider>().Should().ContainSingle()
            .Which.GetType().FullName.Should().Be("Serilog.Extensions.Logging.SerilogLoggerProvider", "logs are Serilog compact JSON (SPEC §10)");
        var loggers = app.Services.GetRequiredService<ILoggerFactory>();
        var worker = loggers.CreateLogger("TelemetryCore.Worker.Ingestion.PartitionProcessor");
        worker.IsEnabled(LogLevel.Information).Should().BeTrue();
        worker.IsEnabled(LogLevel.Debug).Should().BeFalse("per-batch and per-duplicate logs are Debug and must stay off by default");
        loggers.CreateLogger("Microsoft.AspNetCore.Routing.EndpointRoutingMiddleware").IsEnabled(LogLevel.Information).Should().BeFalse(
            "request logs of every scrape would flood the console");
    }
}
