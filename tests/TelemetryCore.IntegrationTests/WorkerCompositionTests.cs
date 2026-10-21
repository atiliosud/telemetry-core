using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
}
