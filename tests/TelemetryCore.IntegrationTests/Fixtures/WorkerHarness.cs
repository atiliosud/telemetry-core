using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using WorkerProgram = TelemetryCore.Worker.Program;

namespace TelemetryCore.IntegrationTests.Fixtures;

/// <summary>Hosts the real worker (<see cref="WorkerProgram.BuildApp"/>) in-process against the test containers.</summary>
public sealed class WorkerHarness : IAsyncDisposable
{
    private readonly WebApplication _app;
    private bool _started;
    private bool _stopped;

    public WorkerHarness(TelemetryStackFixture stack, string topic, string groupId,
        IDictionary<string, string?>? settings = null, Action<IServiceCollection>? overrides = null)
    {
        var config = new Dictionary<string, string?>
        {
            ["Worker:BootstrapServers"] = stack.BootstrapServers,
            ["Worker:Topic"] = topic,
            ["Worker:GroupId"] = groupId,
            ["Worker:PostgresConnectionString"] = stack.WorkerConnectionString,
            ["Urls"] = "http://127.0.0.1:0",
            ["Logging:LogLevel:Default"] = "Error",
        };
        if (settings is not null)
        {
            foreach (var kv in settings)
            {
                config[kv.Key] = kv.Value;
            }
        }

        _app = WorkerProgram.BuildApp(Array.Empty<string>(), overrides, cfg => cfg.AddInMemoryCollection(config));
    }

    public IServiceProvider Services => _app.Services;

    public async Task StartAsync()
    {
        await _app.StartAsync();
        _started = true;
    }

    /// <summary>Graceful stop: drain, flush, commit offsets, close the consumer.</summary>
    public async Task StopAsync()
    {
        if (_started && !_stopped)
        {
            _stopped = true;
            await _app.StopAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _app.DisposeAsync();
    }
}
