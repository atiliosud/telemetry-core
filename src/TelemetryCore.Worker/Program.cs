using Microsoft.Extensions.Options;
using Npgsql;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Dlq;
using TelemetryCore.Worker.Ingestion;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.Worker;

public static class Program
{
    public static Task Main(string[] args) => BuildApp(args).RunAsync();

    /// <summary>
    /// Builds the worker host (D12): the ingestion background service plus health endpoints.
    /// </summary>
    /// <param name="args">Command-line arguments.</param>
    /// <param name="overrides">Applied after the default registrations, so tests can replace services.</param>
    /// <param name="configure">Applied after the default configuration sources, so tests can supply settings
    /// (connection strings, bootstrap servers, topic, group id, <c>Urls</c>).</param>
    public static WebApplication BuildApp(string[] args, Action<IServiceCollection>? overrides = null,
        Action<IConfigurationBuilder>? configure = null)
    {
        var builder = WebApplication.CreateBuilder(args);
        configure?.Invoke(builder.Configuration);

        var services = builder.Services;
        services.AddOptions<WorkerOptions>()
            .Bind(builder.Configuration.GetSection(WorkerOptions.SectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.PostgresConnectionString), "Worker:PostgresConnectionString is required")
            .Validate(o => !string.IsNullOrWhiteSpace(o.BootstrapServers), "Worker:BootstrapServers is required")
            .Validate(o => o.BatchSize > 0 && o.ChannelCapacity > 0 && o.FlushInterval > TimeSpan.Zero, "Batch settings must be positive")
            .ValidateOnStart();
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<WorkerOptions>>().Value);
        services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(25));

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton(sp => NpgsqlDataSource.Create(sp.GetRequiredService<WorkerOptions>().PostgresConnectionString));
        services.AddSingleton<TelemetryRepository>();
        services.AddSingleton<VehicleStateCache>();
        services.AddSingleton<IBatchCommitter, BatchCommitter>();
        services.AddSingleton<IDedupeHint, NoopDedupeHint>();
        services.AddSingleton<IDlqPublisher, LoggingDlqPublisher>();
        services.AddHostedService<IngestionService>();
        services.AddHealthChecks();

        overrides?.Invoke(services);

        var app = builder.Build();
        app.MapHealthChecks("/health/live");
        app.MapHealthChecks("/health/ready"); // stub: real dependency checks come with the readiness task
        return app;
    }
}
