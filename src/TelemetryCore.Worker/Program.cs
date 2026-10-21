using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Serilog;
using Serilog.Extensions.Logging;
using Serilog.Formatting.Compact;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Dlq;
using TelemetryCore.Worker.Health;
using TelemetryCore.Worker.Ingestion;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Outbox;
using TelemetryCore.Worker.Persistence;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.Worker;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Length > 0 && args[0] == HealthProbe.Argument)
        {
            return await HealthProbe.RunAsync().ConfigureAwait(false);
        }

        await BuildApp(args).RunAsync().ConfigureAwait(false);
        return 0;
    }

    /// <summary>
    /// Builds the worker host (D12): the ingestion background service plus /metrics, /health/live and /health/ready.
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
        ConfigureLogging(builder);

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
        services.AddSingleton<CircuitStateTracker>();
        services.AddSingleton<IngestionStatus>();
        services.AddSingleton<BatchCommitter>();
        services.AddSingleton<IBatchCommitter>(sp => new ResilientBatchCommitter(sp.GetRequiredService<BatchCommitter>(),
            ResiliencePipelines.CreateDatabase(sp.GetRequiredService<CircuitStateTracker>(),
                sp.GetRequiredService<ILoggerFactory>().CreateLogger("TelemetryCore.Worker.Resilience.Database"),
                sp.GetRequiredService<TimeProvider>())));
        services.AddSingleton<ChannelDepthRegistry>();
        services.AddSingleton<TelemetryMetrics>();
        services.AddSingleton<IProcessingObserver>(sp => sp.GetRequiredService<TelemetryMetrics>());
        services.AddSingleton<IDedupeHint>(CreateDedupeHint);
        services.AddSingleton<IDlqPublisher, KafkaDlqPublisher>();
        services.AddSingleton<IFaultInjector, NoopFaultInjector>();
        services.AddSingleton<IngestionService>();
        services.AddHostedService(sp => sp.GetRequiredService<IngestionService>());
        // Bulkhead (SPEC §7): the outbox gets its own pool of at most 4 connections, apart from the ingestion pool.
        services.AddHostedService(sp => new OutboxPublisher(sp.GetRequiredService<WorkerOptions>(),
            NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(sp.GetRequiredService<WorkerOptions>().PostgresConnectionString)
            {
                MaxPoolSize = 4,
                ApplicationName = "telemetry-worker-outbox",
            }.ConnectionString), sp.GetRequiredService<ILogger<OutboxPublisher>>()));
        services.AddHealthChecks().AddCheck<ReadinessCheck>("readiness", tags: new[] { ReadinessCheck.Tag });
        var otel = services.AddOpenTelemetry().WithMetrics(metrics => metrics
            .AddMeter(TelemetryMetrics.MeterName)
            .AddPrometheusExporter());
        // Traces only when a collector is configured (OTEL_EXPORTER_OTLP_ENDPOINT, e.g. http://jaeger:4317); without
        // one no span is created at all. The service name comes from OTEL_SERVICE_NAME.
        if (!string.IsNullOrWhiteSpace(builder.Configuration[OtlpEndpointKey]))
        {
            otel.WithTracing(tracing => tracing
                .AddSource(Tracing.SourceName)
                .AddOtlpExporter());
        }

        overrides?.Invoke(services);

        var app = builder.Build();
        app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false });
        app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = check => check.Tags.Contains(ReadinessCheck.Tag) });
        app.MapPrometheusScrapingEndpoint(); // /metrics
        return app;
    }

    internal const string OtlpEndpointKey = "OTEL_EXPORTER_OTLP_ENDPOINT";

    /// <summary>
    /// Serilog compact JSON on the console (SPEC §10), one object per line. Serilog is only the sink: level filtering
    /// stays with the standard <c>Logging:LogLevel</c> configuration (defaults below; hot-path logs are Debug).
    /// </summary>
    private static void ConfigureLogging(WebApplicationBuilder builder)
    {
        builder.Logging.ClearProviders();
        builder.Logging.SetMinimumLevel(LogLevel.Information);
        builder.Logging.AddFilter("Microsoft.AspNetCore", LogLevel.Warning);
        builder.Logging.AddFilter("System.Net.Http", LogLevel.Warning);
        var logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .WriteTo.Console(new CompactJsonFormatter())
            .CreateLogger();
        // AddProvider, not AddSerilog: AddSerilog adds a provider rule that lets every level through, bypassing the filters above.
        builder.Logging.AddProvider(new SerilogLoggerProvider(logger, dispose: true));
    }

    /// <summary>Redis hint when <c>Worker:RedisConnectionString</c> is set; otherwise no hint (the DB key alone decides, D1).</summary>
    private static IDedupeHint CreateDedupeHint(IServiceProvider sp)
    {
        var connectionString = sp.GetRequiredService<WorkerOptions>().RedisConnectionString;
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return new NoopDedupeHint();
        }

        var observer = sp.GetRequiredService<IProcessingObserver>();
        return RedisDedupeHint.Connect(connectionString, ResiliencePipelines.CreateRedis(sp.GetRequiredService<TimeProvider>()),
            () =>
            {
                try
                {
                    observer.OnRedisFallback();
                }
                catch (Exception)
                {
                    // Metrics must never change the fallback path.
                }
            },
            sp.GetRequiredService<ILoggerFactory>().CreateLogger<RedisDedupeHint>());
    }
}
