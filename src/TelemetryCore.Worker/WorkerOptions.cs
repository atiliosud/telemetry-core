using TelemetryCore.Contracts;

namespace TelemetryCore.Worker;

/// <summary>Worker settings, bound from the <c>Worker</c> configuration section (env: <c>Worker__BootstrapServers</c>, ...).</summary>
public sealed class WorkerOptions
{
    public const string SectionName = "Worker";

    public string BootstrapServers { get; set; } = "localhost:29092";
    public string Topic { get; set; } = Topics.Raw;
    public string DlqTopic { get; set; } = Topics.Dlq;
    public string GroupId { get; set; } = "telemetry-worker";
    public int ChannelCapacity { get; set; } = 1000;
    public int BatchSize { get; set; } = 500;
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromMilliseconds(100);
    public string PostgresConnectionString { get; set; } = string.Empty;
    public string? RedisConnectionString { get; set; }
    /// <summary>Kafka <c>session.timeout.ms</c>; null keeps the client default (45 s).</summary>
    public int? SessionTimeoutMs { get; set; }
    public TimeSpan OutboxPollInterval { get; set; } = TimeSpan.FromMilliseconds(500);
}
