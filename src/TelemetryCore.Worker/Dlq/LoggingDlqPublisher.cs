using TelemetryCore.Worker.Ingestion;

namespace TelemetryCore.Worker.Dlq;

/// <summary>Temporary default until the Kafka DLQ publisher exists: only logs the rejected message.</summary>
public sealed partial class LoggingDlqPublisher : IDlqPublisher
{
    private readonly ILogger<LoggingDlqPublisher> _log;

    public LoggingDlqPublisher(ILogger<LoggingDlqPublisher> log) => _log = log;

    public Task PublishAsync(PartitionMessage msg, string reason, CancellationToken ct)
    {
        LogRejected(_log, msg.Partition, msg.Offset, reason);
        return Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Rejected message at partition {Partition} offset {Offset}: {Reason}")]
    private static partial void LogRejected(ILogger log, int partition, long offset, string reason);
}
