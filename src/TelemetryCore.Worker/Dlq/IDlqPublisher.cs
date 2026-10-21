using TelemetryCore.Worker.Ingestion;

namespace TelemetryCore.Worker.Dlq;

/// <summary>Publishes an invalid message to the dead-letter topic. Completes only when the message is durable (D17).</summary>
public interface IDlqPublisher
{
    Task PublishAsync(PartitionMessage msg, string reason, CancellationToken ct);
}
