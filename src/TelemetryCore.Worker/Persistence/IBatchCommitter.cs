using TelemetryCore.Domain;

namespace TelemetryCore.Worker.Persistence;

/// <summary>Writes one batch (events, rules, state, alerts, outbox) in a single DB transaction.</summary>
public interface IBatchCommitter
{
    Task<PersistResult> CommitAsync(ReadOnlyMemory<TelemetryEvent> events, int partition, CancellationToken ct);
}
