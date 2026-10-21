using Polly;
using TelemetryCore.Domain;

namespace TelemetryCore.Worker.Persistence;

/// <summary>
/// Runs every batch commit through the DB pipeline (SPEC §7: retry 3x → circuit breaker → 2 s timeout per attempt).
/// When the pipeline gives up (or the circuit is open) the exception reaches the partition processor, which keeps the
/// batch and tries again after a delay (D11), so nothing is dropped.
/// </summary>
public sealed class ResilientBatchCommitter : IBatchCommitter
{
    private readonly IBatchCommitter _inner;
    private readonly ResiliencePipeline _pipeline;

    public ResilientBatchCommitter(IBatchCommitter inner, ResiliencePipeline pipeline)
    {
        _inner = inner;
        _pipeline = pipeline;
    }

    public Task<PersistResult> CommitAsync(ReadOnlyMemory<TelemetryEvent> events, int partition, CancellationToken ct) =>
        _pipeline.ExecuteAsync(
            static (state, token) => new ValueTask<PersistResult>(state.Inner.CommitAsync(state.Events, state.Partition, token)),
            (Inner: _inner, Events: events, Partition: partition), ct).AsTask();
}
