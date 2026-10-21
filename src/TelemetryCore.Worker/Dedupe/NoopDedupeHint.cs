namespace TelemetryCore.Worker.Dedupe;

/// <summary>No hint: every event goes to the DB, which decides what is new.</summary>
public sealed class NoopDedupeHint : IDedupeHint
{
    public ValueTask<bool[]> AreKnownAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct) =>
        ValueTask.FromResult(new bool[ids.Length]);

    public ValueTask MarkAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct) => ValueTask.CompletedTask;
}
