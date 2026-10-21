namespace TelemetryCore.Worker.Dedupe;

/// <summary>Optional duplicate pre-check (D1). A hit always refers to an already committed event; the DB key stays the source of truth.</summary>
public interface IDedupeHint
{
    ValueTask<bool[]> AreKnownAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct);

    ValueTask MarkAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct);
}
