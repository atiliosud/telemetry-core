namespace TelemetryCore.Worker.Ingestion;

/// <summary>
/// Test hook for crash scenarios (SPEC §16). Called by a partition processor after a batch is committed to the DB and
/// before its offset is stored. The worker registers <see cref="NoopFaultInjector"/>; a test implementation throws
/// <see cref="SimulatedCrashException"/> to make the worker stop as if the process died at that point.
/// </summary>
public interface IFaultInjector
{
    ValueTask AfterDbCommitAsync(int partition, CancellationToken ct);
}

public sealed class NoopFaultInjector : IFaultInjector
{
    public ValueTask AfterDbCommitAsync(int partition, CancellationToken ct) => ValueTask.CompletedTask;
}

/// <summary>
/// Fatal for the worker: the processor stops without storing the offset of the current batch, the host stops and the
/// consumer is disposed without the final offset commit (like a killed process).
/// </summary>
public sealed class SimulatedCrashException : Exception
{
    public SimulatedCrashException()
    {
    }

    public SimulatedCrashException(string message)
        : base(message)
    {
    }

    public SimulatedCrashException(string message, Exception inner)
        : base(message, inner)
    {
    }
}
