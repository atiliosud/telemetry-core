namespace TelemetryCore.Worker.Ingestion;

/// <summary>Whether the consume loop is running; read by the readiness check.</summary>
public sealed class IngestionStatus
{
    private int _running;

    public bool IsRunning => Volatile.Read(ref _running) == 1;

    public void SetRunning(bool running) => Volatile.Write(ref _running, running ? 1 : 0);
}
