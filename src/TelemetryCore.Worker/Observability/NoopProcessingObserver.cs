namespace TelemetryCore.Worker.Observability;

/// <summary>Default observer: records nothing.</summary>
public sealed class NoopProcessingObserver : IProcessingObserver
{
    public void OnBatchCommitted(int partition, int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts)
    {
    }

    public void OnDlq(string reason)
    {
    }

    public void OnRedisFallback()
    {
    }
}
