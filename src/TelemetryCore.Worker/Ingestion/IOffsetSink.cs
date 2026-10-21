namespace TelemetryCore.Worker.Ingestion;

/// <summary>Receives "processed up to here" marks (D9). <paramref name="nextOffset"/> is the last processed offset + 1.</summary>
public interface IOffsetSink
{
    void Store(int partition, long nextOffset);
}
