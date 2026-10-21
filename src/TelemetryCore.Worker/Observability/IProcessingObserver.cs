namespace TelemetryCore.Worker.Observability;

/// <summary>
/// Processing events the metrics layer needs (<c>telemetry_events_processed_total</c>, <c>telemetry_duplicates_total</c>,
/// <c>telemetry_dlq_total</c>, <c>telemetry_redis_fallback_total</c>, ...). Called from partition processors, so
/// implementations must be thread-safe and cheap. Callers guard every call: an observer exception is logged, never
/// allowed to change processing. The worker registers <see cref="TelemetryMetrics"/>.
/// </summary>
public interface IProcessingObserver
{
    /// <summary>
    /// One batch of valid events is done (committed, or fully skipped because the dedupe hint knew every event).
    /// <paramref name="duplicates"/> = events skipped by the hint + duplicates reported by the DB key.
    /// </summary>
    void OnBatchCommitted(int partition, int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts);

    /// <summary>A message was durably written to the DLQ with <paramref name="reason"/> (a <c>DlqReasons</c> value).</summary>
    void OnDlq(string reason);

    /// <summary>A Redis hint call failed or timed out; the DB key alone decided.</summary>
    void OnRedisFallback();

    /// <summary>The consume loop read one Kafka record (<c>telemetry_events_consumed_total</c>).</summary>
    void OnConsumed()
    {
    }

    /// <summary>Time to process one committed batch (<c>telemetry_batch_duration_seconds</c>).</summary>
    void RecordBatchDuration(double seconds)
    {
    }

    /// <summary>Now − event timestamp for one committed event (<c>telemetry_processing_latency_seconds</c>).</summary>
    void RecordLatency(double seconds)
    {
    }
}
