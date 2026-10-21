using System.Diagnostics.Metrics;
using TelemetryCore.Domain;
using TelemetryCore.Worker.Persistence;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.Worker.Observability;

/// <summary>
/// SPEC §10 metrics on the <c>TelemetryCore.Worker</c> meter: counters, the two latency histograms, the channel-depth
/// and circuit-breaker gauges. Exported by the Prometheus endpoint (<c>/metrics</c>), which appends <c>_total</c> to
/// counters (<c>telemetry_duplicates</c> → <c>telemetry_duplicates_total</c>). Histograms already carry the
/// <c>_seconds</c> suffix with unit <c>s</c>, so the exporter keeps the name as is.
/// Counters without labels (and the known label values) start at 0, so every series exists from the first scrape.
/// </summary>
public sealed class TelemetryMetrics : IProcessingObserver
{
    public const string MeterName = "TelemetryCore.Worker";

    private static readonly string[] KnownDlqReasons =
    {
        DlqReasons.PayloadTooLarge, DlqReasons.EmptyPayload, DlqReasons.InvalidJson, DlqReasons.MissingField,
        DlqReasons.LatOutOfRange, DlqReasons.LonOutOfRange, DlqReasons.SpeedOutOfRange, DlqReasons.TimestampInFuture,
        DlqReasons.TimestampTooOld,
    };

    /// <summary>Batch transaction time: 5 ms … 5 s.</summary>
    internal static readonly double[] BatchDurationBuckets = { 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5 };

    /// <summary>Event timestamp → persisted: 10 ms … 30 s.</summary>
    internal static readonly double[] LatencyBuckets = { 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30 };

    private static readonly KeyValuePair<string, object?> SpeedingTag = new("type", BatchCommitter.SpeedingAlertType);

    private readonly Counter<long> _consumed;
    private readonly Counter<long> _processed;
    private readonly Counter<long> _duplicates;
    private readonly Counter<long> _outOfOrder;
    private readonly Counter<long> _dlq;
    private readonly Counter<long> _gpsJumps;
    private readonly Counter<long> _alerts;
    private readonly Counter<long> _redisFallback;
    private readonly Histogram<double> _batchDuration;
    private readonly Histogram<double> _latency;

    public TelemetryMetrics(IMeterFactory meterFactory, CircuitStateTracker tracker, ChannelDepthRegistry channels)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);
        ArgumentNullException.ThrowIfNull(tracker);
        ArgumentNullException.ThrowIfNull(channels);
        var meter = meterFactory.Create(MeterName);
        _consumed = meter.CreateCounter<long>("telemetry_events_consumed", description: "Kafka records read by the consume loop.");
        _processed = meter.CreateCounter<long>("telemetry_events_processed", description: "New events persisted (DB key accepted them).");
        _duplicates = meter.CreateCounter<long>("telemetry_duplicates", description: "Events skipped as duplicates (Redis hint or DB key).");
        _outOfOrder = meter.CreateCounter<long>("telemetry_out_of_order", description: "Events stored as out-of-order (state unchanged).");
        _dlq = meter.CreateCounter<long>("telemetry_dlq", description: "Messages written to the DLQ, by reason.");
        _gpsJumps = meter.CreateCounter<long>("telemetry_gps_jumps", description: "GPS jumps detected (R4).");
        _alerts = meter.CreateCounter<long>("telemetry_alerts_raised", description: "Alerts raised, by type.");
        _redisFallback = meter.CreateCounter<long>("telemetry_redis_fallback", description: "Redis hint calls that failed or timed out.");
        meter.CreateObservableGauge("telemetry_circuit_breaker_state", () => tracker.State,
            description: "DB circuit breaker state: 0 closed, 1 open, 2 half-open.");
        meter.CreateObservableGauge("telemetry_channel_depth", () => ChannelDepths(channels),
            description: "Messages waiting in the bounded channel of each owned partition.");
        _batchDuration = meter.CreateHistogram("telemetry_batch_duration_seconds", "s", "Time to process one batch (dedupe, DB transaction, hint).",
            tags: null, new InstrumentAdvice<double> { HistogramBucketBoundaries = BatchDurationBuckets });
        _latency = meter.CreateHistogram("telemetry_processing_latency_seconds", "s", "Event timestamp to persisted, per committed event.",
            tags: null, new InstrumentAdvice<double> { HistogramBucketBoundaries = LatencyBuckets });

        foreach (var counter in new[] { _consumed, _processed, _duplicates, _outOfOrder, _gpsJumps, _redisFallback })
        {
            counter.Add(0);
        }

        foreach (var reason in KnownDlqReasons)
        {
            _dlq.Add(0, new KeyValuePair<string, object?>("reason", reason));
        }

        _alerts.Add(0, SpeedingTag);
    }

    public void OnConsumed() => _consumed.Add(1);

    public void OnBatchCommitted(int partition, int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts)
    {
        AddIfPositive(_processed, inserted);
        AddIfPositive(_duplicates, duplicates);
        AddIfPositive(_outOfOrder, outOfOrder);
        AddIfPositive(_gpsJumps, gpsJumps);
        if (alerts > 0)
        {
            _alerts.Add(alerts, SpeedingTag); // SPEEDING is the only alert type (R5)
        }
    }

    public void OnDlq(string reason) => _dlq.Add(1, new KeyValuePair<string, object?>("reason", reason));

    public void OnRedisFallback() => _redisFallback.Add(1);

    public void RecordBatchDuration(double seconds) => _batchDuration.Record(seconds);

    public void RecordLatency(double seconds) => _latency.Record(Math.Max(0, seconds)); // producer clock may run ahead

    private static IEnumerable<Measurement<int>> ChannelDepths(ChannelDepthRegistry channels)
    {
        foreach (var (partition, depth) in channels.Snapshot())
        {
            yield return new Measurement<int>(depth, new KeyValuePair<string, object?>("partition", partition));
        }
    }

    private static void AddIfPositive(Counter<long> counter, int value)
    {
        if (value > 0)
        {
            counter.Add(value);
        }
    }
}
