using System.Diagnostics.Metrics;
using System.Threading.Channels;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.Worker.Tests;

public sealed class TelemetryMetricsTests : IDisposable
{
    private readonly ServiceProvider _services = new ServiceCollection().AddMetrics().BuildServiceProvider();
    private readonly CircuitStateTracker _tracker = new();
    private readonly ChannelDepthRegistry _channels = new();
    private readonly Dictionary<string, IReadOnlyList<double>?> _buckets = new();
    private readonly TelemetryMetrics _metrics;
    private readonly MeterListener _listener = new();
    private readonly Dictionary<string, long> _totals = new();

    public TelemetryMetricsTests()
    {
        _metrics = new TelemetryMetrics(_services.GetRequiredService<IMeterFactory>(), _tracker, _channels);
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == TelemetryMetrics.MeterName && instrument.Meter.Scope == _services.GetRequiredService<IMeterFactory>())
            {
                if (instrument is Histogram<double> histogram)
                {
                    _buckets[instrument.Name] = histogram.Advice?.HistogramBucketBoundaries;
                }

                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument.Name, value, tags));
        _listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Record(instrument.Name, value, tags));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => RecordDouble(instrument.Name, value));
        _listener.Start();
    }

    [Fact]
    public void Counters_UseSpecNames_AndObserverMapping()
    {
        _metrics.OnConsumed();
        _metrics.OnConsumed();
        _metrics.OnBatchCommitted(partition: 1, inserted: 10, duplicates: 2, outOfOrder: 3, gpsJumps: 4, alerts: 5);
        _metrics.OnDlq("invalid_json");
        _metrics.OnDlq("invalid_json");
        _metrics.OnDlq("lat_out_of_range");
        _metrics.OnRedisFallback();

        _totals["telemetry_events_consumed"].Should().Be(2);
        _totals["telemetry_events_processed"].Should().Be(10);
        _totals["telemetry_duplicates"].Should().Be(2);
        _totals["telemetry_out_of_order"].Should().Be(3);
        _totals["telemetry_gps_jumps"].Should().Be(4);
        _totals["telemetry_alerts_raised|type=SPEEDING"].Should().Be(5);
        _totals["telemetry_dlq|reason=invalid_json"].Should().Be(2);
        _totals["telemetry_dlq|reason=lat_out_of_range"].Should().Be(1);
        _totals["telemetry_redis_fallback"].Should().Be(1);
    }

    [Fact]
    public void CircuitGauge_ReportsTrackerState()
    {
        _tracker.Set(1);
        _listener.RecordObservableInstruments();

        _totals["telemetry_circuit_breaker_state"].Should().Be(1);
    }

    [Fact]
    public void Histograms_UseSpecNames_SecondsUnit_AndBuckets()
    {
        _metrics.RecordBatchDuration(0.2);
        _metrics.RecordLatency(1.5);
        _metrics.RecordLatency(0.5);

        _doubles["telemetry_batch_duration_seconds"].Should().Equal(0.2);
        _doubles["telemetry_processing_latency_seconds"].Should().Equal(1.5, 0.5);
        _buckets["telemetry_batch_duration_seconds"].Should().NotBeNull();
        _buckets["telemetry_batch_duration_seconds"]![0].Should().Be(0.005);
        _buckets["telemetry_batch_duration_seconds"]![^1].Should().Be(5);
        _buckets["telemetry_processing_latency_seconds"]![0].Should().Be(0.01);
        _buckets["telemetry_processing_latency_seconds"]![^1].Should().Be(30);
    }

    [Fact]
    public void ChannelDepthGauge_ReportsLiveChannelsByPartition()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        var other = Channel.CreateUnbounded<int>();
        _channels.Register(3, channel.Reader);
        _channels.Register(7, other.Reader);
        _channels.Unregister(7);

        _listener.RecordObservableInstruments();

        _totals["telemetry_channel_depth|partition=3"].Should().Be(2);
        _totals.Should().NotContainKey("telemetry_channel_depth|partition=7", "revoked partitions stop reporting");
    }

    public void Dispose()
    {
        _listener.Dispose();
        _services.Dispose();
    }

    private readonly Dictionary<string, List<double>> _doubles = new();

    private void RecordDouble(string name, double value)
    {
        lock (_doubles)
        {
            if (!_doubles.TryGetValue(name, out var list))
            {
                _doubles[name] = list = new List<double>();
            }

            list.Add(value);
        }
    }

    private void Record(string name, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        var key = name;
        foreach (var tag in tags)
        {
            key += "|" + tag.Key + "=" + tag.Value;
        }

        lock (_totals)
        {
            _totals[key] = (_totals.TryGetValue(key, out var v) ? v : 0) + value;
        }
    }
}
