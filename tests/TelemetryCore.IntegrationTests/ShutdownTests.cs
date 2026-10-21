using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;
using TelemetryCore.IntegrationTests.Fixtures;
using TelemetryCore.Worker.Observability;
using TelemetryCore.Worker.Persistence;
using Xunit.Abstractions;

namespace TelemetryCore.IntegrationTests;

[Collection(TelemetryStackCollection.Name)]
public sealed class ShutdownTests
{
    private const int Vehicles = 200;
    private const int EventsPerVehicle = 100;
    private const int Total = Vehicles * EventsPerVehicle;
    private static readonly TimeSpan ProcessingTimeout = TimeSpan.FromSeconds(120);
    private readonly TelemetryStackFixture _stack;
    private readonly ITestOutputHelper _output;

    public ShutdownTests(TelemetryStackFixture stack, ITestOutputHelper output)
    {
        _stack = stack;
        _output = output;
    }

    [Fact]
    public async Task GracefulStop_CommitsLastBatch_UnderTwentySeconds()
    {
        var topic = TelemetryStackFixture.UniqueName("raw");
        var group = TelemetryStackFixture.UniqueName("group");
        var prefix = TelemetryStackFixture.UniqueName("veh");
        await _stack.CreateTopicAsync(topic, 12);
        await _stack.ProduceManyAsync(Events(prefix), topic);

        // Small, slowed-down batches keep the run going long enough to stop it in the middle,
        // with every channel full (1,000 buffered messages per partition) at stop time.
        var settings = new Dictionary<string, string?> { ["Worker:BatchSize"] = "100" };
        await using (var first = new WorkerHarness(_stack, topic, group, settings,
                         services => services.AddSingleton<IBatchCommitter>(sp =>
                             new SlowCommitter(ActivatorUtilities.CreateInstance<BatchCommitter>(sp), TimeSpan.FromMilliseconds(100)))))
        {
            await first.StartAsync();
            await TelemetryStackFixture.WaitUntilAsync(async () => await _stack.CountRowsAsync("processed_events", prefix) >= 2000,
                ProcessingTimeout);

            var sw = Stopwatch.StartNew();
            await first.StopAsync();
            sw.Stop();
            _output.WriteLine($"Graceful stop took {sw.Elapsed.TotalSeconds:F2} s");
            sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(20), "SPEC §7: graceful shutdown under 20 s");
        }

        var persistedAtStop = await _stack.CountRowsAsync("processed_events", prefix);
        _output.WriteLine($"Persisted at stop: {persistedAtStop} of {Total}");
        persistedAtStop.Should().BeLessThan(Total, "the worker was stopped mid-run");
        var committed = await _stack.GetCommittedOffsetsAsync(group, topic, Enumerable.Range(0, 12));
        committed.Values.Where(o => o > 0).Sum().Should().Be(persistedAtStop,
            "every persisted message (and only those) had its next offset committed: drained batches were flushed before the commit");

        var observer = new CountingObserver();
        await using (var second = new WorkerHarness(_stack, topic, group, overrides: services => services.AddSingleton<IProcessingObserver>(observer)))
        {
            await second.StartAsync();
            await TelemetryStackFixture.WaitUntilAsync(async () => await _stack.CountRowsAsync("processed_events", prefix) == Total,
                ProcessingTimeout);
            await TelemetryStackFixture.WaitUntilAsync(() => Task.FromResult(observer.Inserted == Total - persistedAtStop), ProcessingTimeout);
            await Task.Delay(500); // any replayed batch would show up as duplicates now
            await second.StopAsync();
        }

        (await _stack.CountRowsAsync("processed_events", prefix)).Should().Be(Total);
        observer.Duplicates.Should().Be(0, "nothing persisted before the stop is replayed after the restart");
        ((long)observer.Inserted).Should().Be(Total - persistedAtStop);
    }

    private static IEnumerable<TelemetryEventDto> Events(string prefix)
    {
        var t0 = DateTimeOffset.UtcNow.AddMinutes(-30);
        for (var seq = 1; seq <= EventsPerVehicle; seq++)
        {
            for (var v = 0; v < Vehicles; v++)
            {
                yield return new TelemetryEventDto
                {
                    EventId = Guid.NewGuid(),
                    VehicleId = prefix + "-" + v,
                    Seq = seq,
                    TimestampUtc = t0.AddSeconds(seq),
                    Lat = 40.0 + seq * 0.0001,
                    Lon = -3.0 + v * 0.01,
                    SpeedKmh = 40,
                    Ignition = true,
                };
            }
        }
    }

    private sealed class SlowCommitter : IBatchCommitter
    {
        private readonly IBatchCommitter _inner;
        private readonly TimeSpan _delay;

        public SlowCommitter(IBatchCommitter inner, TimeSpan delay)
        {
            _inner = inner;
            _delay = delay;
        }

        public async Task<PersistResult> CommitAsync(ReadOnlyMemory<TelemetryEvent> events, int partition, CancellationToken ct)
        {
            await Task.Delay(_delay, ct);
            return await _inner.CommitAsync(events, partition, ct);
        }
    }

    private sealed class CountingObserver : IProcessingObserver
    {
        private int _inserted;
        private int _duplicates;

        public int Inserted => Volatile.Read(ref _inserted);

        public int Duplicates => Volatile.Read(ref _duplicates);

        public void OnBatchCommitted(int partition, int inserted, int duplicates, int outOfOrder, int gpsJumps, int alerts)
        {
            Interlocked.Add(ref _inserted, inserted);
            Interlocked.Add(ref _duplicates, duplicates);
        }

        public void OnDlq(string reason)
        {
        }

        public void OnRedisFallback()
        {
        }
    }
}
