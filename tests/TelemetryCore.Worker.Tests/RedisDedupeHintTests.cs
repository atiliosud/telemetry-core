using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Polly.Timeout;
using TelemetryCore.Worker.Dedupe;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.Worker.Tests;

public sealed class RedisDedupeHintTests
{
    [Fact]
    public async Task UnreachableRedis_ReportsUnknownAndSwallowsMark_CountingFallbacks()
    {
        var fallbacks = 0;
        // Nothing listens on port 1: the multiplexer never connects (AbortOnConnectFail = false keeps it alive).
        using var hint = RedisDedupeHint.Connect("127.0.0.1:1", ResiliencePipelines.CreateRedis(),
            () => Interlocked.Increment(ref fallbacks), NullLogger.Instance);
        var ids = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };

        var sw = Stopwatch.StartNew();
        var known = await hint.AreKnownAsync(ids, CancellationToken.None);
        await hint.MarkAsync(ids, CancellationToken.None);
        sw.Stop();

        known.Should().Equal(false, false, false);
        fallbacks.Should().Be(2);
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(1), "each call is bounded by the 200 ms guard or fails fast");
    }

    [Fact]
    public async Task UnreachableRedis_CancelledToken_Propagates()
    {
        using var hint = RedisDedupeHint.Connect("127.0.0.1:1", ResiliencePipelines.CreateRedis(), () => { }, NullLogger.Instance);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var act = async () => await hint.AreKnownAsync(new[] { Guid.NewGuid() }, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task RedisPipeline_TimesOutAfter200ms()
    {
        var pipeline = ResiliencePipelines.CreateRedis();
        var sw = Stopwatch.StartNew();

        var act = async () => await pipeline.ExecuteAsync(async token => await Task.Delay(TimeSpan.FromSeconds(5), token), CancellationToken.None);

        await act.Should().ThrowAsync<TimeoutRejectedException>();
        sw.Elapsed.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(180)).And.BeLessThan(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void Key_UsesCompactGuidFormat()
    {
        var id = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");

        RedisDedupeHint.Key(id).Should().Be("dedupe:0f8fad5bd9cb469fa16570867728950e");
    }
}
