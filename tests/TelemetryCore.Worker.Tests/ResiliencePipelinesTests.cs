using System.Diagnostics;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Polly;
using Polly.CircuitBreaker;
using Polly.RateLimiting;
using Polly.Timeout;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.Worker.Tests;

public sealed class ResiliencePipelinesTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private readonly FakeTimeProvider _clock = new(new DateTimeOffset(2026, 10, 21, 12, 0, 0, TimeSpan.Zero));
    private readonly CircuitStateTracker _tracker = new();

    [Fact]
    public async Task DbPipeline_RetriesThreeTimes()
    {
        var pipeline = ResiliencePipelines.CreateDatabase(_tracker, NullLogger.Instance, _clock);
        var calls = 0;

        var result = await RunAsync(pipeline, _ =>
        {
            if (Interlocked.Increment(ref calls) <= 3)
            {
                throw new InvalidOperationException("simulated DB failure");
            }

            return ValueTask.FromResult(42);
        });

        result.Should().Be(42);
        calls.Should().Be(4, "1 attempt + 3 retries");
    }

    [Fact]
    public async Task DbPipeline_GivesUpAfterThreeRetries()
    {
        var pipeline = ResiliencePipelines.CreateDatabase(_tracker, NullLogger.Instance, _clock);
        var calls = 0;

        var act = () => RunAsync<int>(pipeline, _ =>
        {
            Interlocked.Increment(ref calls);
            throw new InvalidOperationException("simulated DB failure");
        });

        await act.Should().ThrowAsync<InvalidOperationException>();
        calls.Should().Be(4);
    }

    [Fact]
    public async Task DbPipeline_DoesNotRetryCallerCancellation()
    {
        var pipeline = ResiliencePipelines.CreateDatabase(_tracker, NullLogger.Instance, _clock);
        using var cts = new CancellationTokenSource();
        var calls = 0;

        var act = async () => await pipeline.ExecuteAsync<int>(_ =>
        {
            Interlocked.Increment(ref calls);
            cts.Cancel();
            throw new OperationCanceledException(cts.Token);
        }, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        calls.Should().Be(1, "cancellation by the caller (shutdown) is not a failure to retry");
    }

    [Fact]
    public async Task DbPipeline_OpensAfterHalfOfMin20Fail()
    {
        var pipeline = ResiliencePipelines.CreateDatabase(_tracker, NullLogger.Instance, _clock);
        var attempts = 0;

        // Every second attempt fails: the breaker sees exactly 50 % failures; it may open only once 20 attempts were sampled.
        while (Volatile.Read(ref attempts) < 20)
        {
            _tracker.State.Should().Be(0, "fewer than 20 attempts were sampled");
            try
            {
                await RunAsync(pipeline, _ =>
                {
                    var n = Interlocked.Increment(ref attempts);
                    if (n % 2 == 0)
                    {
                        throw new InvalidOperationException("simulated DB failure");
                    }

                    return ValueTask.FromResult(n);
                });
            }
            catch (BrokenCircuitException)
            {
                // The retry of the 20th attempt hit the circuit that attempt opened.
            }
        }

        attempts.Should().Be(20);
        _tracker.State.Should().Be(1, "50 % of 20 sampled attempts failed");

        var executed = false;
        var act = () => RunAsync(pipeline, _ =>
        {
            executed = true;
            return ValueTask.FromResult(0);
        });
        await act.Should().ThrowAsync<BrokenCircuitException>();
        executed.Should().BeFalse("an open circuit fails fast without touching the DB");
    }

    [Fact]
    public async Task DbPipeline_HalfOpensAfter15s_AndClosesOnSuccess()
    {
        var pipeline = ResiliencePipelines.CreateDatabase(_tracker, NullLogger.Instance, _clock);
        for (var i = 0; i < 6; i++)
        {
            // 6 calls x 4 attempts = 24 failed attempts.
            var act = () => RunAsync<int>(pipeline, _ => throw new InvalidOperationException("down"));
            await act.Should().ThrowAsync<Exception>();
        }

        _tracker.State.Should().Be(1);
        _clock.Advance(TimeSpan.FromSeconds(16));

        var result = await RunAsync(pipeline, _ =>
        {
            _tracker.State.Should().Be(2, "the probe call runs while half-open");
            return ValueTask.FromResult(7);
        });

        result.Should().Be(7);
        _tracker.State.Should().Be(0);
    }

    [Fact]
    public async Task DbPipeline_TimesOutAt2s()
    {
        var pipeline = ResiliencePipelines.CreateDatabase(_tracker, NullLogger.Instance, _clock);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstAttemptCancelledAt = new TaskCompletionSource<DateTimeOffset>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        var t0 = _clock.GetUtcNow();
        CancellationTokenRegistration firstAttemptRegistration = default;

        var run = pipeline.ExecuteAsync<int>(async token =>
        {
            if (Interlocked.Increment(ref attempts) == 1)
            {
                // Not disposed inside the attempt: the cancelled Task.Delay may resume this method (and leave its scope)
                // before the token reaches this callback.
                firstAttemptRegistration = token.Register(() => firstAttemptCancelledAt.TrySetResult(_clock.GetUtcNow()));
                started.SetResult();
            }

            await Task.Delay(TimeSpan.FromSeconds(100), _clock, token);
            return 0;
        }, CancellationToken.None).AsTask();

        await started.Task.WaitAsync(Timeout);
        _clock.Advance(TimeSpan.FromMilliseconds(1990));
        await Task.Delay(50);
        firstAttemptCancelledAt.Task.IsCompleted.Should().BeFalse("the attempt is still within its 2 s budget");

        // Step the fake clock 1 ms at a time until the attempt's token fires.
        var sw = Stopwatch.StartNew();
        while (!firstAttemptCancelledAt.Task.IsCompleted && sw.Elapsed < Timeout)
        {
            _clock.Advance(TimeSpan.FromMilliseconds(1));
            await Task.Delay(1);
        }

        (await firstAttemptCancelledAt.Task.WaitAsync(Timeout) - t0).Should()
            .BeCloseTo(TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(5), "each attempt has a 2 s budget");

        // Every retry also times out; the caller finally sees the timeout.
        var act = async () => await Drive(run);
        await act.Should().ThrowAsync<TimeoutRejectedException>();
        attempts.Should().Be(4);
        await firstAttemptRegistration.DisposeAsync();
    }

    [Fact]
    public async Task OutboxPipeline_RejectsBeyond4Plus100()
    {
        var pipeline = ResiliencePipelines.CreateOutbox();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var running = 0;
        var maxRunning = 0;

        var calls = new List<Task>();
        for (var i = 0; i < 105; i++)
        {
            calls.Add(pipeline.ExecuteAsync(async _ =>
            {
                var now = Interlocked.Increment(ref running);
                InterlockedMax(ref maxRunning, now);
                await release.Task;
                Interlocked.Decrement(ref running);
            }, CancellationToken.None).AsTask());
        }

        await Task.Delay(100);
        var rejected = calls.Where(t => t.IsFaulted).ToList();
        rejected.Should().ContainSingle("4 run and 100 queue; the 105th is rejected");
        rejected[0].Exception!.InnerException.Should().BeOfType<RateLimiterRejectedException>();
        Volatile.Read(ref running).Should().Be(4);

        release.SetResult();
        await Task.WhenAll(calls.Where(t => !t.IsFaulted)).WaitAsync(Timeout);
        maxRunning.Should().Be(4);
    }

    [Fact]
    public void Tracker_DefaultsToClosed()
    {
        new CircuitStateTracker().State.Should().Be(0);
    }

    /// <summary>Runs <paramref name="op"/> through the pipeline, advancing the fake clock so retry delays elapse.</summary>
    private Task<T> RunAsync<T>(ResiliencePipeline pipeline, Func<CancellationToken, ValueTask<T>> op) =>
        Drive(pipeline.ExecuteAsync(op, CancellationToken.None).AsTask());

    private async Task<T> Drive<T>(Task<T> task)
    {
        var sw = Stopwatch.StartNew();
        while (!task.IsCompleted)
        {
            if (sw.Elapsed > Timeout)
            {
                throw new TimeoutException("Pipeline call did not complete.");
            }

            _clock.Advance(TimeSpan.FromMilliseconds(50));
            await Task.Delay(1);
        }

        return await task;
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, current) != current)
        {
        }
    }
}
