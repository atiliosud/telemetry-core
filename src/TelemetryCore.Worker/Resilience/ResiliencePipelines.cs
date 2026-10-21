using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace TelemetryCore.Worker.Resilience;

/// <summary>Polly v8 pipelines of the worker (SPEC §7). One place, so the policy values stay in sync with the spec.</summary>
public static partial class ResiliencePipelines
{
    public static readonly TimeSpan RedisTimeout = TimeSpan.FromMilliseconds(200);
    public static readonly TimeSpan DatabaseAttemptTimeout = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan DatabaseBreakDuration = TimeSpan.FromSeconds(15);

    /// <summary>
    /// PostgreSQL batch write: Retry 3x exponential + jitter (outer) → circuit breaker 50 % / 30 s / min 20 / break 15 s →
    /// timeout 2 s per attempt (inner). Cancellation by the caller (shutdown, drain abort) is never retried.
    /// Breaker transitions are published to <paramref name="tracker"/> (metrics gauge and readiness).
    /// </summary>
    public static ResiliencePipeline CreateDatabase(CircuitStateTracker tracker, ILogger log, TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(tracker);
        ArgumentNullException.ThrowIfNull(log);
        var builder = new ResiliencePipelineBuilder();
        if (time is not null)
        {
            builder.TimeProvider = time;
        }

        return builder
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                Delay = TimeSpan.FromMilliseconds(200),
                ShouldHandle = args => ValueTask.FromResult(IsFailure(args.Outcome.Exception, args.Context.CancellationToken)),
                OnRetry = args =>
                {
                    LogRetry(log, args.Outcome.Exception, args.AttemptNumber + 1, args.RetryDelay.TotalMilliseconds);
                    return ValueTask.CompletedTask;
                },
            })
            .AddCircuitBreaker(new CircuitBreakerStrategyOptions
            {
                FailureRatio = 0.5,
                SamplingDuration = TimeSpan.FromSeconds(30),
                MinimumThroughput = 20,
                BreakDuration = DatabaseBreakDuration,
                ShouldHandle = args => ValueTask.FromResult(IsFailure(args.Outcome.Exception, args.Context.CancellationToken)),
                OnOpened = args =>
                {
                    tracker.Set(CircuitStateTracker.Open);
                    LogOpened(log, args.Outcome.Exception, args.BreakDuration.TotalSeconds);
                    return ValueTask.CompletedTask;
                },
                OnHalfOpened = _ =>
                {
                    tracker.Set(CircuitStateTracker.HalfOpen);
                    LogHalfOpened(log);
                    return ValueTask.CompletedTask;
                },
                OnClosed = _ =>
                {
                    tracker.Set(CircuitStateTracker.Closed);
                    LogClosed(log);
                    return ValueTask.CompletedTask;
                },
            })
            .AddTimeout(DatabaseAttemptTimeout)
            .Build();
    }

    /// <summary>Redis dedupe hint: 200 ms timeout, no retry. A failure means "fall back to the DB key".</summary>
    public static ResiliencePipeline CreateRedis(TimeProvider? timeProvider = null)
    {
        var builder = new ResiliencePipelineBuilder();
        if (timeProvider is not null)
        {
            builder.TimeProvider = timeProvider;
        }

        return builder.AddTimeout(RedisTimeout).Build();
    }

    /// <summary>Outbox publisher bulkhead: at most 4 concurrent publishes, 100 queued, the rest rejected (isolated from ingestion).</summary>
    public static ResiliencePipeline CreateOutbox() =>
        new ResiliencePipelineBuilder().AddConcurrencyLimiter(permitLimit: 4, queueLimit: 100).Build();

    /// <summary>Any exception is a failure, except an <see cref="OperationCanceledException"/> caused by the caller's token.</summary>
    private static bool IsFailure(Exception? ex, CancellationToken callerToken) =>
        ex is not null && !(ex is OperationCanceledException && callerToken.IsCancellationRequested);

    [LoggerMessage(Level = LogLevel.Warning, Message = "DB batch write failed; retry {Attempt} in {DelayMs:F0} ms")]
    private static partial void LogRetry(ILogger log, Exception? ex, int attempt, double delayMs);

    [LoggerMessage(Level = LogLevel.Error, Message = "DB circuit breaker OPEN for {BreakSeconds} s; batches are kept and retried (backpressure)")]
    private static partial void LogOpened(ILogger log, Exception? ex, double breakSeconds);

    [LoggerMessage(Level = LogLevel.Warning, Message = "DB circuit breaker HALF-OPEN; probing")]
    private static partial void LogHalfOpened(ILogger log);

    [LoggerMessage(Level = LogLevel.Information, Message = "DB circuit breaker CLOSED")]
    private static partial void LogClosed(ILogger log);
}
