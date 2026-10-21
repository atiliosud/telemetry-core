using Polly;

namespace TelemetryCore.Worker.Resilience;

/// <summary>Polly v8 pipelines of the worker (SPEC §7). One place, so the policy values stay in sync with the spec.</summary>
public static class ResiliencePipelines
{
    public static readonly TimeSpan RedisTimeout = TimeSpan.FromMilliseconds(200);

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
}
