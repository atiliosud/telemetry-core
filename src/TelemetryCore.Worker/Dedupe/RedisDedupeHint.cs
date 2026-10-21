using Polly;
using StackExchange.Redis;

namespace TelemetryCore.Worker.Dedupe;

/// <summary>
/// Redis dedupe hint (R2, D1). Keys <c>dedupe:{eventId:N}</c> are written only after the batch commit, so a hit always
/// means "already committed". Every call is guarded by the Redis pipeline (200 ms timeout); a failure or timeout never
/// throws: <see cref="AreKnownAsync"/> reports every event as unknown and <see cref="MarkAsync"/> gives up, both call
/// <c>onFallback</c> (feeds <c>telemetry_redis_fallback_total</c>). Only cancellation of the caller's token propagates.
/// </summary>
public sealed partial class RedisDedupeHint : IDedupeHint, IDisposable
{
    public const string KeyPrefix = "dedupe:";
    public static readonly TimeSpan KeyTtl = TimeSpan.FromSeconds(86400);

    private static readonly RedisValue Marker = 1;

    private readonly IConnectionMultiplexer _redis;
    private readonly bool _ownsConnection;
    private readonly ResiliencePipeline _pipeline;
    private readonly Action _onFallback;
    private readonly ILogger _log;

    public RedisDedupeHint(IConnectionMultiplexer redis, ResiliencePipeline pipeline, Action onFallback, ILogger log)
        : this(redis, ownsConnection: false, pipeline, onFallback, log)
    {
    }

    private RedisDedupeHint(IConnectionMultiplexer redis, bool ownsConnection, ResiliencePipeline pipeline, Action onFallback, ILogger log)
    {
        _redis = redis;
        _ownsConnection = ownsConnection;
        _pipeline = pipeline;
        _onFallback = onFallback;
        _log = log;
        _redis.ConnectionFailed += OnConnectionFailed;
        _redis.ConnectionRestored += OnConnectionRestored;
    }

    /// <summary>
    /// Connects without failing when Redis is down (<c>AbortOnConnectFail = false</c>, <c>ConnectTimeout</c> 1 s): the worker
    /// starts anyway and the multiplexer keeps reconnecting in the background. While disconnected, commands fail fast
    /// instead of queueing.
    /// </summary>
    public static RedisDedupeHint Connect(string connectionString, ResiliencePipeline pipeline, Action onFallback, ILogger log)
    {
        var options = ConfigurationOptions.Parse(connectionString);
        options.AbortOnConnectFail = false;
        options.ConnectTimeout = 1000;
        options.BacklogPolicy = BacklogPolicy.FailFast;
        return new RedisDedupeHint(ConnectionMultiplexer.Connect(options), ownsConnection: true, pipeline, onFallback, log);
    }

    public static string Key(Guid eventId) => KeyPrefix + eventId.ToString("N");

    /// <summary>One <c>MGET</c> for the whole batch.</summary>
    public async ValueTask<bool[]> AreKnownAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (ids.IsEmpty)
        {
            return Array.Empty<bool>();
        }

        var keys = ToKeys(ids.Span);
        try
        {
            var values = await _pipeline.ExecuteAsync(
                static async (state, token) => await Observed(state.Db.StringGetAsync(state.Keys)).WaitAsync(token).ConfigureAwait(false),
                (Db: _redis.GetDatabase(), Keys: keys), ct).ConfigureAwait(false);
            var known = new bool[keys.Length];
            for (var i = 0; i < known.Length && i < values.Length; i++)
            {
                known[i] = !values[i].IsNull;
            }

            return known;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            Fallback(ex, "check");
            return new bool[keys.Length];
        }
    }

    /// <summary>Pipelined <c>SET key 1 EX 86400</c>, one per event. Call only after the batch has committed (D1).</summary>
    public async ValueTask MarkAsync(ReadOnlyMemory<Guid> ids, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (ids.IsEmpty)
        {
            return;
        }

        var keys = ToKeys(ids.Span);
        try
        {
            await _pipeline.ExecuteAsync(static async (state, token) =>
            {
                var batch = state.Db.CreateBatch();
                var sets = new Task[state.Keys.Length];
                for (var i = 0; i < sets.Length; i++)
                {
                    sets[i] = batch.StringSetAsync(state.Keys[i], Marker, KeyTtl);
                }

                batch.Execute();
                await Observed(Task.WhenAll(sets)).WaitAsync(token).ConfigureAwait(false);
            }, (Db: _redis.GetDatabase(), Keys: keys), ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            Fallback(ex, "mark");
        }
    }

    public void Dispose()
    {
        _redis.ConnectionFailed -= OnConnectionFailed;
        _redis.ConnectionRestored -= OnConnectionRestored;
        if (_ownsConnection)
        {
            _redis.Dispose();
        }
    }

    private static RedisKey[] ToKeys(ReadOnlySpan<Guid> ids)
    {
        var keys = new RedisKey[ids.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            keys[i] = Key(ids[i]);
        }

        return keys;
    }

    /// <summary>A command abandoned by the timeout may fault later; observe it so it never surfaces as an unobserved exception.</summary>
    private static Task<T> Observed<T>(Task<T> task)
    {
        _ = task.ContinueWith(static t => _ = t.Exception, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
    }

    private static Task Observed(Task task)
    {
        _ = task.ContinueWith(static t => _ = t.Exception, CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
    }

    private void Fallback(Exception ex, string operation)
    {
        _onFallback();
        LogFallback(_log, ex, operation); // per call at Debug; connection loss/restore is logged once at Warning/Information
    }

    private void OnConnectionFailed(object? sender, ConnectionFailedEventArgs e) =>
        LogConnectionFailed(_log, e.Exception, e.EndPoint?.ToString(), e.FailureType.ToString());

    private void OnConnectionRestored(object? sender, ConnectionFailedEventArgs e) => LogConnectionRestored(_log, e.EndPoint?.ToString());

    [LoggerMessage(Level = LogLevel.Debug, Message = "Redis dedupe {Operation} failed; falling back to the DB key")]
    private static partial void LogFallback(ILogger log, Exception ex, string operation);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Redis connection to {EndPoint} failed ({FailureType}); dedupe falls back to the DB key")]
    private static partial void LogConnectionFailed(ILogger log, Exception? ex, string? endPoint, string failureType);

    [LoggerMessage(Level = LogLevel.Information, Message = "Redis connection to {EndPoint} restored")]
    private static partial void LogConnectionRestored(ILogger log, string? endPoint);
}
