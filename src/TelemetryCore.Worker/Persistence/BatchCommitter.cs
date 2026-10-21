using Npgsql;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;

namespace TelemetryCore.Worker.Persistence;

/// <summary>
/// One batch = one transaction (D1, D5): insert events (the DB key decides what is new) → apply rules, in batch order,
/// to working copies of the touched vehicles → mark out-of-order rows → upsert state → alerts + outbox → commit →
/// only then publish the copies to the in-memory cache. A failed batch leaves neither the DB nor memory changed.
/// </summary>
public sealed class BatchCommitter : IBatchCommitter
{
    public const string SpeedingAlertType = "SPEEDING";

    private readonly NpgsqlDataSource _dataSource;
    private readonly TelemetryRepository _repository;
    private readonly VehicleStateCache _cache;

    public BatchCommitter(NpgsqlDataSource dataSource, TelemetryRepository repository, VehicleStateCache cache)
    {
        _dataSource = dataSource;
        _repository = repository;
        _cache = cache;
    }

    /// <summary>Commits the batch transaction. Test seam: lets tests simulate an ambiguous commit (committed, then a failure reported).</summary>
    internal Func<NpgsqlTransaction, CancellationToken, Task> CommitTransactionAsync { get; init; } = static (tx, ct) => tx.CommitAsync(ct);

    public async Task<PersistResult> CommitAsync(ReadOnlyMemory<TelemetryEvent> events, int partition, CancellationToken ct)
    {
        if (events.IsEmpty)
        {
            return new PersistResult(0, 0, 0, 0, Array.Empty<SpeedingAlertMessage>());
        }

        try
        {
            return await CommitCoreAsync(events, partition, ct).ConfigureAwait(false);
        }
        catch
        {
            // The outcome may be unknown (e.g. COMMIT reached Postgres but the reply was lost). A retry would then see
            // no new ids and never publish, so drop the cached state of every vehicle in the batch: the next attempt
            // reloads the truth from vehicle_state.
            EvictVehicles(events.Span);
            throw;
        }
    }

    private async Task<PersistResult> CommitCoreAsync(ReadOnlyMemory<TelemetryEvent> events, int partition, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);

        var newIds = await _repository.InsertEventsAsync(conn, tx, events, ct).ConfigureAwait(false);
        var outcome = new BatchOutcome(newIds.Count, events.Length - newIds.Count);
        if (newIds.Count > 0)
        {
            var touched = new Dictionary<string, VehicleState>(StringComparer.Ordinal);
            var misses = CloneCachedStates(events.Span, newIds, touched);
            if (misses is not null)
            {
                var loaded = await _repository.LoadStatesAsync(conn, tx, misses, ct).ConfigureAwait(false);
                foreach (var s in loaded)
                {
                    s.Partition = partition;
                    touched[s.VehicleId] = s;
                }

                foreach (var id in misses)
                {
                    if (!touched.ContainsKey(id))
                    {
                        touched[id] = VehicleState.New(id, partition);
                    }
                }
            }

            ApplyRules(events.Span, newIds, touched, outcome);

            if (outcome.OutOfOrderIds is not null)
            {
                await _repository.MarkOutOfOrderAsync(conn, tx, outcome.OutOfOrderIds.ToArray(), ct).ConfigureAwait(false);
            }

            await _repository.UpsertStatesAsync(conn, tx, touched.Values, ct).ConfigureAwait(false);
            await _repository.InsertAlertsAndOutboxAsync(conn, tx, outcome.Alerts, ct).ConfigureAwait(false);
            await CommitTransactionAsync(tx, ct).ConfigureAwait(false);
            _cache.Publish(touched.Values);
        }
        else
        {
            await CommitTransactionAsync(tx, ct).ConfigureAwait(false);
        }

        return new PersistResult(outcome.Inserted, outcome.Duplicates, outcome.OutOfOrderIds?.Count ?? 0, outcome.GpsJumps, outcome.Alerts);
    }

    private void EvictVehicles(ReadOnlySpan<TelemetryEvent> events)
    {
        for (var i = 0; i < events.Length; i++)
        {
            _cache.Evict(events[i].VehicleId);
        }
    }

    /// <summary>Clones cached state for every vehicle of a new event; returns the vehicles that must be loaded from the DB.</summary>
    private string[]? CloneCachedStates(ReadOnlySpan<TelemetryEvent> events, HashSet<Guid> newIds, Dictionary<string, VehicleState> touched)
    {
        List<string>? misses = null;
        HashSet<string>? missSet = null;
        for (var i = 0; i < events.Length; i++)
        {
            ref readonly var e = ref events[i];
            if (!newIds.Contains(e.EventId) || touched.ContainsKey(e.VehicleId))
            {
                continue;
            }

            if (_cache.TryGet(e.VehicleId, out var cached))
            {
                touched[e.VehicleId] = cached.Clone();
            }
            else if ((missSet ??= new HashSet<string>(StringComparer.Ordinal)).Add(e.VehicleId))
            {
                (misses ??= new List<string>()).Add(e.VehicleId);
            }
        }

        return misses?.ToArray();
    }

    private static void ApplyRules(ReadOnlySpan<TelemetryEvent> events, HashSet<Guid> newIds, Dictionary<string, VehicleState> touched,
        BatchOutcome outcome)
    {
        HashSet<Guid>? applied = null;
        for (var i = 0; i < events.Length; i++)
        {
            ref readonly var e = ref events[i];
            if (!newIds.Contains(e.EventId) || !(applied ??= new HashSet<Guid>(newIds.Count)).Add(e.EventId))
            {
                continue; // duplicate (already in the DB, or repeated inside this batch)
            }

            var state = touched[e.VehicleId];
            var result = TelemetryRules.Apply(state, in e);
            if (result.OutOfOrder)
            {
                (outcome.OutOfOrderIds ??= new List<Guid>()).Add(e.EventId);
                continue;
            }

            if (result.GpsJump)
            {
                outcome.GpsJumps++;
            }

            if (result.SpeedingRaised)
            {
                outcome.Alerts.Add(new SpeedingAlertMessage(e.EventId, e.VehicleId, SpeedingAlertType, e.TimestampUtc, e.Seq, e.Lat, e.Lon,
                    e.SpeedKmh));
            }
        }
    }

    private sealed class BatchOutcome
    {
        public BatchOutcome(int inserted, int duplicates)
        {
            Inserted = inserted;
            Duplicates = duplicates;
        }

        public int Inserted { get; }
        public int Duplicates { get; }
        public int GpsJumps { get; set; }
        public List<Guid>? OutOfOrderIds { get; set; }
        public List<SpeedingAlertMessage> Alerts { get; } = new();
    }
}
