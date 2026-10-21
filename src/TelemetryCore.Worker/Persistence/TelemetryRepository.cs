using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;

namespace TelemetryCore.Worker.Persistence;

/// <summary>
/// Set-based SQL for one batch. Every statement sends whole columns as arrays (<c>unnest</c>), so a batch costs a
/// fixed number of round trips regardless of its size. Plain Npgsql with typed parameters (no per-row boxing).
/// </summary>
public sealed class TelemetryRepository
{
    private const string InsertEventsSql = """
        INSERT INTO processed_events (event_id, vehicle_id, seq, ts_utc, lat, lon, speed_kmh)
        SELECT * FROM unnest(@ids::uuid[], @vehicles::text[], @seqs::bigint[], @ts::timestamptz[],
                             @lats::double precision[], @lons::double precision[], @speeds::real[])
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
        """;

    private const string LoadStatesSql = """
        SELECT vehicle_id, last_seq, last_event_utc, lat, lon, speed_kmh, ignition, distance_km_today, speeding,
               overspeed_streak, distance_date
        FROM vehicle_state
        WHERE vehicle_id = ANY(@ids)
        """;

    private const string MarkOutOfOrderSql = "UPDATE processed_events SET out_of_order = true WHERE event_id = ANY(@ids)";

    // The WHERE guard stops a late (zombie) processor from moving a vehicle back to an older seq after a rebalance.
    private const string UpsertStatesSql = """
        INSERT INTO vehicle_state (vehicle_id, last_seq, last_event_utc, lat, lon, speed_kmh, ignition, distance_km_today,
                                   speeding, updated_utc, overspeed_streak, distance_date)
        SELECT u.vehicle_id, u.last_seq, u.last_event_utc, u.lat, u.lon, u.speed_kmh, u.ignition, u.distance_km_today,
               u.speeding, now(), u.overspeed_streak, u.distance_date
        FROM unnest(@vehicles::text[], @seqs::bigint[], @ts::timestamptz[], @lats::double precision[], @lons::double precision[],
                    @speeds::real[], @ignitions::boolean[], @distances::double precision[], @speeding::boolean[],
                    @streaks::integer[], @dates::date[])
             AS u(vehicle_id, last_seq, last_event_utc, lat, lon, speed_kmh, ignition, distance_km_today, speeding,
                  overspeed_streak, distance_date)
        ON CONFLICT (vehicle_id) DO UPDATE SET
            last_seq = EXCLUDED.last_seq,
            last_event_utc = EXCLUDED.last_event_utc,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            speed_kmh = EXCLUDED.speed_kmh,
            ignition = EXCLUDED.ignition,
            distance_km_today = EXCLUDED.distance_km_today,
            speeding = EXCLUDED.speeding,
            updated_utc = EXCLUDED.updated_utc,
            overspeed_streak = EXCLUDED.overspeed_streak,
            distance_date = EXCLUDED.distance_date
        WHERE vehicle_state.last_seq IS NULL OR EXCLUDED.last_seq >= vehicle_state.last_seq
        """;

    // Outbox rows only for alerts that were really inserted, so a replay can never enqueue a second message (D8).
    private const string InsertAlertsSql = """
        WITH inserted AS (
            INSERT INTO alerts (alert_id, vehicle_id, type, raised_utc, payload)
            SELECT * FROM unnest(@ids::uuid[], @vehicles::text[], @types::text[], @raised::timestamptz[], @payloads::jsonb[])
            ON CONFLICT (alert_id) DO NOTHING
            RETURNING vehicle_id, payload
        )
        INSERT INTO outbox (topic, key, payload)
        SELECT @topic, vehicle_id, payload FROM inserted
        """;

    /// <summary>Inserts the batch; returns the ids that were new. Repeated ids inside the batch are sent once.</summary>
    public async Task<HashSet<Guid>> InsertEventsAsync(NpgsqlConnection conn, NpgsqlTransaction tx, ReadOnlyMemory<TelemetryEvent> events,
        CancellationToken ct)
    {
        var columns = EventColumns.From(events.Span);
        await using var cmd = new NpgsqlCommand(InsertEventsSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("ids", columns.Ids));
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("vehicles", columns.Vehicles));
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("seqs", columns.Seqs));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset[]>("ts", columns.Timestamps));
        cmd.Parameters.Add(new NpgsqlParameter<double[]>("lats", columns.Lats));
        cmd.Parameters.Add(new NpgsqlParameter<double[]>("lons", columns.Lons));
        cmd.Parameters.Add(new NpgsqlParameter<float[]>("speeds", columns.Speeds));

        var inserted = new HashSet<Guid>(columns.Ids.Length);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            inserted.Add(reader.GetGuid(0));
        }

        return inserted;
    }

    public async Task<List<VehicleState>> LoadStatesAsync(NpgsqlConnection conn, NpgsqlTransaction tx, string[] vehicleIds,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(LoadStatesSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("ids", vehicleIds));
        var states = new List<VehicleState>(vehicleIds.Length);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            states.Add(new VehicleState
            {
                VehicleId = reader.GetString(0),
                LastSeq = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                LastEventUtc = reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2),
                Lat = reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                Lon = reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                SpeedKmh = reader.IsDBNull(5) ? 0 : reader.GetFloat(5),
                Ignition = !reader.IsDBNull(6) && reader.GetBoolean(6),
                DistanceKmToday = reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                Speeding = !reader.IsDBNull(8) && reader.GetBoolean(8),
                OverspeedStreak = reader.GetInt32(9),
                DistanceDate = reader.IsDBNull(10) ? null : reader.GetFieldValue<DateOnly>(10),
            });
        }

        return states;
    }

    public async Task MarkOutOfOrderAsync(NpgsqlConnection conn, NpgsqlTransaction tx, Guid[] ids, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(MarkOutOfOrderSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("ids", ids));
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task UpsertStatesAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyCollection<VehicleState> states,
        CancellationToken ct)
    {
        var n = states.Count;
        var vehicles = new string[n];
        var seqs = new long[n];
        var ts = new DateTimeOffset?[n];
        var lats = new double[n];
        var lons = new double[n];
        var speeds = new float[n];
        var ignitions = new bool[n];
        var distances = new double[n];
        var speeding = new bool[n];
        var streaks = new int[n];
        var dates = new DateOnly?[n];
        var i = 0;
        foreach (var s in states)
        {
            vehicles[i] = s.VehicleId;
            seqs[i] = s.LastSeq;
            ts[i] = s.LastEventUtc;
            lats[i] = s.Lat;
            lons[i] = s.Lon;
            speeds[i] = (float)s.SpeedKmh;
            ignitions[i] = s.Ignition;
            distances[i] = s.DistanceKmToday;
            speeding[i] = s.Speeding;
            streaks[i] = s.OverspeedStreak;
            dates[i] = s.DistanceDate;
            i++;
        }

        await using var cmd = new NpgsqlCommand(UpsertStatesSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("vehicles", vehicles));
        cmd.Parameters.Add(new NpgsqlParameter<long[]>("seqs", seqs));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset?[]>("ts", ts));
        cmd.Parameters.Add(new NpgsqlParameter<double[]>("lats", lats));
        cmd.Parameters.Add(new NpgsqlParameter<double[]>("lons", lons));
        cmd.Parameters.Add(new NpgsqlParameter<float[]>("speeds", speeds));
        cmd.Parameters.Add(new NpgsqlParameter<bool[]>("ignitions", ignitions));
        cmd.Parameters.Add(new NpgsqlParameter<double[]>("distances", distances));
        cmd.Parameters.Add(new NpgsqlParameter<bool[]>("speeding", speeding));
        cmd.Parameters.Add(new NpgsqlParameter<int[]>("streaks", streaks));
        cmd.Parameters.Add(new NpgsqlParameter<DateOnly?[]>("dates", dates));
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task InsertAlertsAndOutboxAsync(NpgsqlConnection conn, NpgsqlTransaction tx, IReadOnlyList<SpeedingAlertMessage> alerts,
        CancellationToken ct)
    {
        var n = alerts.Count;
        if (n == 0)
        {
            return;
        }

        var ids = new Guid[n];
        var vehicles = new string[n];
        var types = new string[n];
        var raised = new DateTimeOffset[n];
        var payloads = new string[n];
        for (var i = 0; i < n; i++)
        {
            var a = alerts[i];
            ids[i] = a.AlertId;
            vehicles[i] = a.VehicleId;
            types[i] = a.Type;
            raised[i] = a.RaisedUtc;
            payloads[i] = JsonSerializer.Serialize(a, TelemetryJsonContext.Default.SpeedingAlertMessage);
        }

        await using var cmd = new NpgsqlCommand(InsertAlertsSql, conn, tx);
        cmd.Parameters.Add(new NpgsqlParameter<Guid[]>("ids", ids));
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("vehicles", vehicles));
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("types", types));
        cmd.Parameters.Add(new NpgsqlParameter<DateTimeOffset[]>("raised", raised));
        cmd.Parameters.Add(new NpgsqlParameter<string[]>("payloads", payloads) { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Jsonb });
        cmd.Parameters.Add(new NpgsqlParameter<string>("topic", Topics.Alerts));
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private readonly struct EventColumns
    {
        public Guid[] Ids { get; private init; }
        public string[] Vehicles { get; private init; }
        public long[] Seqs { get; private init; }
        public DateTimeOffset[] Timestamps { get; private init; }
        public double[] Lats { get; private init; }
        public double[] Lons { get; private init; }
        public float[] Speeds { get; private init; }

        public static EventColumns From(ReadOnlySpan<TelemetryEvent> events)
        {
            // Drop repeated ids inside the batch: the first occurrence wins, like the rules loop.
            var seen = new HashSet<Guid>(events.Length);
            var unique = 0;
            for (var i = 0; i < events.Length; i++)
            {
                if (seen.Add(events[i].EventId))
                {
                    unique++;
                }
            }

            var c = new EventColumns
            {
                Ids = new Guid[unique],
                Vehicles = new string[unique],
                Seqs = new long[unique],
                Timestamps = new DateTimeOffset[unique],
                Lats = new double[unique],
                Lons = new double[unique],
                Speeds = new float[unique],
            };
            seen.Clear();
            var j = 0;
            for (var i = 0; i < events.Length; i++)
            {
                ref readonly var e = ref events[i];
                if (!seen.Add(e.EventId))
                {
                    continue;
                }

                c.Ids[j] = e.EventId;
                c.Vehicles[j] = e.VehicleId;
                c.Seqs[j] = e.Seq;
                c.Timestamps[j] = e.TimestampUtc.ToUniversalTime();
                c.Lats[j] = e.Lat;
                c.Lons[j] = e.Lon;
                c.Speeds[j] = (float)e.SpeedKmh;
                j++;
            }

            return c;
        }
    }
}
