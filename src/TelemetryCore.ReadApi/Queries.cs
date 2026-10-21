using System.Text;
using System.Text.Json;
using Dapper;
using Npgsql;

namespace TelemetryCore.ReadApi;

public sealed record VehicleStateResponse(
    string VehicleId, long? LastSeq, DateTimeOffset? LastEventUtc, double? Lat, double? Lon, float? SpeedKmh,
    bool? Ignition, double? DistanceKmToday, bool? Speeding, DateTimeOffset UpdatedUtc);

public sealed record AlertItem(Guid AlertId, string VehicleId, string Type, DateTimeOffset RaisedUtc, JsonElement Payload);

public sealed record AlertsPage(IReadOnlyList<AlertItem> Items, int Page, int PageSize, bool HasMore);

/// <summary>Read-side queries. SQL text is constant; every user value travels as a parameter.</summary>
public sealed class Queries(NpgsqlDataSource dataSource)
{
    private const int CommandTimeoutSeconds = 5;

    static Queries()
    {
        DefaultTypeMap.MatchNamesWithUnderscores = true;
    }

    public async Task<VehicleStateResponse?> GetStateAsync(string vehicleId, CancellationToken ct)
    {
        const string sql =
            "SELECT vehicle_id, last_seq, last_event_utc, lat, lon, speed_kmh, ignition, distance_km_today, speeding, updated_utc " +
            "FROM vehicle_state WHERE vehicle_id = @vehicleId";
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var row = await conn.QuerySingleOrDefaultAsync<StateRow>(
            new CommandDefinition(sql, new { vehicleId }, commandTimeout: CommandTimeoutSeconds, cancellationToken: ct));
        return row is null
            ? null
            : new VehicleStateResponse(row.VehicleId, row.LastSeq, Utc(row.LastEventUtc), row.Lat, row.Lon, row.SpeedKmh,
                row.Ignition, row.DistanceKmToday, row.Speeding, Utc(row.UpdatedUtc)!.Value);
    }

    public async Task<AlertsPage> GetAlertsAsync(AlertsQuery q, CancellationToken ct)
    {
        var sql = new StringBuilder("SELECT alert_id, vehicle_id, type, raised_utc, payload::text AS payload FROM alerts WHERE 1 = 1");
        var args = new DynamicParameters();
        if (q.VehicleId is not null)
        {
            sql.Append(" AND vehicle_id = @vehicleId");
            args.Add("vehicleId", q.VehicleId);
        }

        if (q.From is not null)
        {
            sql.Append(" AND raised_utc >= @from");
            args.Add("from", DateTime.SpecifyKind(q.From.Value, DateTimeKind.Utc));
        }

        if (q.To is not null)
        {
            sql.Append(" AND raised_utc <= @to");
            args.Add("to", DateTime.SpecifyKind(q.To.Value, DateTimeKind.Utc));
        }

        sql.Append(" ORDER BY raised_utc DESC, alert_id LIMIT @limit OFFSET @offset");
        args.Add("limit", q.PageSize + 1);
        args.Add("offset", (q.Page - 1) * q.PageSize);

        await using var conn = await dataSource.OpenConnectionAsync(ct);
        var rows = (await conn.QueryAsync<AlertRow>(
            new CommandDefinition(sql.ToString(), args, commandTimeout: CommandTimeoutSeconds, cancellationToken: ct))).ToList();

        var hasMore = rows.Count > q.PageSize;
        var items = rows.Take(q.PageSize)
            .Select(r => new AlertItem(r.AlertId, r.VehicleId, r.Type, Utc(r.RaisedUtc)!.Value, JsonDocument.Parse(r.Payload).RootElement.Clone()))
            .ToList();
        return new AlertsPage(items, q.Page, q.PageSize, hasMore);
    }

    public async Task<bool> PingAsync(CancellationToken ct)
    {
        await using var conn = await dataSource.OpenConnectionAsync(ct);
        return await conn.ExecuteScalarAsync<int>(new CommandDefinition("SELECT 1", commandTimeout: CommandTimeoutSeconds, cancellationToken: ct)) == 1;
    }

    private static DateTimeOffset? Utc(DateTime? value) =>
        value is null ? null : new DateTimeOffset(DateTime.SpecifyKind(value.Value, DateTimeKind.Utc));

    private sealed class StateRow
    {
        public string VehicleId { get; set; } = string.Empty;
        public long? LastSeq { get; set; }
        public DateTime? LastEventUtc { get; set; }
        public double? Lat { get; set; }
        public double? Lon { get; set; }
        public float? SpeedKmh { get; set; }
        public bool? Ignition { get; set; }
        public double? DistanceKmToday { get; set; }
        public bool? Speeding { get; set; }
        public DateTime UpdatedUtc { get; set; }
    }

    private sealed class AlertRow
    {
        public Guid AlertId { get; set; }
        public string VehicleId { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public DateTime RaisedUtc { get; set; }
        public string Payload { get; set; } = "{}";
    }
}
