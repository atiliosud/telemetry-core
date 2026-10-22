using System.Globalization;
using TelemetryCore.Contracts;

namespace TelemetryCore.ReadApi;

public sealed record AlertsQuery(string? VehicleId, DateTime? From, DateTime? To, int Page, int PageSize);

public static partial class QueryValidation
{
    public const int DefaultPageSize = 50;
    public const int MaxPageSize = 100;
    public const int MaxPage = 10_000;
    public static readonly TimeSpan MaxRange = TimeSpan.FromDays(31);

    private static readonly string[] IsoFormats =
    {
        "yyyy-MM-dd", "yyyy-MM-ddTHH:mmK", "yyyy-MM-ddTHH:mm:ssK", "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
    };

    public static bool IsValidVehicleId(string? vehicleId) => VehicleIds.IsValid(vehicleId);

    public static bool TryParseAlerts(IQueryCollection query, out AlertsQuery? result, out Dictionary<string, string[]> errors)
    {
        errors = new Dictionary<string, string[]>();
        result = null;

        string? vehicleId = null;
        if (query.TryGetValue("vehicleId", out var v))
        {
            vehicleId = v.ToString();
            if (!IsValidVehicleId(vehicleId))
            {
                errors["vehicleId"] = new[] { $"vehicleId must match {VehicleIds.Pattern}." };
            }
        }

        var from = ParseTimestamp(query, "from", errors);
        var to = ParseTimestamp(query, "to", errors);
        if (from is not null && to is not null)
        {
            if (from > to)
            {
                errors["from"] = new[] { "from must be less than or equal to to." };
            }
            else if (to - from > MaxRange)
            {
                errors["to"] = new[] { "The range between from and to must not exceed 31 days." };
            }
        }

        var page = ParseInt(query, "page", 1, 1, MaxPage, errors);
        var pageSize = ParseInt(query, "pageSize", DefaultPageSize, 1, MaxPageSize, errors);

        if (errors.Count > 0)
        {
            return false;
        }

        result = new AlertsQuery(vehicleId, from, to, page, pageSize);
        return true;
    }

    private static DateTime? ParseTimestamp(IQueryCollection query, string name, Dictionary<string, string[]> errors)
    {
        if (!query.TryGetValue(name, out var raw))
        {
            return null;
        }

        var text = raw.ToString();
        if (text.Length <= 40 && DateTimeOffset.TryParseExact(text, IsoFormats, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
        {
            return parsed.UtcDateTime;
        }

        errors[name] = new[] { $"{name} must be an ISO-8601 date or timestamp." };
        return null;
    }

    private static int ParseInt(IQueryCollection query, string name, int fallback, int min, int max, Dictionary<string, string[]> errors)
    {
        if (!query.TryGetValue(name, out var raw))
        {
            return fallback;
        }

        if (int.TryParse(raw.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value >= min && value <= max)
        {
            return value;
        }

        errors[name] = new[] { $"{name} must be an integer between {min} and {max}." };
        return fallback;
    }
}
