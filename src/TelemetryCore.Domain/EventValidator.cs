using System.Text.Json;
using TelemetryCore.Contracts;

namespace TelemetryCore.Domain;

/// <summary>R1: parses and validates a raw payload. Never trusts input.</summary>
public static class EventValidator
{
    public const int MaxPayloadBytes = 4096;

    private static readonly TimeSpan MaxFuture = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan MaxAge = TimeSpan.FromHours(24);

    public static bool TryParse(ReadOnlySpan<byte> payload, DateTimeOffset nowUtc, out TelemetryEvent evt, out string? reason)
    {
        evt = default;

        if (payload.Length > MaxPayloadBytes)
        {
            reason = DlqReasons.PayloadTooLarge;
            return false;
        }

        if (payload.IsEmpty)
        {
            reason = DlqReasons.EmptyPayload;
            return false;
        }

        TelemetryEventDto? dto;
        try
        {
            dto = JsonSerializer.Deserialize(payload, TelemetryJsonContext.Default.TelemetryEventDto);
        }
        catch (JsonException)
        {
            reason = DlqReasons.InvalidJson;
            return false;
        }

        if (dto is null)
        {
            reason = DlqReasons.InvalidJson;
            return false;
        }

        if (dto.EventId is null || string.IsNullOrEmpty(dto.VehicleId) || dto.Seq is null || dto.TimestampUtc is null
            || dto.Lat is null || dto.Lon is null || dto.SpeedKmh is null || dto.Ignition is null)
        {
            reason = DlqReasons.MissingField;
            return false;
        }

        var lat = dto.Lat.Value;
        var lon = dto.Lon.Value;
        var speed = dto.SpeedKmh.Value;
        var ts = dto.TimestampUtc.Value.ToUniversalTime();

        if (!(lat >= -90.0 && lat <= 90.0))
        {
            reason = DlqReasons.LatOutOfRange;
            return false;
        }

        if (!(lon >= -180.0 && lon <= 180.0))
        {
            reason = DlqReasons.LonOutOfRange;
            return false;
        }

        if (!(speed >= 0.0 && speed <= 300.0))
        {
            reason = DlqReasons.SpeedOutOfRange;
            return false;
        }

        if (ts > nowUtc + MaxFuture)
        {
            reason = DlqReasons.TimestampInFuture;
            return false;
        }

        if (ts < nowUtc - MaxAge)
        {
            reason = DlqReasons.TimestampTooOld;
            return false;
        }

        evt = new TelemetryEvent(dto.EventId.Value, dto.VehicleId, dto.Seq.Value, ts, lat, lon, speed, dto.Ignition.Value);
        reason = null;
        return true;
    }
}
