namespace TelemetryCore.Domain;

public static class DlqReasons
{
    public const string PayloadTooLarge = "payload_too_large";
    public const string EmptyPayload = "empty_payload";
    public const string InvalidJson = "invalid_json";
    public const string MissingField = "missing_field";
    public const string LatOutOfRange = "lat_out_of_range";
    public const string LonOutOfRange = "lon_out_of_range";
    public const string SpeedOutOfRange = "speed_out_of_range";
    public const string TimestampInFuture = "timestamp_in_future";
    public const string TimestampTooOld = "timestamp_too_old";
}
