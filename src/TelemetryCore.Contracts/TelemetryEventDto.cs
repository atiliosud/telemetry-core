namespace TelemetryCore.Contracts;

/// <summary>Wire format of a telemetry event. Every property is nullable so missing fields can be detected.</summary>
public sealed class TelemetryEventDto
{
    public Guid? EventId { get; set; }
    public string? VehicleId { get; set; }
    public long? Seq { get; set; }
    public DateTimeOffset? TimestampUtc { get; set; }
    public double? Lat { get; set; }
    public double? Lon { get; set; }
    public double? SpeedKmh { get; set; }
    public bool? Ignition { get; set; }
}
