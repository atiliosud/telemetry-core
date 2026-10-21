namespace TelemetryCore.Domain;

public readonly record struct TelemetryEvent(
    Guid EventId,
    string VehicleId,
    long Seq,
    DateTimeOffset TimestampUtc,
    double Lat,
    double Lon,
    double SpeedKmh,
    bool Ignition);
