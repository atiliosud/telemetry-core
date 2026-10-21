namespace TelemetryCore.Contracts;

public sealed record SpeedingAlertMessage(
    Guid AlertId,
    string VehicleId,
    string Type,
    DateTimeOffset RaisedUtc,
    long Seq,
    double Lat,
    double Lon,
    double SpeedKmh);
