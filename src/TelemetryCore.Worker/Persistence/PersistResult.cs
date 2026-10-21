using TelemetryCore.Contracts;

namespace TelemetryCore.Worker.Persistence;

/// <summary>Outcome of one batch transaction. Counts feed the metrics added later.</summary>
public sealed record PersistResult(
    int Inserted,
    int Duplicates,
    int OutOfOrder,
    int GpsJumps,
    IReadOnlyList<SpeedingAlertMessage> Alerts);
