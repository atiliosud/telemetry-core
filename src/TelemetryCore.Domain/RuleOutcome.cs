namespace TelemetryCore.Domain;

public readonly record struct RuleOutcome(bool OutOfOrder, bool GpsJump, bool SpeedingRaised, double DistanceAddedKm);
