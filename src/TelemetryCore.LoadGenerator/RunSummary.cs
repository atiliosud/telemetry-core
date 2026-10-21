namespace TelemetryCore.LoadGenerator;

public sealed record RunSummary(
    long Sent,
    long DuplicatesInjected,
    long InvalidInjected,
    long OutOfOrderInjected,
    long ExpectedUnique,
    long VehiclesSpeeding,
    double Seconds,
    double EventsPerSecond);
