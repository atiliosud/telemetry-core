namespace TelemetryCore.Domain;

/// <summary>Mutable per-vehicle state (R6). Cloned before rules run so memory only changes after a commit.</summary>
public sealed class VehicleState
{
    public string VehicleId { get; set; } = string.Empty;
    public int Partition { get; set; }
    public long LastSeq { get; set; }
    public DateTimeOffset? LastEventUtc { get; set; }
    public double Lat { get; set; }
    public double Lon { get; set; }
    public double SpeedKmh { get; set; }
    public bool Ignition { get; set; }
    public double DistanceKmToday { get; set; }
    public DateOnly? DistanceDate { get; set; }
    public bool Speeding { get; set; }
    public int OverspeedStreak { get; set; }

    public static VehicleState New(string vehicleId, int partition) =>
        new() { VehicleId = vehicleId, Partition = partition };

    public VehicleState Clone() => (VehicleState)MemberwiseClone();
}
