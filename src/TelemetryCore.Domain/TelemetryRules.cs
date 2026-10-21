namespace TelemetryCore.Domain;

/// <summary>R3-R5: O(1) per event, no allocations.</summary>
public static class TelemetryRules
{
    public const double SpeedingKmh = 120;
    public const double ClearKmh = 110;
    public const int StreakToRaise = 3;
    public const double GpsJumpKm = 5;
    public static readonly TimeSpan GpsJumpWindow = TimeSpan.FromSeconds(10);

    public static RuleOutcome Apply(VehicleState state, in TelemetryEvent evt)
    {
        if (state.LastEventUtc is not null && evt.Seq <= state.LastSeq)
        {
            return new RuleOutcome(true, false, false, 0);
        }

        var eventDate = DateOnly.FromDateTime(evt.TimestampUtc.UtcDateTime);
        if (state.DistanceDate != eventDate)
        {
            state.DistanceKmToday = 0;
            state.DistanceDate = eventDate;
        }

        var gpsJump = false;
        var distanceAdded = 0.0;
        if (state.LastEventUtc is { } lastUtc)
        {
            var km = Geo.HaversineKm(state.Lat, state.Lon, evt.Lat, evt.Lon);
            if (km > GpsJumpKm && evt.TimestampUtc - lastUtc < GpsJumpWindow)
            {
                gpsJump = true;
            }
            else
            {
                distanceAdded = km;
                state.DistanceKmToday += km;
            }
        }

        var raised = false;
        if (evt.SpeedKmh > SpeedingKmh)
        {
            state.OverspeedStreak++;
            if (state.OverspeedStreak >= StreakToRaise && !state.Speeding)
            {
                state.Speeding = true;
                raised = true;
            }
        }
        else
        {
            state.OverspeedStreak = 0;
            if (evt.SpeedKmh < ClearKmh)
            {
                state.Speeding = false;
            }
        }

        state.Lat = evt.Lat;
        state.Lon = evt.Lon;
        state.SpeedKmh = evt.SpeedKmh;
        state.Ignition = evt.Ignition;
        state.LastSeq = evt.Seq;
        state.LastEventUtc = evt.TimestampUtc;

        return new RuleOutcome(false, gpsJump, raised, distanceAdded);
    }
}
