using FluentAssertions;
using TelemetryCore.Domain;

namespace TelemetryCore.Domain.Tests;

public class TelemetryRulesTests
{
    private static readonly DateTimeOffset T0 = new(2026, 10, 22, 10, 0, 0, TimeSpan.Zero);

    private static TelemetryEvent Evt(long seq, double speed, DateTimeOffset? ts = null, double lat = 40.0, double lon = -3.0) =>
        new(Guid.NewGuid(), "VH-1", seq, ts ?? T0.AddSeconds(seq * 30), lat, lon, speed, true);

    private static VehicleState NewState() => VehicleState.New("VH-1", 0);

    [Fact]
    public void Speeding_ThreeConsecutiveOver120_RaisesOnce()
    {
        var s = NewState();
        TelemetryRules.Apply(s, Evt(1, 121)).SpeedingRaised.Should().BeFalse();
        TelemetryRules.Apply(s, Evt(2, 121)).SpeedingRaised.Should().BeFalse();
        TelemetryRules.Apply(s, Evt(3, 121)).SpeedingRaised.Should().BeTrue();
        TelemetryRules.Apply(s, Evt(4, 130)).SpeedingRaised.Should().BeFalse();
        s.Speeding.Should().BeTrue();
    }

    [Fact]
    public void Speeding_TwoOverThenUnder_DoesNotRaise()
    {
        var s = NewState();
        foreach (var (seq, speed) in new[] { (1L, 121.0), (2, 121), (3, 100), (4, 121) })
            TelemetryRules.Apply(s, Evt(seq, speed)).SpeedingRaised.Should().BeFalse();
        s.Speeding.Should().BeFalse();
    }

    [Fact]
    public void Speeding_ClearsOnlyBelow110()
    {
        var s = NewState();
        long seq = 0;
        for (var i = 0; i < 3; i++) TelemetryRules.Apply(s, Evt(++seq, 121));
        s.Speeding.Should().BeTrue();

        TelemetryRules.Apply(s, Evt(++seq, 115));
        s.Speeding.Should().BeTrue();

        TelemetryRules.Apply(s, Evt(++seq, 109));
        s.Speeding.Should().BeFalse();
        s.OverspeedStreak.Should().Be(0);

        TelemetryRules.Apply(s, Evt(++seq, 121)).SpeedingRaised.Should().BeFalse();
        TelemetryRules.Apply(s, Evt(++seq, 121)).SpeedingRaised.Should().BeFalse();
        TelemetryRules.Apply(s, Evt(++seq, 121)).SpeedingRaised.Should().BeTrue();
    }

    [Fact]
    public void Exactly120_DoesNotCountAsOver()
    {
        var s = NewState();
        for (long i = 1; i <= 5; i++)
            TelemetryRules.Apply(s, Evt(i, 120)).SpeedingRaised.Should().BeFalse();
        s.OverspeedStreak.Should().Be(0);
        s.Speeding.Should().BeFalse();
    }

    [Fact]
    public void OutOfOrder_SeqEqualOrLower_DoesNotChangeState()
    {
        var s = NewState();
        TelemetryRules.Apply(s, Evt(5, 121));
        TelemetryRules.Apply(s, Evt(6, 121));
        var before = s.Clone();

        foreach (var seq in new long[] { 6, 3 })
        {
            var outcome = TelemetryRules.Apply(s, Evt(seq, 200, lat: 41, lon: 2));
            outcome.OutOfOrder.Should().BeTrue();
            outcome.SpeedingRaised.Should().BeFalse();
            outcome.DistanceAddedKm.Should().Be(0);
            s.Should().BeEquivalentTo(before);
        }
    }

    [Fact]
    public void FirstEvent_NoDistance()
    {
        var s = NewState();
        var o = TelemetryRules.Apply(s, Evt(1, 50));
        o.DistanceAddedKm.Should().Be(0);
        o.OutOfOrder.Should().BeFalse();
        s.DistanceKmToday.Should().Be(0);
        s.LastSeq.Should().Be(1);
        s.LastEventUtc.Should().Be(T0.AddSeconds(30));
        s.Lat.Should().Be(40.0);
        s.DistanceDate.Should().Be(DateOnly.FromDateTime(T0.UtcDateTime));
    }

    [Fact]
    public void Distance_AccumulatesHaversine()
    {
        var s = NewState();
        TelemetryRules.Apply(s, Evt(1, 50, lat: 40.0, lon: -3.0));
        var o1 = TelemetryRules.Apply(s, Evt(2, 50, lat: 40.01, lon: -3.0));
        var o2 = TelemetryRules.Apply(s, Evt(3, 50, lat: 40.02, lon: -3.0));
        var d1 = Geo.HaversineKm(40.0, -3.0, 40.01, -3.0);
        var d2 = Geo.HaversineKm(40.01, -3.0, 40.02, -3.0);
        o1.DistanceAddedKm.Should().BeApproximately(d1, 1e-9);
        o2.DistanceAddedKm.Should().BeApproximately(d2, 1e-9);
        s.DistanceKmToday.Should().BeApproximately(d1 + d2, 1e-9);
    }

    [Fact]
    public void GpsJump_Over5KmUnder10s_IgnoredButPositionUpdated()
    {
        var s = NewState();
        TelemetryRules.Apply(s, Evt(1, 50, T0, 40.0, -3.0));
        var o = TelemetryRules.Apply(s, Evt(2, 50, T0.AddSeconds(5), 41.0, -3.0));
        o.GpsJump.Should().BeTrue();
        o.DistanceAddedKm.Should().Be(0);
        s.DistanceKmToday.Should().Be(0);
        s.Lat.Should().Be(41.0);
        s.Lon.Should().Be(-3.0);
    }

    [Fact]
    public void Over5KmAfter10s_IsNotJump()
    {
        var s = NewState();
        TelemetryRules.Apply(s, Evt(1, 50, T0, 40.0, -3.0));
        var o = TelemetryRules.Apply(s, Evt(2, 50, T0.AddSeconds(10), 41.0, -3.0));
        o.GpsJump.Should().BeFalse();
        o.DistanceAddedKm.Should().BeGreaterThan(100);
        s.DistanceKmToday.Should().Be(o.DistanceAddedKm);
    }

    [Fact]
    public void DistanceResets_OnNewUtcDate()
    {
        var s = NewState();
        var t1 = new DateTimeOffset(2026, 10, 22, 23, 59, 59, TimeSpan.Zero);
        TelemetryRules.Apply(s, Evt(1, 50, t1.AddSeconds(-20), 40.0, -3.0));
        TelemetryRules.Apply(s, Evt(2, 50, t1, 40.01, -3.0));
        s.DistanceKmToday.Should().BeGreaterThan(0);

        var t2 = new DateTimeOffset(2026, 10, 23, 0, 0, 1, TimeSpan.Zero);
        var o = TelemetryRules.Apply(s, Evt(3, 50, t2, 40.02, -3.0));
        var seg = Geo.HaversineKm(40.01, -3.0, 40.02, -3.0);
        o.DistanceAddedKm.Should().BeApproximately(seg, 1e-9);
        s.DistanceKmToday.Should().BeApproximately(seg, 1e-9);
        s.DistanceDate.Should().Be(new DateOnly(2026, 10, 23));
    }
}
