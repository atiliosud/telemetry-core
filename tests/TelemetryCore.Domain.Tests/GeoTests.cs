using FluentAssertions;
using TelemetryCore.Domain;

namespace TelemetryCore.Domain.Tests;

public class GeoTests
{
    [Fact]
    public void Haversine_MadridToBarcelona_IsAbout505Km()
    {
        var km = Geo.HaversineKm(40.4168, -3.7038, 41.3874, 2.1686);
        km.Should().BeInRange(504, 506);
    }

    [Fact]
    public void Haversine_SamePoint_IsZero()
    {
        Geo.HaversineKm(10.5, 20.5, 10.5, 20.5).Should().Be(0);
    }
}
