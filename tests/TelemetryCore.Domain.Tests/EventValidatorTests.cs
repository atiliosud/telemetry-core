using System.Text;
using FluentAssertions;
using TelemetryCore.Domain;

namespace TelemetryCore.Domain.Tests;

public class EventValidatorTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 22, 12, 0, 0, TimeSpan.Zero);

    private static string Json(
        string? eventId = "8b6f0c1e-2c1a-4b7e-9a51-1f2d3c4b5a6e",
        string? vehicleId = "VH-000123",
        string? seq = "4182",
        string? ts = null,
        string lat = "40.4168",
        string lon = "-3.7038",
        string speed = "87.5")
    {
        ts ??= Ts(Now);
        var parts = new List<string>();
        if (eventId != null) parts.Add($"\"eventId\":\"{eventId}\"");
        if (vehicleId != null) parts.Add($"\"vehicleId\":\"{vehicleId}\"");
        if (seq != null) parts.Add($"\"seq\":{seq}");
        if (ts != "") parts.Add($"\"timestampUtc\":{ts}");
        parts.Add($"\"lat\":{lat}");
        parts.Add($"\"lon\":{lon}");
        parts.Add($"\"speedKmh\":{speed}");
        parts.Add("\"ignition\":true");
        return "{" + string.Join(",", parts) + "}";
    }

    private static (bool ok, string? reason, TelemetryEvent evt) Run(string json) => Run(Encoding.UTF8.GetBytes(json));

    private static (bool ok, string? reason, TelemetryEvent evt) Run(byte[] bytes)
    {
        var ok = EventValidator.TryParse(bytes, Now, out var evt, out var reason);
        return (ok, reason, evt);
    }

    private static string Ts(DateTimeOffset t) => "\"" + t.ToString("O") + "\"";

    [Fact]
    public void ValidPayload_ParsesAllFields()
    {
        var (ok, reason, evt) = Run(Json());
        ok.Should().BeTrue();
        reason.Should().BeNull();
        evt.EventId.Should().Be(Guid.Parse("8b6f0c1e-2c1a-4b7e-9a51-1f2d3c4b5a6e"));
        evt.VehicleId.Should().Be("VH-000123");
        evt.Seq.Should().Be(4182);
        evt.TimestampUtc.Should().Be(Now);
        evt.Lat.Should().Be(40.4168);
        evt.Lon.Should().Be(-3.7038);
        evt.SpeedKmh.Should().Be(87.5);
        evt.Ignition.Should().BeTrue();
    }

    [Fact]
    public void PayloadOf4097Bytes_IsTooLarge()
    {
        var (ok, reason, _) = Run(new byte[4097]);
        ok.Should().BeFalse();
        reason.Should().Be(DlqReasons.PayloadTooLarge);
    }

    [Fact]
    public void Empty_IsEmptyPayload()
    {
        var (ok, reason, _) = Run(Array.Empty<byte>());
        ok.Should().BeFalse();
        reason.Should().Be(DlqReasons.EmptyPayload);
    }

    [Fact]
    public void NotJson_IsInvalidJson()
    {
        var (ok, reason, _) = Run("not json");
        ok.Should().BeFalse();
        reason.Should().Be(DlqReasons.InvalidJson);
    }

    [Fact]
    public void LatAsString_IsInvalidJson()
    {
        var (ok, reason, _) = Run(Json(lat: "\"abc\""));
        ok.Should().BeFalse();
        reason.Should().Be(DlqReasons.InvalidJson);
    }

    [Theory]
    [InlineData("vehicleId")]
    [InlineData("eventId")]
    [InlineData("seq")]
    [InlineData("timestampUtc")]
    public void MissingRequiredField_IsMissingField(string field)
    {
        var json = field switch
        {
            "vehicleId" => Json(vehicleId: null),
            "eventId" => Json(eventId: null),
            "seq" => Json(seq: null),
            _ => Json(ts: ""),
        };
        var (ok, reason, _) = Run(json);
        ok.Should().BeFalse();
        reason.Should().Be(DlqReasons.MissingField);
    }

    [Theory]
    [InlineData("90.0001", "0", "0", DlqReasons.LatOutOfRange)]
    [InlineData("-90.0001", "0", "0", DlqReasons.LatOutOfRange)]
    [InlineData("0", "-180.1", "0", DlqReasons.LonOutOfRange)]
    [InlineData("0", "180.1", "0", DlqReasons.LonOutOfRange)]
    [InlineData("0", "0", "-1", DlqReasons.SpeedOutOfRange)]
    [InlineData("0", "0", "300.1", DlqReasons.SpeedOutOfRange)]
    public void OutOfRange_IsRejected(string lat, string lon, string speed, string expected)
    {
        var (ok, reason, _) = Run(Json(lat: lat, lon: lon, speed: speed));
        ok.Should().BeFalse();
        reason.Should().Be(expected);
    }

    [Fact]
    public void TimestampTooFarInFuture_IsRejected()
    {
        var (ok, reason, _) = Run(Json(ts: Ts(Now.AddMinutes(5).AddSeconds(1))));
        ok.Should().BeFalse();
        reason.Should().Be(DlqReasons.TimestampInFuture);
    }

    [Fact]
    public void TimestampTooOld_IsRejected()
    {
        var (ok, reason, _) = Run(Json(ts: Ts(Now.AddHours(-24).AddSeconds(-1))));
        ok.Should().BeFalse();
        reason.Should().Be(DlqReasons.TimestampTooOld);
    }

    [Fact]
    public void RangeCheckOrder_LatBeforeLonBeforeSpeed()
    {
        var (_, reason, _) = Run(Json(lat: "91", lon: "181", speed: "999"));
        reason.Should().Be(DlqReasons.LatOutOfRange);
    }

    [Theory]
    [InlineData("90", "0", "0")]
    [InlineData("-90", "0", "0")]
    [InlineData("0", "-180", "0")]
    [InlineData("0", "180", "0")]
    [InlineData("0", "0", "300")]
    public void Boundaries_AreValid(string lat, string lon, string speed)
    {
        var (ok, reason, _) = Run(Json(lat: lat, lon: lon, speed: speed));
        ok.Should().BeTrue();
        reason.Should().BeNull();
    }

    [Fact]
    public void TimestampBoundaries_AreValid()
    {
        Run(Json(ts: Ts(Now.AddMinutes(5)))).ok.Should().BeTrue();
        Run(Json(ts: Ts(Now.AddHours(-24)))).ok.Should().BeTrue();
    }
}
