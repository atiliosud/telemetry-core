using System.Text.Json;
using TelemetryCore.Contracts;

namespace TelemetryCore.LoadGenerator;

/// <summary>Simulates vehicle movement around Madrid (D16). Deterministic for a given Random.</summary>
public sealed class FleetSimulator
{
    private const double MadridLat = 40.4168;
    private const double MadridLon = -3.7038;
    private const double MetersPerDegLat = 111_320.0;

    private sealed class Vehicle
    {
        public required string Id;
        public double Lat, Lon, HeadingRad, BaseSpeed;
        public long Seq;
        public bool Speeding;
        public int Counter;
    }

    private readonly Vehicle[] _vehicles;
    private readonly Random _rng;

    public FleetSimulator(int vehicles, int speedingVehicles, Random rng, string vehiclePrefix = "VH-")
    {
        _rng = rng;
        _vehicles = new Vehicle[vehicles];
        for (var i = 0; i < vehicles; i++)
        {
            var r = 20_000.0 * Math.Sqrt(rng.NextDouble());
            var a = rng.NextDouble() * 2 * Math.PI;
            _vehicles[i] = new Vehicle
            {
                Id = $"{vehiclePrefix}{i + 1:D6}",
                Lat = MadridLat + r * Math.Cos(a) / MetersPerDegLat,
                Lon = MadridLon + r * Math.Sin(a) / (MetersPerDegLat * Math.Cos(MadridLat * Math.PI / 180)),
                HeadingRad = rng.NextDouble() * 2 * Math.PI,
                BaseSpeed = 20 + rng.NextDouble() * 90,
                Speeding = i < speedingVehicles,
                Seq = 1,
            };
        }
    }

    public int Count => _vehicles.Length;

    public int PickIndex() => _rng.Next(_vehicles.Length);

    public string IdOf(int index) => _vehicles[index].Id;

    /// <summary>Next in-order event for the vehicle: advances seq and position by speed x 1 s.</summary>
    public TelemetryEventDto Next(int index, DateTimeOffset now)
    {
        var v = _vehicles[index];
        double speed;
        if (v.Speeding)
        {
            var high = (v.Counter / 10) % 2 == 0;
            speed = high ? 125 + _rng.NextDouble() * 15 : 80 + _rng.NextDouble() * 20;
        }
        else
        {
            v.BaseSpeed = Math.Clamp(v.BaseSpeed + (_rng.NextDouble() - 0.5) * 6, 20, 110);
            speed = v.BaseSpeed;
        }
        v.Counter++;

        v.HeadingRad += (_rng.NextDouble() - 0.5) * 0.3;
        var meters = speed / 3.6;
        v.Lat += meters * Math.Cos(v.HeadingRad) / MetersPerDegLat;
        v.Lon += meters * Math.Sin(v.HeadingRad) / (MetersPerDegLat * Math.Cos(v.Lat * Math.PI / 180));

        return new TelemetryEventDto
        {
            EventId = NewGuid(),
            VehicleId = v.Id,
            Seq = v.Seq++,
            TimestampUtc = now,
            Lat = Math.Round(v.Lat, 6),
            Lon = Math.Round(v.Lon, 6),
            SpeedKmh = Math.Round(speed, 1),
            Ignition = true,
        };
    }

    /// <summary>A well-formed event not tied to vehicle state (base for invalid ones; does not advance the vehicle).</summary>
    public TelemetryEventDto Scratch(int index, DateTimeOffset now)
    {
        var v = _vehicles[index];
        return new TelemetryEventDto
        {
            EventId = NewGuid(),
            VehicleId = v.Id,
            Seq = v.Seq,
            TimestampUtc = now,
            Lat = Math.Round(v.Lat, 6),
            Lon = Math.Round(v.Lon, 6),
            SpeedKmh = 50.0,
            Ignition = true,
        };
    }

    public Guid NewGuid()
    {
        Span<byte> b = stackalloc byte[16];
        _rng.NextBytes(b);
        return new Guid(b);
    }

    public static byte[] Serialize(TelemetryEventDto dto) =>
        JsonSerializer.SerializeToUtf8Bytes(dto, TelemetryJsonContext.Default.TelemetryEventDto);
}
