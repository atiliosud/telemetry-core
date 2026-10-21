using System.Globalization;

namespace TelemetryCore.LoadGenerator;

public sealed class GeneratorOptions
{
    public long Total { get; init; } = 1_000_000;
    public int Vehicles { get; init; } = 1000;
    public int Rate { get; init; }
    public double DuplicatePercent { get; init; } = 2.0;
    public double OutOfOrderPercent { get; init; } = 1.0;
    public double InvalidPercent { get; init; } = 0.5;
    public double SpeedingVehiclesPercent { get; init; } = 5.0;
    public int Seed { get; init; } = 42;
    public string Bootstrap { get; init; } = "localhost:29092";
    public string Topic { get; init; } = "telemetry.raw";
    /// <summary>Vehicle ids are <c>{prefix}{n:D6}</c>. A fresh prefix per run gives fresh vehicles (seq restarts at 1 without clashing with stored state).</summary>
    public string VehiclePrefix { get; init; } = "VH-";

    public static GeneratorOptions Parse(string[] args)
    {
        var total = 1_000_000L;
        var vehicles = 1000;
        var rate = 0;
        var dup = 2.0;
        var ooo = 1.0;
        var inv = 0.5;
        var speeding = 5.0;
        var seed = 42;
        var bootstrap = "localhost:29092";
        var topic = "telemetry.raw";
        var vehiclePrefix = "VH-";

        for (var i = 0; i < args.Length; i++)
        {
            var flag = args[i];
            if (!flag.StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Unexpected argument '{flag}'. Flags look like --total 1000.");
            if (i + 1 >= args.Length)
                throw new ArgumentException($"Flag '{flag}' needs a value.");
            var value = args[++i];

            switch (flag)
            {
                case "--total": total = ParseLong(flag, value, 1); break;
                case "--vehicles": vehicles = (int)ParseLong(flag, value, 1, int.MaxValue); break;
                case "--rate": rate = (int)ParseLong(flag, value, 0, int.MaxValue); break;
                case "--duplicates": dup = ParsePercent(flag, value); break;
                case "--out-of-order": ooo = ParsePercent(flag, value); break;
                case "--invalid": inv = ParsePercent(flag, value); break;
                case "--speeding-vehicles": speeding = ParsePercent(flag, value); break;
                case "--seed": seed = (int)ParseLong(flag, value, int.MinValue, int.MaxValue); break;
                case "--bootstrap":
                    if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("--bootstrap must not be empty.");
                    bootstrap = value;
                    break;
                case "--topic":
                    if (string.IsNullOrWhiteSpace(value)) throw new ArgumentException("--topic must not be empty.");
                    topic = value;
                    break;
                case "--vehicle-prefix":
                    if (value.Length is 0 or > 32 || !value.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_'))
                        throw new ArgumentException("--vehicle-prefix expects 1-32 characters [A-Za-z0-9_-].");
                    vehiclePrefix = value;
                    break;
                default:
                    throw new ArgumentException($"Unknown flag '{flag}'.");
            }
        }

        return new GeneratorOptions
        {
            Total = total,
            Vehicles = vehicles,
            Rate = rate,
            DuplicatePercent = dup,
            OutOfOrderPercent = ooo,
            InvalidPercent = inv,
            SpeedingVehiclesPercent = speeding,
            Seed = seed,
            Bootstrap = bootstrap,
            Topic = topic,
            VehiclePrefix = vehiclePrefix,
        };
    }

    private static long ParseLong(string flag, string value, long min, long max = long.MaxValue)
    {
        if (!long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) || n < min || n > max)
            throw new ArgumentException($"Flag '{flag}' expects an integer >= {min}, got '{value}'.");
        return n;
    }

    private static double ParsePercent(string flag, string value)
    {
        var text = value.EndsWith('%') ? value[..^1] : value;
        if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var p)
            || double.IsNaN(p) || p < 0 || p > 100)
            throw new ArgumentException($"Flag '{flag}' expects a percent between 0 and 100 (e.g. 2% or 2), got '{value}'.");
        return p;
    }
}
