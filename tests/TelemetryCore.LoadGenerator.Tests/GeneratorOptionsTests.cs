using FluentAssertions;
using TelemetryCore.LoadGenerator;

namespace TelemetryCore.LoadGenerator.Tests;

public class GeneratorOptionsTests
{
    [Fact]
    public void Parse_AcceptsPercentWithOrWithoutSign()
    {
        var a = GeneratorOptions.Parse(new[] { "--duplicates", "2%", "--out-of-order", "1", "--invalid", "0.5%", "--speeding-vehicles", "5" });
        a.DuplicatePercent.Should().Be(2.0);
        a.OutOfOrderPercent.Should().Be(1.0);
        a.InvalidPercent.Should().Be(0.5);
        a.SpeedingVehiclesPercent.Should().Be(5.0);
    }

    [Fact]
    public void Parse_UsesDefaults()
    {
        var o = GeneratorOptions.Parse(Array.Empty<string>());
        o.Total.Should().Be(1_000_000);
        o.Vehicles.Should().Be(1000);
        o.Rate.Should().Be(0);
        o.Seed.Should().Be(42);
        o.Bootstrap.Should().Be("localhost:29092");
        o.Topic.Should().Be("telemetry.raw");
    }

    [Theory]
    [InlineData("-1")]
    [InlineData("101%")]
    [InlineData("abc")]
    public void Parse_RejectsNegativeOrOver100(string value)
    {
        var act = () => GeneratorOptions.Parse(new[] { "--duplicates", value });
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Parse_RejectsUnknownFlagAndBadNumbers()
    {
        ((Action)(() => GeneratorOptions.Parse(new[] { "--nope", "1" }))).Should().Throw<ArgumentException>();
        ((Action)(() => GeneratorOptions.Parse(new[] { "--total", "0" }))).Should().Throw<ArgumentException>();
        ((Action)(() => GeneratorOptions.Parse(new[] { "--total" }))).Should().Throw<ArgumentException>();
    }
}
