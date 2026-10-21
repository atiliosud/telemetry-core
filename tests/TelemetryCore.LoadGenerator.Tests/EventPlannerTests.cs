using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;
using TelemetryCore.LoadGenerator;

namespace TelemetryCore.LoadGenerator.Tests;

public class EventPlannerTests
{
    private static readonly DateTimeOffset Now = new(2026, 10, 22, 10, 0, 0, TimeSpan.Zero);

    private static GeneratorOptions Opts(int total = 10_000, int seed = 42) => GeneratorOptions.Parse(new[]
    {
        "--total", total.ToString(), "--vehicles", "100", "--seed", seed.ToString(),
        "--duplicates", "2%", "--out-of-order", "1%", "--invalid", "0.5%", "--speeding-vehicles", "5%",
    });

    private static (List<PlannedMessage> Msgs, EventPlanner Planner) Run(GeneratorOptions o)
    {
        var planner = new EventPlanner(o, new FakeTimeProvider(Now));
        return (planner.Plan().ToList(), planner);
    }

    private static TelemetryEventDto Decode(PlannedMessage m) =>
        JsonSerializer.Deserialize(m.Payload, TelemetryJsonContext.Default.TelemetryEventDto)!;

    [Fact]
    public void Plan_UsesVehiclePrefix_ForKeysAndIds()
    {
        var o = GeneratorOptions.Parse(new[] { "--total", "500", "--vehicles", "10", "--vehicle-prefix", "RUN7-" });
        var (msgs, _) = Run(o);
        msgs.Should().OnlyContain(m => m.Key.StartsWith("RUN7-"));
        msgs.Where(m => m.Kind != MessageKind.Invalid).Select(Decode).Should().OnlyContain(e => e.VehicleId!.StartsWith("RUN7-"));
    }

    [Fact]
    public void Plan_EmitsExactlyTotal()
    {
        var (msgs, p) = Run(Opts());
        msgs.Should().HaveCount(10_000);
        p.Summary.Sent.Should().Be(10_000);
    }

    [Fact]
    public void Plan_CountsMatchRatios()
    {
        var (msgs, p) = Run(Opts());
        msgs.Count(m => m.Kind == MessageKind.Duplicate).Should().Be(200);
        msgs.Count(m => m.Kind == MessageKind.Invalid).Should().Be(50);
        p.Summary.DuplicatesInjected.Should().Be(200);
        p.Summary.InvalidInjected.Should().Be(50);
        p.Summary.OutOfOrderInjected.Should().Be(100);
        p.Summary.ExpectedUnique.Should().Be(10_000 - 200 - 50);
        p.Summary.VehiclesSpeeding.Should().Be(5);
    }

    [Fact]
    public void Plan_UniqueValidEventIdsEqualExpectedUnique()
    {
        var (msgs, p) = Run(Opts());
        var ids = msgs.Where(m => m.Kind != MessageKind.Invalid).Select(m => Decode(m).EventId!.Value).ToHashSet();
        ids.Count.Should().Be((int)p.Summary.ExpectedUnique);
    }

    [Fact]
    public void Plan_DuplicatesReuseExistingEventIds()
    {
        var (msgs, _) = Run(Opts());
        var seen = new Dictionary<Guid, byte[]>();
        foreach (var m in msgs)
        {
            if (m.Kind == MessageKind.Invalid) continue;
            var id = Decode(m).EventId!.Value;
            if (m.Kind == MessageKind.Duplicate)
            {
                seen.Should().ContainKey(id);
                m.Payload.Should().Equal(seen[id]);
            }
            else
            {
                seen.Should().NotContainKey(id);
                seen[id] = m.Payload;
            }
        }
    }

    [Fact]
    public void Plan_SeqIncreasesPerVehicleExceptInjectedSwaps()
    {
        var (msgs, p) = Run(Opts());
        var last = new Dictionary<string, long>();
        var inversions = 0;
        foreach (var m in msgs.Where(m => m.Kind == MessageKind.Valid))
        {
            var d = Decode(m);
            if (last.TryGetValue(d.VehicleId!, out var prev) && d.Seq!.Value < prev) inversions++;
            else last[d.VehicleId!] = d.Seq!.Value;
        }
        inversions.Should().Be((int)p.Summary.OutOfOrderInjected);
    }

    [Fact]
    public void Plan_InvalidPayloadsFailValidator()
    {
        var (msgs, _) = Run(Opts());
        var invalid = msgs.Where(m => m.Kind == MessageKind.Invalid).ToList();
        invalid.Should().NotBeEmpty();
        foreach (var m in invalid)
            EventValidator.TryParse(m.Payload, Now, out _, out _).Should().BeFalse();
        invalid.Select(m => { EventValidator.TryParse(m.Payload, Now, out _, out var r); return r; })
            .Distinct().Should().HaveCountGreaterThan(3);
    }

    [Fact]
    public void Plan_ValidPayloadsPassValidator()
    {
        var (msgs, _) = Run(Opts());
        foreach (var m in msgs.Where(m => m.Kind != MessageKind.Invalid))
            EventValidator.TryParse(m.Payload, Now, out _, out _).Should().BeTrue();
    }

    [Fact]
    public void Plan_SameSeedSameOutput()
    {
        var (a, _) = Run(Opts(2000));
        var (b, _) = Run(Opts(2000));
        var (c, _) = Run(Opts(2000, seed: 7));
        a.Select(m => (m.Key, m.Kind, Convert.ToBase64String(m.Payload)))
            .Should().Equal(b.Select(m => (m.Key, m.Kind, Convert.ToBase64String(m.Payload))));
        a.Select(m => Convert.ToBase64String(m.Payload)).Should().NotEqual(c.Select(m => Convert.ToBase64String(m.Payload)));
    }

    [Fact]
    public void Plan_SmallTotalsStillExact()
    {
        var o = GeneratorOptions.Parse(new[] { "--total", "10", "--vehicles", "3", "--duplicates", "20", "--invalid", "10", "--out-of-order", "20" });
        var (msgs, p) = Run(o);
        msgs.Should().HaveCount(10);
        p.Summary.DuplicatesInjected.Should().Be(2);
        p.Summary.InvalidInjected.Should().Be(1);
        p.Summary.OutOfOrderInjected.Should().Be(2);
    }
}
