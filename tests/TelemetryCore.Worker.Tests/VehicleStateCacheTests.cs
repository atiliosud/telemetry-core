using FluentAssertions;
using TelemetryCore.Domain;
using TelemetryCore.Worker.Persistence;

namespace TelemetryCore.Worker.Tests;

public sealed class VehicleStateCacheTests
{
    [Fact]
    public void Publish_OlderState_DoesNotReplaceNewer()
    {
        var cache = new VehicleStateCache();
        var newer = VehicleState.New("VH-1", 0);
        newer.LastSeq = 10;
        var older = VehicleState.New("VH-1", 0);
        older.LastSeq = 7;

        cache.Publish(new[] { newer });
        cache.Publish(new[] { older });

        cache.TryGet("VH-1", out var state).Should().BeTrue();
        state.Should().BeSameAs(newer);
    }

    [Fact]
    public void Publish_SameOrNewerState_Replaces()
    {
        var cache = new VehicleStateCache();
        var first = VehicleState.New("VH-1", 0);
        first.LastSeq = 10;
        var same = VehicleState.New("VH-1", 0);
        same.LastSeq = 10;

        cache.Publish(new[] { first });
        cache.Publish(new[] { same });

        cache.TryGet("VH-1", out var state).Should().BeTrue();
        state.Should().BeSameAs(same);
    }

    [Fact]
    public void Evict_And_EvictPartition_RemoveEntries()
    {
        var cache = new VehicleStateCache();
        cache.Publish(new[] { VehicleState.New("A", 1), VehicleState.New("B", 1), VehicleState.New("C", 2) });

        cache.Evict("C");
        cache.EvictPartition(1);

        cache.Count.Should().Be(0);
    }
}
