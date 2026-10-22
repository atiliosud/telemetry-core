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

    [Fact]
    public void Publish_FromEarlierOwnership_AfterRevokeAndReassign_IsDropped()
    {
        // A processor cancelled after a drain timeout is still in COMMIT; its batch captured the generation before.
        var cache = new VehicleStateCache();
        var staleGeneration = cache.Generation(3);
        cache.EvictPartition(3); // revoked
        cache.EvictPartition(3); // assigned back to this pod
        var stale = VehicleState.New("VH-1", 3);
        stale.LastSeq = 40;

        cache.Publish(new[] { stale }, 3, staleGeneration).Should().BeFalse();

        cache.TryGet("VH-1", out _).Should().BeFalse("the new owner must reload VH-1 from vehicle_state, not use the zombie's copy");
    }

    [Fact]
    public void Publish_CurrentOwnership_IsPublished_AndOtherPartitionsAreIndependent()
    {
        var cache = new VehicleStateCache();
        cache.EvictPartition(3);
        var generation = cache.Generation(3);
        var otherGeneration = cache.Generation(4);
        cache.EvictPartition(5); // another partition's rebalance does not affect partitions 3 and 4

        cache.Publish(new[] { VehicleState.New("VH-1", 3) }, 3, generation).Should().BeTrue();
        cache.Publish(new[] { VehicleState.New("VH-2", 4) }, 4, otherGeneration).Should().BeTrue();

        cache.TryGet("VH-1", out _).Should().BeTrue();
        cache.TryGet("VH-2", out _).Should().BeTrue();
    }
}
