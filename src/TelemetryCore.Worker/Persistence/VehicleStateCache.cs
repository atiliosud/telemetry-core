using System.Collections.Concurrent;
using TelemetryCore.Domain;

namespace TelemetryCore.Worker.Persistence;

/// <summary>
/// Committed vehicle state (D5). Published objects are never mutated: processors clone before applying rules.
/// A vehicle belongs to one partition, so only one processor reads/writes a given key.
/// </summary>
public sealed class VehicleStateCache
{
    private readonly ConcurrentDictionary<string, VehicleState> _states = new(StringComparer.Ordinal);

    public int Count => _states.Count;

    public bool TryGet(string id, out VehicleState state) => _states.TryGetValue(id, out state!);

    /// <summary>Publishes committed states. An entry is only replaced by a state that is not older (a late processor
    /// left over from a rebalance can never move a vehicle back).</summary>
    public void Publish(IEnumerable<VehicleState> states)
    {
        foreach (var s in states)
        {
            _states.AddOrUpdate(
                s.VehicleId,
                static (_, added) => added,
                static (_, existing, added) => added.LastSeq >= existing.LastSeq ? added : existing,
                s);
        }
    }

    public void Evict(string vehicleId) => _states.TryRemove(vehicleId, out _);

    public void EvictPartition(int partition)
    {
        foreach (var kv in _states)
        {
            if (kv.Value.Partition == partition)
            {
                _states.TryRemove(kv);
            }
        }
    }
}
