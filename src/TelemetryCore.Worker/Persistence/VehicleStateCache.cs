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

    public void Publish(IEnumerable<VehicleState> states)
    {
        foreach (var s in states)
        {
            _states[s.VehicleId] = s;
        }
    }

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
