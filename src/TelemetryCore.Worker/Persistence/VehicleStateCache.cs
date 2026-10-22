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

    // Ownership generation per partition, bumped by EvictPartition (assign / revoke). A batch captures it before it
    // starts; a processor left over from an earlier ownership (e.g. cancelled after a drain timeout but already in
    // COMMIT) then cannot publish into the cache of a later ownership. _gate makes "check generation + publish"
    // atomic against "bump generation + evict".
    private readonly Dictionary<int, long> _generations = new();
    private readonly object _gate = new();

    public int Count => _states.Count;

    public bool TryGet(string id, out VehicleState state) => _states.TryGetValue(id, out state!);

    /// <summary>Current ownership generation of <paramref name="partition"/>; capture it before reading state for a batch.</summary>
    public long Generation(int partition)
    {
        lock (_gate)
        {
            return _generations.GetValueOrDefault(partition);
        }
    }

    /// <summary>Publishes committed states only if <paramref name="partition"/> is still in <paramref name="generation"/>;
    /// returns false (nothing published) when the partition was evicted since.</summary>
    public bool Publish(IEnumerable<VehicleState> states, int partition, long generation)
    {
        lock (_gate)
        {
            if (_generations.GetValueOrDefault(partition) != generation)
            {
                return false;
            }

            Publish(states);
            return true;
        }
    }

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
        lock (_gate)
        {
            _generations[partition] = _generations.GetValueOrDefault(partition) + 1;
            foreach (var kv in _states)
            {
                if (kv.Value.Partition == partition)
                {
                    _states.TryRemove(kv);
                }
            }
        }
    }
}
