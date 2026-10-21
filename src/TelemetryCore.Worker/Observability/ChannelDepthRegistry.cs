using System.Collections.Concurrent;
using System.Threading.Channels;

namespace TelemetryCore.Worker.Observability;

/// <summary>
/// Live partition channels, read by the <c>telemetry_channel_depth{partition}</c> gauge. The consume loop registers a
/// channel on assign and unregisters it on revoke/stop, so only owned partitions report.
/// </summary>
public sealed class ChannelDepthRegistry
{
    private readonly ConcurrentDictionary<int, Func<int>> _channels = new();

    public void Register<T>(int partition, ChannelReader<T> reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        _channels[partition] = () => reader.Count;
    }

    public void Unregister(int partition) => _channels.TryRemove(partition, out _);

    /// <summary>Current depth of every live channel.</summary>
    public IEnumerable<(int Partition, int Depth)> Snapshot()
    {
        foreach (var kv in _channels)
        {
            yield return (kv.Key, kv.Value());
        }
    }
}
