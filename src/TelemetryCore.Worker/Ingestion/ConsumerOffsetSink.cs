using Confluent.Kafka;

namespace TelemetryCore.Worker.Ingestion;

/// <summary>Stores offsets in the consumer; the client's auto-commit (and the explicit commits on revoke/stop) send them to Kafka.</summary>
public sealed partial class ConsumerOffsetSink : IOffsetSink
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly string _topic;
    private readonly ILogger _log;

    public ConsumerOffsetSink(IConsumer<string, byte[]> consumer, string topic, ILogger log)
    {
        _consumer = consumer;
        _topic = topic;
        _log = log;
    }

    public void Store(int partition, long nextOffset)
    {
        try
        {
            _consumer.StoreOffset(new TopicPartitionOffset(_topic, partition, nextOffset));
        }
        catch (KafkaException ex)
        {
            // The partition is no longer assigned (lost / late processor). The new owner replays from the last
            // committed offset and the DB key makes the replay idempotent (D1).
            LogStoreFailed(_log, ex, partition, nextOffset);
        }
        catch (ObjectDisposedException ex)
        {
            // A processor aborted after a shutdown drain timeout finished after the consumer was closed.
            LogStoreFailed(_log, ex, partition, nextOffset);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not store offset {NextOffset} for partition {Partition}; it will be replayed by the partition owner")]
    private static partial void LogStoreFailed(ILogger log, Exception ex, int partition, long nextOffset);
}
