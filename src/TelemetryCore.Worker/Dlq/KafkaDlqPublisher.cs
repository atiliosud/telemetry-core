using System.Globalization;
using System.Text;
using Confluent.Kafka;
using TelemetryCore.Contracts;
using TelemetryCore.Worker.Ingestion;

namespace TelemetryCore.Worker.Dlq;

/// <summary>
/// Writes rejected messages to the dead-letter topic (D17: at-least-once). Key and value are the original ones (a null
/// value becomes an empty array); headers <c>reason</c>, <c>source-partition</c>, <c>source-offset</c> say why and where
/// from. <see cref="PublishAsync"/> completes only after the broker acknowledged the write (acks=all, idempotent producer).
/// </summary>
public sealed class KafkaDlqPublisher : IDlqPublisher, IDisposable
{
    private readonly IProducer<string?, byte[]> _producer;
    private readonly string _topic;

    public KafkaDlqPublisher(WorkerOptions options)
    {
        _topic = options.DlqTopic;
        _producer = new ProducerBuilder<string?, byte[]>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 5,
            // A delivery that cannot complete fails within 30 s; the processor logs it and retries (offset not stored).
            MessageTimeoutMs = 30_000,
        }).Build();
    }

    public async Task PublishAsync(PartitionMessage msg, string reason, CancellationToken ct)
    {
        var headers = new Headers
        {
            { TelemetryHeaders.Reason, Encoding.UTF8.GetBytes(reason) },
            { TelemetryHeaders.SourcePartition, Encoding.UTF8.GetBytes(msg.Partition.ToString(CultureInfo.InvariantCulture)) },
            { TelemetryHeaders.SourceOffset, Encoding.UTF8.GetBytes(msg.Offset.ToString(CultureInfo.InvariantCulture)) },
        };
        var message = new Message<string?, byte[]> { Key = msg.Key, Value = msg.Value ?? Array.Empty<byte>(), Headers = headers };
        await _producer.ProduceAsync(_topic, message, ct).ConfigureAwait(false);
    }

    public void Dispose() => _producer.Dispose();
}
