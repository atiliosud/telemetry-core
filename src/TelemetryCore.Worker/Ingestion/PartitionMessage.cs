using System.Diagnostics;

namespace TelemetryCore.Worker.Ingestion;

/// <summary>One Kafka record handed from the consume loop to the processor of its partition.</summary>
public readonly record struct PartitionMessage(
    int Partition,
    long Offset,
    string? Key,
    byte[]? Value,
    ActivityContext ParentContext,
    DateTimeOffset ConsumedAt);
