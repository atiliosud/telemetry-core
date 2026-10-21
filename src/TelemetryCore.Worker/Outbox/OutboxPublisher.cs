using System.Collections.Concurrent;
using Confluent.Kafka;
using Npgsql;
using Polly;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.Worker.Outbox;

/// <summary>
/// Publishes pending <c>outbox</c> rows to Kafka (D8: at-least-once). Every poll runs one transaction that locks up to
/// <see cref="BatchLimit"/> unpublished rows (<c>FOR UPDATE SKIP LOCKED</c>, so replicas never take the same row), sends
/// them through the bulkhead (4 concurrent, 100 queued) and marks the acknowledged ones. A failed send leaves the row
/// pending for the next poll. The publisher has its own producer and its own small connection pool, so it can never
/// starve ingestion, and it never throws into the host.
/// </summary>
public sealed partial class OutboxPublisher : BackgroundService
{
    public const int BatchLimit = 100;

    private const string SelectSql = """
        SELECT id, topic, key, payload::text FROM outbox
        WHERE published_utc IS NULL ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED
        """;

    private const string MarkSql = "UPDATE outbox SET published_utc = now() WHERE id = ANY(@published)";

    private readonly NpgsqlDataSource _dataSource;
    private readonly IProducer<string, string> _producer;
    private readonly ResiliencePipeline _bulkhead = ResiliencePipelines.CreateOutbox();
    private readonly TimeSpan _pollInterval;
    private readonly ILogger<OutboxPublisher> _log;

    public OutboxPublisher(WorkerOptions options, NpgsqlDataSource outboxDataSource, ILogger<OutboxPublisher> log)
    {
        _dataSource = outboxDataSource;
        _log = log;
        _pollInterval = options.OutboxPollInterval;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 5,
            MessageTimeoutMs = 10_000,
        }).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var published = await PublishPendingAsync(stoppingToken).ConfigureAwait(false);
                if (published > 0)
                {
                    LogPublished(_log, published);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogFailed(_log, ex);
            }

            try
            {
                await Task.Delay(_pollInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    /// <summary>One poll: lock, publish, mark, commit. Returns how many rows were published.</summary>
    private async Task<int> PublishPendingAsync(CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(ct).ConfigureAwait(false);

        var rows = new List<(long Id, string Topic, string Key, string Payload)>();
        await using (var select = new NpgsqlCommand(SelectSql, conn, tx))
        await using (var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add((reader.GetInt64(0), reader.GetString(1), reader.GetString(2), reader.GetString(3)));
            }
        }

        if (rows.Count == 0)
        {
            return 0;
        }

        var published = new ConcurrentBag<long>();
        // The first failure cancels the rest of the batch (queued sends never start): the broker is down, no point waiting.
        using var batchCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var sends = rows.Select(async row =>
        {
            try
            {
                await _bulkhead.ExecuteAsync(async token =>
                {
                    await _producer.ProduceAsync(row.Topic, new Message<string, string> { Key = row.Key, Value = row.Payload }, token)
                        .ConfigureAwait(false);
                }, batchCts.Token).ConfigureAwait(false);
                published.Add(row.Id);
            }
            catch (Exception ex)
            {
                if (!batchCts.IsCancellationRequested)
                {
                    LogSendFailed(_log, ex, row.Id);
                    await batchCts.CancelAsync().ConfigureAwait(false);
                }
            }
        }).ToArray();
        await Task.WhenAll(sends).ConfigureAwait(false);

        if (published.IsEmpty)
        {
            return 0;
        }

        // Acknowledged rows are marked even when shutdown began mid-batch; a lost mark only means a duplicate (at-least-once).
        using var markCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using (var mark = new NpgsqlCommand(MarkSql, conn, tx))
        {
            mark.Parameters.Add(new NpgsqlParameter<long[]>("published", published.ToArray()));
            await mark.ExecuteNonQueryAsync(markCts.Token).ConfigureAwait(false);
        }

        await tx.CommitAsync(markCts.Token).ConfigureAwait(false);
        return published.Count;
    }

    public override void Dispose()
    {
        _producer.Dispose();
        base.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Outbox published {Count} message(s)")]
    private static partial void LogPublished(ILogger log, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Outbox poll failed; retrying on the next poll")]
    private static partial void LogFailed(ILogger log, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Outbox row {Id} was not published; it stays pending")]
    private static partial void LogSendFailed(ILogger log, Exception ex, long id);
}
