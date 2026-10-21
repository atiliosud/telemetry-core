using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;
using TelemetryCore.Contracts;
using TelemetryCore.Domain;
using Testcontainers.Kafka;
using Testcontainers.PostgreSql;
using Testcontainers.Redis;

namespace TelemetryCore.IntegrationTests.Fixtures;

[CollectionDefinition(Name)]
public sealed class TelemetryStackCollection : ICollectionFixture<TelemetryStackFixture>
{
    public const string Name = "telemetry-stack";
}

/// <summary>
/// One Postgres (initialised with the real <c>db/</c> scripts), Redis and Kafka (KRaft, apache/kafka) per test run.
/// Tests stay independent by using a unique topic, consumer group and vehicle prefix.
/// </summary>
public sealed class TelemetryStackFixture : IAsyncLifetime
{
    public const string WorkerPassword = "worker_test_pw";
    public const string ReaderPassword = "reader_test_pw";
    private const string Database = "telemetry";

    private static readonly HashSet<string> KnownTables = new(StringComparer.Ordinal)
    {
        "processed_events", "vehicle_state", "alerts", "outbox",
    };

    private readonly PostgreSqlContainer _postgres;
    private readonly RedisContainer _redis;
    private readonly KafkaContainer _kafka;
    private IProducer<string?, byte[]?>? _producer;

    public TelemetryStackFixture()
    {
        var dbDir = Path.Combine(FindRepoRoot(), "db");
        _postgres = new PostgreSqlBuilder("postgres:16-alpine")
            .WithDatabase(Database)
            .WithUsername("postgres")
            .WithPassword("postgres_test_pw")
            .WithEnvironment("TELEMETRY_WORKER_DB_PASSWORD", WorkerPassword)
            .WithEnvironment("TELEMETRY_READER_DB_PASSWORD", ReaderPassword)
            .WithResourceMapping(ReadLf(Path.Combine(dbDir, "000_roles.sh")), "/docker-entrypoint-initdb.d/000_roles.sh")
            .WithResourceMapping(ReadLf(Path.Combine(dbDir, "001_init.sql")), "/docker-entrypoint-initdb.d/001_init.sql")
            .Build();
        _redis = new RedisBuilder("redis:7.4-alpine").Build();
        _kafka = new KafkaBuilder("apache/kafka:3.9.0")
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .Build();
    }

    public string BootstrapServers { get; private set; } = string.Empty;

    /// <summary>Superuser connection, for test assertions only.</summary>
    public string AdminConnectionString { get; private set; } = string.Empty;

    /// <summary>Least-privilege role used by the worker (D13).</summary>
    public string WorkerConnectionString { get; private set; } = string.Empty;

    public string RedisConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        await Task.WhenAll(_postgres.StartAsync(), _redis.StartAsync(), _kafka.StartAsync());

        AdminConnectionString = _postgres.GetConnectionString();
        WorkerConnectionString = new NpgsqlConnectionStringBuilder
        {
            Host = _postgres.Hostname,
            Port = _postgres.GetMappedPublicPort(PostgreSqlBuilder.PostgreSqlPort),
            Database = Database,
            Username = "telemetry_worker",
            Password = WorkerPassword,
        }.ConnectionString;
        RedisConnectionString = _redis.GetConnectionString();
        BootstrapServers = _kafka.GetBootstrapAddress();

        // The init scripts run before the final server start, but be defensive about the window.
        await WaitUntilAsync(async () =>
        {
            try
            {
                await using var conn = new NpgsqlConnection(WorkerConnectionString);
                await conn.OpenAsync();
                return true;
            }
            catch (NpgsqlException)
            {
                return false;
            }
        }, TimeSpan.FromSeconds(30));

        await CreateTopicAsync(Topics.Raw, 12);
        await CreateTopicAsync(Topics.Dlq, 3);
        await CreateTopicAsync(Topics.Alerts, 3);

        _producer = new ProducerBuilder<string?, byte[]?>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All,
            LingerMs = 5,
        }).Build();
    }

    public async Task DisposeAsync()
    {
        _producer?.Dispose();
        await Task.WhenAll(_kafka.DisposeAsync().AsTask(), _redis.DisposeAsync().AsTask(), _postgres.DisposeAsync().AsTask());
    }

    public static string UniqueName(string prefix) => prefix + "-" + Guid.NewGuid().ToString("N")[..10];

    public async Task CreateTopicAsync(string topic, int partitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        try
        {
            await admin.CreateTopicsAsync(new[] { new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 } });
        }
        catch (CreateTopicsException ex) when (ex.Results.TrueForAll(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }
    }

    public async Task ProduceAsync(IEnumerable<TelemetryEventDto> events, string topic = Topics.Raw)
    {
        foreach (var dto in events)
        {
            var payload = JsonSerializer.SerializeToUtf8Bytes(dto, TelemetryJsonContext.Default.TelemetryEventDto);
            await ProduceRawAsync(dto.VehicleId, payload, topic);
        }
    }

    public async Task ProduceRawAsync(string? key, byte[]? value, string topic = Topics.Raw)
    {
        await _producer!.ProduceAsync(topic, new Message<string?, byte[]?> { Key = key, Value = value });
    }

    public async Task<long> CountRowsAsync(string table, string? vehiclePrefix = null)
    {
        if (!KnownTables.Contains(table))
        {
            throw new ArgumentException("Unknown table " + table, nameof(table));
        }

        await using var conn = new NpgsqlConnection(AdminConnectionString);
        await conn.OpenAsync();
        var column = table == "outbox" ? "key" : "vehicle_id";
        await using var cmd = new NpgsqlCommand(
            vehiclePrefix is null
                ? $"SELECT count(*) FROM {table}"
                : $"SELECT count(*) FROM {table} WHERE {column} LIKE @prefix || '%'", conn);
        if (vehiclePrefix is not null)
        {
            cmd.Parameters.AddWithValue("prefix", vehiclePrefix);
        }

        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    public async Task<VehicleState?> GetStateAsync(string vehicleId)
    {
        await using var conn = new NpgsqlConnection(AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            "SELECT last_seq, last_event_utc, lat, lon, speed_kmh, ignition, distance_km_today, speeding, overspeed_streak, distance_date " +
            "FROM vehicle_state WHERE vehicle_id = @id", conn);
        cmd.Parameters.AddWithValue("id", vehicleId);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return new VehicleState
        {
            VehicleId = vehicleId,
            LastSeq = reader.GetInt64(0),
            LastEventUtc = reader.GetFieldValue<DateTimeOffset>(1),
            Lat = reader.GetDouble(2),
            Lon = reader.GetDouble(3),
            SpeedKmh = reader.GetFloat(4),
            Ignition = reader.GetBoolean(5),
            DistanceKmToday = reader.GetDouble(6),
            Speeding = reader.GetBoolean(7),
            OverspeedStreak = reader.GetInt32(8),
            DistanceDate = reader.IsDBNull(9) ? null : reader.GetFieldValue<DateOnly>(9),
        };
    }

    public static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (!await condition())
        {
            if (sw.Elapsed > timeout)
            {
                throw new TimeoutException($"Condition not met within {timeout}.");
            }

            await Task.Delay(100);
        }
    }

    /// <summary>Reads every message currently in <paramref name="topic"/> (all partitions, up to the high watermark).</summary>
    public async Task<List<ConsumeResult<string?, byte[]?>>> ConsumeAllAsync(string topic)
    {
        var highs = await GetHighWatermarksAsync(topic);
        var results = new List<ConsumeResult<string?, byte[]?>>();
        var pending = new Dictionary<int, long>();
        foreach (var (partition, high) in highs)
        {
            if (high > 0)
            {
                pending[partition] = high;
            }
        }

        if (pending.Count == 0)
        {
            return results;
        }

        using var consumer = new ConsumerBuilder<string?, byte[]?>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = UniqueName("reader"),
            EnableAutoCommit = false,
        }).Build();
        var assignment = new List<TopicPartitionOffset>();
        foreach (var partition in pending.Keys)
        {
            assignment.Add(new TopicPartitionOffset(topic, partition, Offset.Beginning));
        }

        consumer.Assign(assignment);
        var sw = Stopwatch.StartNew();
        while (pending.Count > 0)
        {
            if (sw.Elapsed > TimeSpan.FromSeconds(30))
            {
                throw new TimeoutException("Could not read topic " + topic);
            }

            var result = consumer.Consume(TimeSpan.FromMilliseconds(200));
            if (result is null)
            {
                continue;
            }

            results.Add(result);
            if (pending.TryGetValue(result.Partition.Value, out var high) && result.Offset.Value + 1 >= high)
            {
                pending.Remove(result.Partition.Value);
            }
        }

        consumer.Close();
        return results;
    }

    public Task<Dictionary<int, long>> GetHighWatermarksAsync(string topic)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();
        var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(10));
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = UniqueName("watermarks"),
        }).Build();
        var highs = new Dictionary<int, long>();
        foreach (var p in metadata.Topics[0].Partitions)
        {
            var w = consumer.QueryWatermarkOffsets(new TopicPartition(topic, p.PartitionId), TimeSpan.FromSeconds(10));
            highs[p.PartitionId] = w.High.Value;
        }

        return Task.FromResult(highs);
    }

    /// <summary>Committed offsets of <paramref name="groupId"/> per partition (-1 when nothing is committed).</summary>
    public Task<Dictionary<int, long>> GetCommittedOffsetsAsync(string groupId, string topic, IEnumerable<int> partitions)
    {
        using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
        }).Build();
        var tps = new List<TopicPartition>();
        foreach (var p in partitions)
        {
            tps.Add(new TopicPartition(topic, p));
        }

        var committed = consumer.Committed(tps, TimeSpan.FromSeconds(10));
        var result = new Dictionary<int, long>();
        foreach (var c in committed)
        {
            result[c.Partition.Value] = c.Offset.IsSpecial ? -1 : c.Offset.Value;
        }

        return Task.FromResult(result);
    }

    private static byte[] ReadLf(string path) =>
        Encoding.UTF8.GetBytes(File.ReadAllText(path).Replace("\r\n", "\n", StringComparison.Ordinal));

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "TelemetryCore.sln")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("Repository root (TelemetryCore.sln) not found.");
    }
}
