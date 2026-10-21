using Microsoft.Extensions.Diagnostics.HealthChecks;
using Npgsql;
using TelemetryCore.Worker.Ingestion;
using TelemetryCore.Worker.Resilience;

namespace TelemetryCore.Worker.Health;

/// <summary>
/// <c>/health/ready</c>: the ingestion loop is running, the DB circuit is not open, and the DB answers <c>SELECT 1</c>
/// within 2 s. <c>/health/live</c> runs no checks (the process answers HTTP).
/// </summary>
public sealed class ReadinessCheck : IHealthCheck
{
    public const string Tag = "ready";
    private static readonly TimeSpan DbTimeout = TimeSpan.FromSeconds(2);

    private readonly NpgsqlDataSource _dataSource;
    private readonly CircuitStateTracker _circuit;
    private readonly IngestionStatus _ingestion;

    public ReadinessCheck(NpgsqlDataSource dataSource, CircuitStateTracker circuit, IngestionStatus ingestion)
    {
        _dataSource = dataSource;
        _circuit = circuit;
        _ingestion = ingestion;
    }

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_ingestion.IsRunning)
        {
            return HealthCheckResult.Unhealthy("ingestion loop not running");
        }

        if (_circuit.State == CircuitStateTracker.Open)
        {
            return HealthCheckResult.Unhealthy("database circuit breaker open");
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(DbTimeout);
        try
        {
            await using var cmd = _dataSource.CreateCommand("SELECT 1");
            await cmd.ExecuteScalarAsync(timeout.Token).ConfigureAwait(false);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex) when (ex is NpgsqlException or OperationCanceledException or TimeoutException or System.Net.Sockets.SocketException)
        {
            return HealthCheckResult.Unhealthy("database unreachable", ex);
        }
    }
}
