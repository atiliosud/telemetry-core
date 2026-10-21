namespace TelemetryCore.Worker.Resilience;

/// <summary>State of the DB circuit breaker for metrics and readiness: 0 closed, 1 open, 2 half-open.</summary>
public sealed class CircuitStateTracker
{
    public const int Closed = 0;
    public const int Open = 1;
    public const int HalfOpen = 2;

    private int _state;

    public int State => Volatile.Read(ref _state);

    public void Set(int state) => Volatile.Write(ref _state, state);
}
