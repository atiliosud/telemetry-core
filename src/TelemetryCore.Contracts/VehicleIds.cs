using System.Text.RegularExpressions;

namespace TelemetryCore.Contracts;

/// <summary>
/// Shared vehicle id format (D29): 1-64 ASCII letters, digits, '_' or '-'. Used by the worker validator and the Read API,
/// so every id that reaches PostgreSQL can also be queried, and NUL or control characters never reach a text column.
/// </summary>
public static partial class VehicleIds
{
    /// <summary>Documented form of the rule; matching uses <c>\z</c> so a trailing newline is rejected too.</summary>
    public const string Pattern = "^[A-Za-z0-9_-]{1,64}$";

    [GeneratedRegex(@"^[A-Za-z0-9_-]{1,64}\z", RegexOptions.CultureInvariant)]
    private static partial Regex Regex();

    public static bool IsValid(string? vehicleId) => vehicleId is not null && Regex().IsMatch(vehicleId);
}
