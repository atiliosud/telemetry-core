namespace TelemetryCore.ReadApi;

public sealed class ReadApiOptions
{
    public const string SectionName = "ReadApi";
    public const int MinApiKeyLength = 16;

    /// <summary>Connection string of the <c>telemetry_reader</c> role (SELECT only, D13).</summary>
    public string PostgresConnectionString { get; set; } = string.Empty;

    /// <summary>Shared secret expected in <c>X-Api-Key</c>; comes from env <c>TELEMETRY_API_KEY</c>.</summary>
    public string ApiKey { get; set; } = string.Empty;
}
