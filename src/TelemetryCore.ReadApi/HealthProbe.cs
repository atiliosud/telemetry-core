namespace TelemetryCore.ReadApi;

/// <summary>
/// <c>dotnet TelemetryCore.ReadApi.dll --healthcheck</c>: GET the liveness endpoint and exit 0/1. Used by the container
/// healthcheck, because the aspnet base image has no curl or wget (same approach as the worker).
/// </summary>
internal static class HealthProbe
{
    public const string Argument = "--healthcheck";

    public static async Task<int> RunAsync()
    {
        var url = Environment.GetEnvironmentVariable("HEALTHCHECK_URL") ?? "http://127.0.0.1:8080/health/live";
        try
        {
            using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(3) };
            using var response = await http.GetAsync(url).ConfigureAwait(false);
            return response.IsSuccessStatusCode ? 0 : 1;
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return 1;
        }
    }
}
