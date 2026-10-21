namespace TelemetryCore.ReadApi;

public static class Endpoints
{
    public static void MapReadApi(this IEndpointRouteBuilder app)
    {
        app.MapGet("/health/live", () => Results.Ok(new { status = "live" }));

        app.MapGet("/health/ready", async (Queries queries, CancellationToken ct) =>
        {
            try
            {
                return await queries.PingAsync(ct)
                    ? Results.Ok(new { status = "ready" })
                    : Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
            }
            catch (Exception) when (!ct.IsCancellationRequested)
            {
                return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
            }
        });

        app.MapGet("/vehicles/{vehicleId}/state", async (string vehicleId, Queries queries, CancellationToken ct) =>
        {
            if (!QueryValidation.IsValidVehicleId(vehicleId))
            {
                return Results.ValidationProblem(new Dictionary<string, string[]>
                {
                    ["vehicleId"] = new[] { "vehicleId must match ^[A-Za-z0-9_-]{1,64}$." },
                });
            }

            var state = await queries.GetStateAsync(vehicleId, ct);
            return state is null ? Results.Problem(statusCode: StatusCodes.Status404NotFound, title: "Vehicle not found.") : Results.Ok(state);
        });

        app.MapGet("/alerts", async (HttpContext http, Queries queries, CancellationToken ct) =>
        {
            if (!QueryValidation.TryParseAlerts(http.Request.Query, out var query, out var errors))
            {
                return Results.ValidationProblem(errors);
            }

            return Results.Ok(await queries.GetAlertsAsync(query!, ct));
        });
    }
}
