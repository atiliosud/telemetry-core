using System.Diagnostics;
using System.Globalization;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Options;
using Npgsql;
using TelemetryCore.ReadApi;

var builder = WebApplication.CreateBuilder(args);
var services = builder.Services;

services.AddOptions<ReadApiOptions>()
    .Bind(builder.Configuration.GetSection(ReadApiOptions.SectionName))
    .PostConfigure<IConfiguration>((o, config) =>
    {
        // The API key comes from env TELEMETRY_API_KEY (flat key, not under the ReadApi section).
        var key = config["TELEMETRY_API_KEY"];
        if (!string.IsNullOrEmpty(key))
        {
            o.ApiKey = key;
        }
    })
    .Validate(o => !string.IsNullOrWhiteSpace(o.PostgresConnectionString), "ReadApi:PostgresConnectionString is required")
    .Validate(o => o.ApiKey.Length >= ReadApiOptions.MinApiKeyLength, "TELEMETRY_API_KEY is required and must have at least 16 characters")
    .ValidateOnStart();

services.AddSingleton(sp => NpgsqlDataSource.Create(sp.GetRequiredService<IOptions<ReadApiOptions>>().Value.PostgresConnectionString));
services.AddSingleton<Queries>();
services.AddProblemDetails();

// Registered through a startup filter so it always wraps the whole pipeline (also anything added after it).
services.AddTransient<IStartupFilter, SafetyStartupFilter>();

services.AddRateLimiter(o =>
{
    o.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
    o.OnRejected = async (ctx, ct) =>
    {
        if (ctx.Lease.TryGetMetadata(MetadataName.RetryAfter, out var retryAfter))
        {
            ctx.HttpContext.Response.Headers.RetryAfter = ((int)Math.Ceiling(retryAfter.TotalSeconds)).ToString(CultureInfo.InvariantCulture);
        }

        await Results.Problem(statusCode: StatusCodes.Status429TooManyRequests, title: "Too many requests.")
            .ExecuteAsync(ctx.HttpContext);
    };
    o.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(http =>
    {
        // Only a valid key gets its own bucket; anything else shares the per-IP bucket so random keys cannot mint buckets.
        var expected = http.RequestServices.GetRequiredService<IOptions<ReadApiOptions>>().Value.ApiKey;
        var partition = ApiKeyHeader.IsValid(http, expected)
            ? "key"
            : "ip:" + (http.Connection.RemoteIpAddress?.ToString() ?? "unknown");
        return RateLimitPartition.GetFixedWindowLimiter(partition, _ => new FixedWindowRateLimiterOptions
        {
            PermitLimit = 100,
            Window = TimeSpan.FromSeconds(10),
            QueueLimit = 0,
            AutoReplenishment = true,
        });
    });
});

var app = builder.Build();
app.UseRateLimiter();
app.UseMiddleware<ApiKeyMiddleware>();
app.MapReadApi();
app.Run();

/// <summary>Entry point marker for <c>WebApplicationFactory&lt;Program&gt;</c>.</summary>
public partial class Program
{
}

/// <summary>Security headers and the generic 500 handler, outermost in the pipeline.</summary>
internal sealed class SafetyStartupFilter : IStartupFilter
{
    public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
    {
        app.Use(async (context, nextMiddleware) =>
        {
            context.Response.OnStarting(() =>
            {
                context.Response.Headers["X-Content-Type-Options"] = "nosniff";
                context.Response.Headers.CacheControl = "no-store";
                return Task.CompletedTask;
            });
            await nextMiddleware(context);
        });
        app.UseExceptionHandler(new ExceptionHandlerOptions
        {
            ExceptionHandler = context =>
            {
                var traceId = Activity.Current?.Id ?? context.TraceIdentifier;
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                return Results.Problem(
                        statusCode: StatusCodes.Status500InternalServerError,
                        title: "An unexpected error occurred.",
                        extensions: new Dictionary<string, object?> { ["traceId"] = traceId })
                    .ExecuteAsync(context);
            },
        });
        next(app);
    };
}
