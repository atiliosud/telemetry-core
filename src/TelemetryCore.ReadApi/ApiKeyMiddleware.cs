using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;

namespace TelemetryCore.ReadApi;

public static class ApiKeyHeader
{
    public const string Name = "X-Api-Key";

    public static bool IsValid(HttpContext context, string expectedKey)
    {
        var presented = context.Request.Headers[Name].ToString();
        if (presented.Length == 0)
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(Encoding.UTF8.GetBytes(presented), Encoding.UTF8.GetBytes(expectedKey));
    }
}

/// <summary>Requires a valid <c>X-Api-Key</c> on everything except the health endpoints.</summary>
public sealed class ApiKeyMiddleware(RequestDelegate next, IOptions<ReadApiOptions> options)
{
    private readonly string _expectedKey = options.Value.ApiKey;

    public async Task InvokeAsync(HttpContext context)
    {
        if (context.Request.Path.StartsWithSegments("/health") || ApiKeyHeader.IsValid(context, _expectedKey))
        {
            await next(context);
            return;
        }

        await Results.Problem(statusCode: StatusCodes.Status401Unauthorized, title: "Unauthorized.").ExecuteAsync(context);
    }
}
