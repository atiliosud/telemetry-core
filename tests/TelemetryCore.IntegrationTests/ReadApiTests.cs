using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using TelemetryCore.IntegrationTests.Fixtures;

namespace TelemetryCore.IntegrationTests;

[Collection(TelemetryStackCollection.Name)]
public sealed class ReadApiTests
{
    private const string ApiKey = "test-api-key-0123456789";
    private static readonly DateTime Base = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly TelemetryStackFixture _stack;

    public ReadApiTests(TelemetryStackFixture stack) => _stack = stack;

    private string ReaderConnectionString => new NpgsqlConnectionStringBuilder(_stack.AdminConnectionString)
    {
        Username = "telemetry_reader",
        Password = TelemetryStackFixture.ReaderPassword,
    }.ConnectionString;

    private WebApplicationFactory<Program> CreateFactory(Action<IServiceCollection>? configureServices = null, string? apiKey = ApiKey) =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ReadApi:PostgresConnectionString"] = ReaderConnectionString,
                ["TELEMETRY_API_KEY"] = apiKey,
            }));
            if (configureServices is not null)
            {
                b.ConfigureTestServices(configureServices);
            }
        });

    private static HttpClient Client(WebApplicationFactory<Program> factory, string? key = ApiKey)
    {
        var client = factory.CreateClient();
        if (key is not null)
        {
            client.DefaultRequestHeaders.Add("X-Api-Key", key);
        }

        return client;
    }

    private async Task ExecAsync(string sql, params (string Name, object Value)[] args)
    {
        await using var conn = new NpgsqlConnection(_stack.AdminConnectionString);
        await conn.OpenAsync();
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var (name, value) in args)
        {
            cmd.Parameters.AddWithValue(name, value);
        }

        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedAlertsAsync(string vehicleId, int count) => ExecAsync(
        "INSERT INTO alerts (alert_id, vehicle_id, type, raised_utc, payload) " +
        "SELECT gen_random_uuid(), @v, 'SPEEDING', @base - (g || ' seconds')::interval, '{\"speedKmh\":130}'::jsonb " +
        "FROM generate_series(1, @n) g",
        ("v", vehicleId), ("base", Base), ("n", count));

    [Fact]
    public async Task State_ReturnsSeededVehicle()
    {
        var id = TelemetryStackFixture.UniqueName("rd");
        await ExecAsync(
            "INSERT INTO vehicle_state (vehicle_id, last_seq, last_event_utc, lat, lon, speed_kmh, ignition, distance_km_today, speeding, updated_utc) " +
            "VALUES (@id, 42, @t, -23.5, -46.6, 88.5, true, 12.25, false, @t)",
            ("id", id), ("t", Base));
        using var factory = CreateFactory();
        var client = Client(factory);

        var response = await client.GetAsync($"/vehicles/{id}/state");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Headers.GetValues("X-Content-Type-Options").Should().Contain("nosniff");
        response.Headers.CacheControl!.NoStore.Should().BeTrue();
        var json = await response.Content.ReadFromJsonAsync<JsonElement>();
        json.GetProperty("vehicleId").GetString().Should().Be(id);
        json.GetProperty("lastSeq").GetInt64().Should().Be(42);
        json.GetProperty("lastEventUtc").GetDateTimeOffset().Should().Be(new DateTimeOffset(Base));
        json.GetProperty("lat").GetDouble().Should().Be(-23.5);
        json.GetProperty("lon").GetDouble().Should().Be(-46.6);
        json.GetProperty("speedKmh").GetDouble().Should().Be(88.5);
        json.GetProperty("ignition").GetBoolean().Should().BeTrue();
        json.GetProperty("distanceKmToday").GetDouble().Should().Be(12.25);
        json.GetProperty("speeding").GetBoolean().Should().BeFalse();
        json.TryGetProperty("updatedUtc", out _).Should().BeTrue();
        json.TryGetProperty("overspeedStreak", out _).Should().BeFalse();
    }

    [Fact]
    public async Task State_Unknown_404()
    {
        using var factory = CreateFactory();
        var response = await Client(factory).GetAsync("/vehicles/" + TelemetryStackFixture.UniqueName("none") + "/state");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task Alerts_FilterAndPaging()
    {
        var id = TelemetryStackFixture.UniqueName("al");
        var other = TelemetryStackFixture.UniqueName("al");
        await SeedAlertsAsync(id, 120);
        await SeedAlertsAsync(other, 5);
        using var factory = CreateFactory();
        var client = Client(factory);

        var page1 = await client.GetFromJsonAsync<JsonElement>($"/alerts?vehicleId={id}&pageSize=100");
        page1.GetProperty("items").GetArrayLength().Should().Be(100);
        page1.GetProperty("hasMore").GetBoolean().Should().BeTrue();
        page1.GetProperty("page").GetInt32().Should().Be(1);
        page1.GetProperty("pageSize").GetInt32().Should().Be(100);
        var first = page1.GetProperty("items")[0];
        first.GetProperty("vehicleId").GetString().Should().Be(id);
        first.GetProperty("type").GetString().Should().Be("SPEEDING");
        first.GetProperty("payload").GetProperty("speedKmh").GetInt32().Should().Be(130);
        first.GetProperty("raisedUtc").GetDateTimeOffset().Should().Be(new DateTimeOffset(Base.AddSeconds(-1)));

        var page2 = await client.GetFromJsonAsync<JsonElement>($"/alerts?vehicleId={id}&pageSize=100&page=2");
        page2.GetProperty("items").GetArrayLength().Should().Be(20);
        page2.GetProperty("hasMore").GetBoolean().Should().BeFalse();

        var from = Base.AddSeconds(-30).ToString("O");
        var to = Base.ToString("O");
        var ranged = await client.GetFromJsonAsync<JsonElement>(
            $"/alerts?vehicleId={id}&from={Uri.EscapeDataString(from)}&to={Uri.EscapeDataString(to)}");
        ranged.GetProperty("items").GetArrayLength().Should().Be(30);
        ranged.GetProperty("hasMore").GetBoolean().Should().BeFalse();
    }

    [Fact]
    public async Task Alerts_PageSizeOver100_400()
    {
        using var factory = CreateFactory();
        var response = await Client(factory).GetAsync("/alerts?pageSize=10000");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await response.Content.ReadAsStringAsync()).Should().Contain("pageSize");
    }

    [Fact]
    public async Task Alerts_FromAfterTo_400()
    {
        using var factory = CreateFactory();
        var response = await Client(factory).GetAsync("/alerts?from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task Alerts_RangeOver31Days_400()
    {
        using var factory = CreateFactory();
        var response = await Client(factory).GetAsync("/alerts?from=2026-01-01T00:00:00Z&to=2026-03-01T00:00:00Z");
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }

    [Fact]
    public async Task MaliciousVehicleId_400()
    {
        using var factory = CreateFactory();
        var client = Client(factory);
        var encoded = Uri.EscapeDataString("' OR 1=1--");

        var alerts = await client.GetAsync($"/alerts?vehicleId={encoded}");
        var state = await client.GetAsync($"/vehicles/{encoded}/state");

        alerts.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        state.StatusCode.Should().Be(HttpStatusCode.BadRequest);
        (await alerts.Content.ReadAsStringAsync()).Should().NotContain("OR 1=1");
    }

    [Fact]
    public async Task MissingKey_401()
    {
        using var factory = CreateFactory();
        var response = await Client(factory, key: null).GetAsync("/alerts");
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task WrongKey_401()
    {
        using var factory = CreateFactory();
        var response = await Client(factory, key: "wrong-key-wrong-key-1").GetAsync("/alerts");
        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Health_NoKeyNeeded()
    {
        using var factory = CreateFactory();
        var client = Client(factory, key: null);
        (await client.GetAsync("/health/live")).StatusCode.Should().Be(HttpStatusCode.OK);
        (await client.GetAsync("/health/ready")).StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("short-key")]
    public void ShortOrMissingKey_RefusesToStart(string? key)
    {
        using var factory = CreateFactory(apiKey: key);
        var act = () => factory.CreateClient();
        act.Should().Throw<Exception>();
    }

    [Fact]
    public async Task RateLimit_Exceeded_429()
    {
        using var factory = CreateFactory();
        var client = Client(factory);
        var id = TelemetryStackFixture.UniqueName("rl");

        for (var i = 0; i < 100; i++)
        {
            (await client.GetAsync($"/vehicles/{id}/state")).StatusCode.Should().Be(HttpStatusCode.NotFound);
        }

        var limited = await client.GetAsync($"/vehicles/{id}/state");
        limited.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        limited.Headers.Contains("Retry-After").Should().BeTrue();
    }

    [Fact]
    public async Task UnhandledError_ReturnsGeneric500WithoutStackTrace()
    {
        using var factory = CreateFactory(services => services.AddTransient<IStartupFilter, ThrowingStartupFilter>());
        var response = await Client(factory).GetAsync("/__test/throw");
        var body = await response.Content.ReadAsStringAsync();

        response.StatusCode.Should().Be(HttpStatusCode.InternalServerError);
        body.Should().Contain("An unexpected error occurred.");
        body.Should().NotContain("secret detail");
        body.Should().NotContain("at ");
        body.Should().NotContain("InvalidOperationException");
    }

    /// <summary>Test-only: registered after the production filters, so it sits inside the exception handler.</summary>
    private sealed class ThrowingStartupFilter : IStartupFilter
    {
        public Action<IApplicationBuilder> Configure(Action<IApplicationBuilder> next) => app =>
        {
            app.Use((context, nextMiddleware) =>
                context.Request.Path == "/__test/throw"
                    ? throw new InvalidOperationException("secret detail")
                    : nextMiddleware(context));
            next(app);
        };
    }
}
