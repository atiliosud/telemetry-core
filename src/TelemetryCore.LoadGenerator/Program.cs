using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Trace;
using TelemetryCore.Contracts;
using TelemetryCore.LoadGenerator;

GeneratorOptions options;
try
{
    options = GeneratorOptions.Parse(args);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 2;
}

// Spans need a listener to exist. With OTEL_EXPORTER_OTLP_ENDPOINT set (e.g. http://jaeger:4317) the chunk spans are
// exported, so a trace shows generator -> worker (the worker's batch spans link to them); otherwise a bare listener is
// enough to get the W3C context for the traceparent header. Disposing the provider flushes the last spans.
const string SourceName = "TelemetryCore.LoadGenerator";
using var tracerProvider = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT"))
    ? null
    : Sdk.CreateTracerProviderBuilder().AddSource(SourceName).AddOtlpExporter().Build();
using var listener = new ActivityListener
{
    ShouldListenTo = s => tracerProvider is null && s.Name == SourceName,
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
};
ActivitySource.AddActivityListener(listener);
using var source = new ActivitySource(SourceName);

var config = new ProducerConfig
{
    BootstrapServers = options.Bootstrap,
    Acks = Acks.All,
    EnableIdempotence = true,
    LingerMs = 5,
    BatchSize = 256 * 1024,
    CompressionType = CompressionType.Lz4,
};

long delivered = 0;
long failed = 0;
string? firstError = null;

using var producer = new ProducerBuilder<string, byte[]>(config).Build();
EventPlanner planner;
try
{
    planner = new EventPlanner(options, TimeProvider.System);
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 2;
}

var clock = Stopwatch.StartNew();
Activity? span = null;
string? traceparent = null;
long n = 0;

void OnDelivery(DeliveryReport<string, byte[]> r)
{
    if (r.Error.IsError)
    {
        Interlocked.Increment(ref failed);
        Interlocked.CompareExchange(ref firstError, r.Error.Reason, null);
    }
    else
    {
        Interlocked.Increment(ref delivered);
    }
}

try
{
    foreach (var msg in planner.Plan())
    {
        if (n % 1000 == 0)
        {
            span?.Dispose();
            span = source.StartActivity("publish-chunk", ActivityKind.Producer);
            span?.SetTag("chunk.start", n);
            traceparent = span?.Id;
        }
        n++;

        var m = new Message<string, byte[]> { Key = msg.Key, Value = msg.Payload };
        if (traceparent is not null)
        {
            m.Headers = new Headers { { TelemetryHeaders.TraceParent, Encoding.ASCII.GetBytes(traceparent) } };
        }

        while (true)
        {
            try
            {
                producer.Produce(options.Topic, m, OnDelivery);
                break;
            }
            catch (ProduceException<string, byte[]> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
            {
                producer.Poll(TimeSpan.FromMilliseconds(50));
            }
        }

        producer.Poll(TimeSpan.Zero);

        if (options.Rate > 0)
        {
            var wait = TimeSpan.FromSeconds((double)n / options.Rate) - clock.Elapsed;
            if (wait > TimeSpan.FromMilliseconds(2)) Thread.Sleep(wait);
        }
    }
}
catch (ArgumentException ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 2;
}

span?.Dispose();
producer.Flush(TimeSpan.FromSeconds(60));
clock.Stop();

var p = planner.Summary;
var seconds = clock.Elapsed.TotalSeconds;
var summary = p with
{
    Sent = delivered,
    ExpectedUnique = delivered - p.DuplicatesInjected - p.InvalidInjected,
    Seconds = Math.Round(seconds, 3),
    EventsPerSecond = seconds > 0 ? Math.Round(delivered / seconds, 1) : 0,
};

Console.WriteLine("+----------------------+------------+");
Console.WriteLine($"| {"Sent",-20} | {summary.Sent,10} |");
Console.WriteLine($"| {"Duplicates injected",-20} | {summary.DuplicatesInjected,10} |");
Console.WriteLine($"| {"Invalid injected",-20} | {summary.InvalidInjected,10} |");
Console.WriteLine($"| {"Out-of-order",-20} | {summary.OutOfOrderInjected,10} |");
Console.WriteLine($"| {"Expected unique",-20} | {summary.ExpectedUnique,10} |");
Console.WriteLine($"| {"Vehicles speeding",-20} | {summary.VehiclesSpeeding,10} |");
Console.WriteLine($"| {"Seconds",-20} | {summary.Seconds,10:F3} |");
Console.WriteLine($"| {"Events/s",-20} | {summary.EventsPerSecond,10:F1} |");
Console.WriteLine("+----------------------+------------+");
Console.WriteLine("SUMMARY " + JsonSerializer.Serialize(summary, SummaryJson.Options));

if (failed > 0 || delivered != options.Total)
{
    Console.Error.WriteLine($"error: {failed} delivery failures, {delivered}/{options.Total} delivered. First error: {firstError}");
    return 1;
}

return 0;

internal static class SummaryJson
{
    public static readonly JsonSerializerOptions Options = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
}
