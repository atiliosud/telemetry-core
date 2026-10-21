using System.Text.Json.Serialization;

namespace TelemetryCore.Contracts;

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(TelemetryEventDto))]
[JsonSerializable(typeof(SpeedingAlertMessage))]
public partial class TelemetryJsonContext : JsonSerializerContext
{
}
