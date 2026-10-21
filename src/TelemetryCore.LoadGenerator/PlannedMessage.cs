namespace TelemetryCore.LoadGenerator;

public enum MessageKind { Valid, Duplicate, Invalid }

public readonly record struct PlannedMessage(string Key, byte[] Payload, MessageKind Kind);
