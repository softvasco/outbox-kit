namespace OutboxKit.Inbox;

/// <summary>A message a consumer gave up on, kept with its error until replayed.</summary>
public sealed class ParkedMessage
{
    public const int MaxIdLength = 256;

    public ParkedMessage(string messageId, string consumer, string type, string payload, string error, DateTimeOffset parkedAt)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(messageId.Length, MaxIdLength, nameof(messageId));
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(consumer.Length, MaxIdLength, nameof(consumer));
        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(type.Length, OutboxMessage.MaxTypeLength, nameof(type));
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        MessageId = messageId;
        Consumer = consumer;
        Type = type;
        Payload = payload;
        Error = error;
        ParkedAt = parkedAt;
    }

    public string MessageId { get; }

    public string Consumer { get; }

    /// <summary>The type as it came off the broker, so a replay can pick the handler.</summary>
    public string Type { get; }

    public string Payload { get; }

    /// <summary>Why the last attempt failed, usually the exception message.</summary>
    public string Error { get; }

    public DateTimeOffset ParkedAt { get; }
}
