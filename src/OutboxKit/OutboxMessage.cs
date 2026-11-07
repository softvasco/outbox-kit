namespace OutboxKit;

/// <summary>A message waiting in the outbox, stored with the transaction that produced it.</summary>
public sealed class OutboxMessage
{
    public const int MaxTypeLength = 256;
    public const int MaxOrderingKeyLength = 256;

    public OutboxMessage(Guid id, string type, string payload, DateTimeOffset createdAt, string? orderingKey = null, int attempts = 0)
    {
        if (id == Guid.Empty)
        {
            throw new ArgumentException("Message id can't be empty.", nameof(id));
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(type.Length, MaxTypeLength, nameof(type));
        ArgumentNullException.ThrowIfNull(payload);

        if (orderingKey is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(orderingKey);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(orderingKey.Length, MaxOrderingKeyLength, nameof(orderingKey));
        }

        ArgumentOutOfRangeException.ThrowIfNegative(attempts);

        Id = id;
        Type = type;
        Payload = payload;
        CreatedAt = createdAt;
        OrderingKey = orderingKey;
        Attempts = attempts;
    }

    public Guid Id { get; }

    /// <summary>The registered name of the message type, not the CLR type name.</summary>
    public string Type { get; }

    public string Payload { get; }

    public DateTimeOffset CreatedAt { get; }

    /// <summary>Messages with the same key are delivered in the order they were saved. Null means no ordering.</summary>
    public string? OrderingKey { get; }

    /// <summary>How many sends of this message have failed so far.</summary>
    public int Attempts { get; }
}
