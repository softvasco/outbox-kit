namespace OutboxKit;

/// <summary>Turns an application message into an <see cref="OutboxMessage"/> ready to be saved.</summary>
public sealed class OutboxMessageFactory
{
    private readonly MessageTypeRegistry _registry;
    private readonly IMessageSerializer _serializer;
    private readonly TimeProvider _timeProvider;

    public OutboxMessageFactory(MessageTypeRegistry registry, IMessageSerializer serializer, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _registry = registry;
        _serializer = serializer;
        _timeProvider = timeProvider;
    }

    public OutboxMessage Create(object message, string? orderingKey = null)
    {
        ArgumentNullException.ThrowIfNull(message);

        // the runtime type, so a message passed as a base type or interface still finds its registration
        var type = message.GetType();
        var name = _registry.GetName(type);
        var createdAt = _timeProvider.GetUtcNow();

        // version 7 ids sort by creation time, which keeps inserts at the end of the primary key index
        return new OutboxMessage(Guid.CreateVersion7(createdAt), name, _serializer.Serialize(message, type), createdAt, orderingKey);
    }
}
