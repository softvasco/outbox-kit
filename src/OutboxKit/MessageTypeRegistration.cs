namespace OutboxKit;

public sealed record MessageTypeRegistration(Type ClrType, string Name)
{
    public static MessageTypeRegistration For<T>(string name) => new(typeof(T), name);
}
