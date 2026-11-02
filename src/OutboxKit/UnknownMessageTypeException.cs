namespace OutboxKit;

public sealed class UnknownMessageTypeException : Exception
{
    public UnknownMessageTypeException()
    {
    }

    public UnknownMessageTypeException(string message)
        : base(message)
    {
    }

    public UnknownMessageTypeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public static UnknownMessageTypeException For(Type type) =>
        new($"{type.FullName} is not registered as a message type.");

    public static UnknownMessageTypeException For(string name) =>
        new($"No message type is registered under the name '{name}'.");
}
