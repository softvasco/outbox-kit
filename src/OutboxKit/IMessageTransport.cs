namespace OutboxKit;

/// <summary>Sends a message to wherever consumers read it from: a broker, a queue, an HTTP endpoint.</summary>
public interface IMessageTransport
{
    Task SendAsync(OutboxMessage message, CancellationToken cancellationToken);
}
