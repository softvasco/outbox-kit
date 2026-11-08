using System.Threading.Channels;

namespace OutboxKit.Transports;

/// <summary>
/// Keeps every sent message in memory, in send order, so tests and samples can read them back without a broker.
/// Nothing is ever dropped, so don't use it in production.
/// </summary>
public sealed class InMemoryTransport : IMessageTransport
{
    private readonly Channel<OutboxMessage> _sent = Channel.CreateUnbounded<OutboxMessage>();

    /// <summary>The messages sent so far, read in the order they went out.</summary>
    public ChannelReader<OutboxMessage> Sent => _sent.Reader;

    public Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        return _sent.Writer.WriteAsync(message, cancellationToken).AsTask();
    }
}
