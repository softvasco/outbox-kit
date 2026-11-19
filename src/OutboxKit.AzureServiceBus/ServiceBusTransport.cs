using Azure.Messaging.ServiceBus;

namespace OutboxKit.AzureServiceBus;

/// <summary>
/// Sends outbox messages to an Azure Service Bus queue or topic. The outbox id goes out as the MessageId, so an entity
/// with duplicate detection on drops a message the relay sent twice.
/// </summary>
public sealed class ServiceBusTransport : IMessageTransport, IAsyncDisposable
{
    public const string OrderingKeyProperty = "outbox-ordering-key";

    // the broker's limit, lower than the outbox allows for an ordering key
    public const int MaxSessionIdLength = 128;

    private readonly ServiceBusSender _sender;
    private readonly ServiceBusTransportOptions _options;

    // the client stays the caller's, so one connection can serve the transport and the processors
    public ServiceBusTransport(ServiceBusClient client, ServiceBusTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);

        options.Validate();
        _options = options;
        _sender = client.CreateSender(options.EntityName);
    }

    public async Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        var outgoing = ToServiceBusMessage(message);

        // the client retries on its own for up to a few minutes by default, which is far longer than a claim lasts
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_options.SendTimeout);
        await _sender.SendMessageAsync(outgoing, timeout.Token);
    }

    public ValueTask DisposeAsync() => _sender.DisposeAsync();

    private ServiceBusMessage ToServiceBusMessage(OutboxMessage message)
    {
        var outgoing = new ServiceBusMessage(BinaryData.FromString(message.Payload))
        {
            MessageId = message.Id.ToString(),
            Subject = message.Type,
            ContentType = _options.ContentType,
        };

        if (message.OrderingKey is null)
        {
            return outgoing;
        }

        outgoing.ApplicationProperties[OrderingKeyProperty] = message.OrderingKey;
        if (_options.OrderingKeyAsSessionId)
        {
            if (message.OrderingKey.Length > MaxSessionIdLength)
            {
                throw new ArgumentException(
                    $"Ordering key '{message.OrderingKey}' is longer than the {MaxSessionIdLength} characters a session id allows.",
                    nameof(message));
            }

            outgoing.SessionId = message.OrderingKey;
        }

        return outgoing;
    }
}
