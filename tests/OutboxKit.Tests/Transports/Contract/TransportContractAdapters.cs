using System.Text;
using OutboxKit.AzureServiceBus;
using OutboxKit.RabbitMQ;
using OutboxKit.Transports;

namespace OutboxKit.Tests.Transports.Contract;

public sealed class InMemoryTransportContract : TransportContract
{
    private readonly InMemoryTransport _transport = new();

    protected override bool CanFailSends => false;

    protected override Task<IMessageTransport> CreateTransportAsync() => Task.FromResult<IMessageTransport>(_transport);

    protected override Task<IMessageTransport> CreateTransportForMissingDestinationAsync() =>
        throw new NotSupportedException("The in-memory transport has no destination to miss.");

    protected override Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(IReadOnlyCollection<Guid> ids, Guid? stopAfter = null)
    {
        var received = new List<ReceivedMessage>();
        while (_transport.Sent.TryRead(out var message))
        {
            if (ids.Contains(message.Id))
            {
                received.Add(new ReceivedMessage(message.Id.ToString(), message.Type, message.Payload, message.OrderingKey));
            }
        }

        return Task.FromResult<IReadOnlyList<ReceivedMessage>>(received);
    }
}

public sealed class RabbitMqTransportContract(RabbitMqBroker broker) : TransportContract, IClassFixture<RabbitMqBroker>
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(10);

    private string? _queue;

    protected override async Task<IMessageTransport> CreateTransportAsync()
    {
        await using var channel = await broker.Connection.CreateChannelAsync(cancellationToken: Token);
        var declared = await channel.QueueDeclareAsync($"contract-{Guid.NewGuid():N}", durable: true, exclusive: false, autoDelete: false, cancellationToken: Token);
        var queue = declared.QueueName;
        _queue = queue;
        return new RabbitMqTransport(broker.Connection, new RabbitMqTransportOptions { RoutingKey = _ => queue });
    }

    protected override Task<IMessageTransport> CreateTransportForMissingDestinationAsync() =>
        Task.FromResult<IMessageTransport>(new RabbitMqTransport(broker.Connection, new RabbitMqTransportOptions { Exchange = $"missing-{Guid.NewGuid():N}" }));

    protected override async Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(IReadOnlyCollection<Guid> ids, Guid? stopAfter = null)
    {
        await using var channel = await broker.Connection.CreateChannelAsync(cancellationToken: Token);
        var wanted = ids.Select(id => id.ToString()).ToHashSet();
        var stop = stopAfter?.ToString();
        var received = new List<ReceivedMessage>();
        var deadline = DateTime.UtcNow + ReceiveTimeout;

        // basic.get returns null on an empty queue instead of waiting, so poll until the deadline
        while (DateTime.UtcNow < deadline && received.Count < wanted.Count && (stop is null || received.LastOrDefault()?.Id != stop))
        {
            var result = await channel.BasicGetAsync(_queue!, autoAck: true, Token);
            if (result is null)
            {
                await Task.Delay(50, Token);
                continue;
            }

            var properties = result.BasicProperties;
            if (wanted.Contains(properties.MessageId!))
            {
                var orderingKey = properties.Headers?.TryGetValue(RabbitMqTransport.OrderingKeyHeader, out var key) == true
                    ? Encoding.UTF8.GetString((byte[])key!)
                    : null;
                received.Add(new ReceivedMessage(properties.MessageId!, properties.Type!, Encoding.UTF8.GetString(result.Body.Span), orderingKey));
            }
        }

        return received;
    }
}

[Collection(SharedServiceBusEmulator.Name)]
public sealed class ServiceBusTransportContract(ServiceBusEmulator emulator) : TransportContract
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(10);

    protected override Task<IMessageTransport> CreateTransportAsync() =>
        Task.FromResult<IMessageTransport>(new ServiceBusTransport(emulator.Client, new ServiceBusTransportOptions { EntityName = ServiceBusEmulator.ContractQueue }));

    protected override Task<IMessageTransport> CreateTransportForMissingDestinationAsync() =>
        Task.FromResult<IMessageTransport>(new ServiceBusTransport(
            emulator.Client,
            new ServiceBusTransportOptions { EntityName = "no-such-queue", SendTimeout = TimeSpan.FromSeconds(5) }));

    protected override async Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(IReadOnlyCollection<Guid> ids, Guid? stopAfter = null)
    {
        await using var receiver = emulator.Client.CreateReceiver(ServiceBusEmulator.ContractQueue);
        var wanted = ids.Select(id => id.ToString()).ToHashSet();
        var stop = stopAfter?.ToString();
        var received = new List<ReceivedMessage>();

        while (received.Count < wanted.Count && (stop is null || received.LastOrDefault()?.Id != stop)
            && await receiver.ReceiveMessageAsync(ReceiveTimeout, Token) is { } message)
        {
            await receiver.CompleteMessageAsync(message, Token);
            if (wanted.Contains(message.MessageId))
            {
                var orderingKey = message.ApplicationProperties.TryGetValue(ServiceBusTransport.OrderingKeyProperty, out var key) ? (string)key : null;
                received.Add(new ReceivedMessage(message.MessageId, message.Subject, message.Body.ToString(), orderingKey));
            }
        }

        return received;
    }
}
