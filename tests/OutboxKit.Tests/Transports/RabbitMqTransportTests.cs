using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OutboxKit.RabbitMQ;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Testcontainers.RabbitMq;

namespace OutboxKit.Tests.Transports;

public sealed class RabbitMqBroker : IAsyncLifetime
{
    private readonly RabbitMqContainer _container = new RabbitMqBuilder("rabbitmq:4-alpine").Build();

    public IConnection Connection { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        var factory = new ConnectionFactory { Uri = new Uri(_container.GetConnectionString()) };
        Connection = await factory.CreateConnectionAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await Connection.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public sealed class RabbitMqTransportTests(RabbitMqBroker broker) : IClassFixture<RabbitMqBroker>
{
    private static readonly DateTimeOffset Now = new(2026, 11, 18, 22, 0, 0, TimeSpan.Zero);

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_sent_message_is_in_the_queue_as_persistent_with_its_id_and_type()
    {
        await using var channel = await broker.Connection.CreateChannelAsync(cancellationToken: Token);
        var queue = await DeclareQueueAsync(channel);
        await using var transport = new RabbitMqTransport(broker.Connection, new RabbitMqTransportOptions { RoutingKey = _ => queue });
        var message = new OutboxMessage(Guid.CreateVersion7(), "orders.placed", """{"orderId":42}""", Now, orderingKey: "order-42");

        await transport.SendAsync(message, Token);

        var received = await channel.BasicGetAsync(queue, autoAck: true, Token);
        Assert.NotNull(received);
        Assert.Equal(message.Id.ToString(), received.BasicProperties.MessageId);
        Assert.Equal("orders.placed", received.BasicProperties.Type);
        Assert.Equal("application/json", received.BasicProperties.ContentType);
        Assert.True(received.BasicProperties.Persistent);
        Assert.Equal(Now.ToUnixTimeSeconds(), received.BasicProperties.Timestamp.UnixTime);
        Assert.Equal("order-42", Encoding.UTF8.GetString((byte[])received.BasicProperties.Headers![RabbitMqTransport.OrderingKeyHeader]!));
        Assert.Equal(message.Payload, Encoding.UTF8.GetString(received.Body.Span));
    }

    [Fact]
    public async Task Messages_reach_a_topic_exchange_routed_by_their_type()
    {
        await using var channel = await broker.Connection.CreateChannelAsync(cancellationToken: Token);
        var exchange = await DeclareTopicExchangeAsync(channel);
        var orders = await DeclareQueueAsync(channel);
        await channel.QueueBindAsync(orders, exchange, "orders.*", cancellationToken: Token);
        await using var transport = new RabbitMqTransport(broker.Connection, new RabbitMqTransportOptions { Exchange = exchange });

        await transport.SendAsync(Message("orders.placed"), Token);
        await transport.SendAsync(Message("orders.cancelled"), Token);

        var first = await channel.BasicGetAsync(orders, autoAck: true, Token);
        var second = await channel.BasicGetAsync(orders, autoAck: true, Token);
        Assert.Equal(["orders.placed", "orders.cancelled"], [first?.BasicProperties.Type, second?.BasicProperties.Type]);
    }

    [Fact]
    public async Task A_message_no_queue_is_bound_for_fails_the_send()
    {
        await using var channel = await broker.Connection.CreateChannelAsync(cancellationToken: Token);
        var exchange = await DeclareTopicExchangeAsync(channel);
        await using var transport = new RabbitMqTransport(broker.Connection, new RabbitMqTransportOptions { Exchange = exchange });

        var failure = await Assert.ThrowsAsync<PublishReturnException>(() => transport.SendAsync(Message("orders.placed"), Token));

        Assert.Equal(312, failure.ReplyCode);
    }

    [Fact]
    public async Task After_the_exchange_comes_back_the_next_send_goes_through()
    {
        await using var channel = await broker.Connection.CreateChannelAsync(cancellationToken: Token);
        var exchange = await DeclareTopicExchangeAsync(channel);
        var queue = await DeclareQueueAsync(channel);
        await channel.QueueBindAsync(queue, exchange, "#", cancellationToken: Token);
        await using var transport = new RabbitMqTransport(broker.Connection, new RabbitMqTransportOptions { Exchange = exchange });

        await channel.ExchangeDeleteAsync(exchange, cancellationToken: Token);
        await Assert.ThrowsAnyAsync<Exception>(() => transport.SendAsync(Message("orders.placed"), Token));

        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic, cancellationToken: Token);
        await channel.QueueBindAsync(queue, exchange, "#", cancellationToken: Token);
        await transport.SendAsync(Message("orders.placed"), Token);

        Assert.Equal(1u, await channel.MessageCountAsync(queue, Token));
    }

    [Fact]
    public void A_confirm_timeout_of_zero_is_rejected()
    {
        var options = new RabbitMqTransportOptions { ConfirmTimeout = TimeSpan.Zero };

        Assert.Throws<ArgumentOutOfRangeException>(() => new RabbitMqTransport(broker.Connection, options));
    }

    [Fact]
    public async Task The_registered_transport_publishes_on_the_connection_from_the_container()
    {
        await using var channel = await broker.Connection.CreateChannelAsync(cancellationToken: Token);
        var exchange = await DeclareTopicExchangeAsync(channel);
        var queue = await DeclareQueueAsync(channel);
        await channel.QueueBindAsync(queue, exchange, "#", cancellationToken: Token);
        await using var provider = new ServiceCollection()
            .AddSingleton(broker.Connection)
            .AddRabbitMqTransport(o => o.Exchange = exchange)
            .BuildServiceProvider(validateScopes: true);

        await provider.GetRequiredService<IMessageTransport>().SendAsync(Message("orders.placed"), Token);

        Assert.Equal(1u, await channel.MessageCountAsync(queue, Token));
    }

    [Fact]
    public void Bad_registered_options_fail_when_resolved()
    {
        using var provider = new ServiceCollection()
            .AddSingleton(broker.Connection)
            .AddRabbitMqTransport(o => o.ConfirmTimeout = TimeSpan.Zero)
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IMessageTransport>());
    }

    private static OutboxMessage Message(string type) => new(Guid.CreateVersion7(), type, "{}", Now);

    private static async Task<string> DeclareQueueAsync(IChannel channel)
    {
        var declared = await channel.QueueDeclareAsync($"outbox-{Guid.NewGuid():N}", durable: true, exclusive: false, autoDelete: false, cancellationToken: Token);
        return declared.QueueName;
    }

    private static async Task<string> DeclareTopicExchangeAsync(IChannel channel)
    {
        var exchange = $"outbox-{Guid.NewGuid():N}";
        await channel.ExchangeDeclareAsync(exchange, ExchangeType.Topic, cancellationToken: Token);
        return exchange;
    }
}
