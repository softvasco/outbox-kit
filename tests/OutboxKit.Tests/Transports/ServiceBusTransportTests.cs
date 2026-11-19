using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OutboxKit.AzureServiceBus;
using Testcontainers.ServiceBus;

namespace OutboxKit.Tests.Transports;

public sealed class ServiceBusEmulator : IAsyncLifetime
{
    public const string Queue = "outbox";
    public const string SessionQueue = "outbox-sessions";
    public const string ContractQueue = "outbox-contract";
    public const string Topic = "outbox-events";
    public const string Subscription = "all";

    private readonly ServiceBusContainer _container = new ServiceBusBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
        .WithAcceptLicenseAgreement(true)
        .WithConfig(ConfigFile())
        .Build();

    public ServiceBusClient Client { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        Client = new ServiceBusClient(_container.GetConnectionString());
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DisposeAsync();
        await _container.DisposeAsync();
    }

    private static string ConfigFile([CallerFilePath] string testFile = "") =>
        Path.Join(Path.GetDirectoryName(testFile)!, "servicebus-config.json");
}

// the emulator takes a while to start, so every Service Bus test class shares one
[CollectionDefinition(Name)]
public sealed class SharedServiceBusEmulator : ICollectionFixture<ServiceBusEmulator>
{
    public const string Name = "Service Bus emulator";
}

[Collection(SharedServiceBusEmulator.Name)]
public sealed class ServiceBusTransportTests(ServiceBusEmulator emulator)
{
    private static readonly DateTimeOffset Now = new(2026, 11, 19, 20, 0, 0, TimeSpan.Zero);

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_sent_message_is_in_the_queue_with_its_id_type_and_ordering_key()
    {
        await using var transport = Transport(ServiceBusEmulator.Queue);
        var message = new OutboxMessage(Guid.CreateVersion7(), "orders.placed", """{"orderId":42}""", Now, orderingKey: "order-42");

        await transport.SendAsync(message, Token);

        await using var receiver = emulator.Client.CreateReceiver(ServiceBusEmulator.Queue);
        var received = await ReceiveAsync(receiver, message.Id);
        Assert.Equal("orders.placed", received.Subject);
        Assert.Equal("application/json", received.ContentType);
        Assert.Equal("order-42", received.ApplicationProperties[ServiceBusTransport.OrderingKeyProperty]);
        Assert.Null(received.SessionId);
        Assert.Equal(message.Payload, received.Body.ToString());
    }

    [Fact]
    public async Task A_message_sent_to_a_topic_reaches_its_subscription()
    {
        await using var transport = Transport(ServiceBusEmulator.Topic);
        var message = Message("orders.cancelled");

        await transport.SendAsync(message, Token);

        await using var receiver = emulator.Client.CreateReceiver(ServiceBusEmulator.Topic, ServiceBusEmulator.Subscription);
        var received = await ReceiveAsync(receiver, message.Id);
        Assert.Equal("orders.cancelled", received.Subject);
    }

    [Fact]
    public async Task The_ordering_key_becomes_the_session_id_when_asked()
    {
        await using var transport = Transport(ServiceBusEmulator.SessionQueue, sessions: true);
        var first = Message("orders.placed", "order-7");
        var second = Message("orders.cancelled", "order-7");

        await transport.SendAsync(first, Token);
        await transport.SendAsync(second, Token);

        await using var session = await emulator.Client.AcceptSessionAsync(ServiceBusEmulator.SessionQueue, "order-7", cancellationToken: Token);
        var received = await session.ReceiveMessagesAsync(2, TimeSpan.FromSeconds(10), Token);
        Assert.Equal([first.Id.ToString(), second.Id.ToString()], received.Select(m => m.MessageId));
    }

    [Fact]
    public async Task An_ordering_key_too_long_for_a_session_id_fails_the_send()
    {
        await using var transport = Transport(ServiceBusEmulator.SessionQueue, sessions: true);
        var message = Message("orders.placed", new string('k', ServiceBusTransport.MaxSessionIdLength + 1));

        await Assert.ThrowsAsync<ArgumentException>(() => transport.SendAsync(message, Token));
    }

    [Fact]
    public async Task Sending_to_an_entity_that_does_not_exist_fails()
    {
        await using var transport = Transport("no-such-queue");

        await Assert.ThrowsAnyAsync<Exception>(() => transport.SendAsync(Message("orders.placed"), Token));
    }

    [Fact]
    public void The_default_send_timeout_is_well_under_the_default_claim()
    {
        // a send still retrying could land after another relay resent it (docs/delivery.md)
        Assert.True(new ServiceBusTransportOptions().SendTimeout * 2 <= new OutboxDispatcherOptions().ClaimDuration);
    }

    [Theory]
    [InlineData("", 10)]
    [InlineData("outbox", 0)]
    public void Options_without_an_entity_or_a_timeout_are_rejected(string entity, int timeoutSeconds)
    {
        var options = new ServiceBusTransportOptions { EntityName = entity, SendTimeout = TimeSpan.FromSeconds(timeoutSeconds) };

        Assert.ThrowsAny<ArgumentException>(() => new ServiceBusTransport(emulator.Client, options));
    }

    [Fact]
    public async Task The_registered_transport_sends_through_the_client_from_the_container()
    {
        await using var provider = new ServiceCollection()
            .AddSingleton(emulator.Client)
            .AddServiceBusTransport(o => o.EntityName = ServiceBusEmulator.Queue)
            .BuildServiceProvider(validateScopes: true);
        var message = Message("orders.placed");

        await provider.GetRequiredService<IMessageTransport>().SendAsync(message, Token);

        await using var receiver = emulator.Client.CreateReceiver(ServiceBusEmulator.Queue);
        await ReceiveAsync(receiver, message.Id);
    }

    [Fact]
    public void Registered_options_without_an_entity_fail_when_resolved()
    {
        using var provider = new ServiceCollection()
            .AddSingleton(emulator.Client)
            .AddServiceBusTransport(_ => { })
            .BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IMessageTransport>());
    }

    private ServiceBusTransport Transport(string entity, bool sessions = false) => new(
        emulator.Client,
        new ServiceBusTransportOptions { EntityName = entity, OrderingKeyAsSessionId = sessions, SendTimeout = TimeSpan.FromSeconds(5) });

    private static OutboxMessage Message(string type, string? orderingKey = null) => new(Guid.CreateVersion7(), type, "{}", Now, orderingKey);

    // the entities are shared by the whole class, so skip whatever an earlier test left behind
    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, Guid messageId)
    {
        while (await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10), Token) is { } received)
        {
            await receiver.CompleteMessageAsync(received, Token);
            if (received.MessageId == messageId.ToString())
            {
                return received;
            }
        }

        throw new Xunit.Sdk.XunitException($"Message {messageId} never arrived.");
    }
}
