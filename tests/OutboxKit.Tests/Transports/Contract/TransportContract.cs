namespace OutboxKit.Tests.Transports.Contract;

/// <summary>What a consumer reads back, mapped out of the broker's own format.</summary>
public sealed record ReceivedMessage(string Id, string Type, string Payload, string? OrderingKey);

/// <summary>What every transport owes the dispatcher, run by each adapter.</summary>
public abstract class TransportContract : IAsyncDisposable
{
    private static readonly DateTimeOffset Now = new(2026, 11, 19, 21, 0, 0, TimeSpan.Zero);

    private readonly List<IMessageTransport> _created = [];

    protected static CancellationToken Token => TestContext.Current.CancellationToken;

    /// <summary>False for a transport that can't fail a send, like the in-memory one.</summary>
    protected virtual bool CanFailSends => true;

    [Fact]
    public async Task A_message_arrives_with_its_id_type_payload_and_ordering_key()
    {
        var transport = await TransportAsync();
        var message = new OutboxMessage(Guid.CreateVersion7(), "orders.placed", """{"orderId":42}""", Now, orderingKey: "order-42");

        await transport.SendAsync(message, Token);

        var received = Assert.Single(await ReceiveAsync([message.Id]));
        Assert.Equal(new ReceivedMessage(message.Id.ToString(), "orders.placed", message.Payload, "order-42"), received);
    }

    [Fact]
    public async Task Messages_arrive_in_the_order_they_were_sent()
    {
        var transport = await TransportAsync();
        var messages = Enumerable.Range(1, 5).Select(i => Message($"orders.step{i}")).ToList();

        foreach (var message in messages)
        {
            await transport.SendAsync(message, Token);
        }

        var received = await ReceiveAsync([.. messages.Select(m => m.Id)]);
        Assert.Equal(messages.Select(m => m.Id.ToString()), received.Select(m => m.Id));
    }

    [Fact]
    public async Task A_send_with_a_cancelled_token_throws_and_sends_nothing()
    {
        var transport = await TransportAsync();
        var cancelled = Message("orders.placed");
        var next = Message("orders.cancelled");

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.SendAsync(cancelled, new CancellationToken(canceled: true)));
        await transport.SendAsync(next, Token);

        // the queue keeps send order, so had the first one gone out it would come before the second
        var received = await ReceiveAsync([cancelled.Id, next.Id], stopAfter: next.Id);
        Assert.Equal([next.Id.ToString()], received.Select(m => m.Id));
    }

    [Fact]
    public async Task A_send_to_a_destination_that_does_not_exist_throws()
    {
        Assert.SkipUnless(CanFailSends, "This transport never fails a send.");
        var transport = Track(await CreateTransportForMissingDestinationAsync());

        await Assert.ThrowsAnyAsync<Exception>(() => transport.SendAsync(Message("orders.placed"), Token));
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var transport in _created)
        {
            switch (transport)
            {
                case IAsyncDisposable asyncDisposable:
                    await asyncDisposable.DisposeAsync();
                    break;
                case IDisposable disposable:
                    disposable.Dispose();
                    break;
            }
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>A transport whose messages <see cref="ReceiveAsync"/> reads back.</summary>
    protected abstract Task<IMessageTransport> CreateTransportAsync();

    protected abstract Task<IMessageTransport> CreateTransportForMissingDestinationAsync();

    /// <summary>Messages with these ids in arrival order, until all or stopAfter came.</summary>
    protected abstract Task<IReadOnlyList<ReceivedMessage>> ReceiveAsync(IReadOnlyCollection<Guid> ids, Guid? stopAfter = null);

    private static OutboxMessage Message(string type) => new(Guid.CreateVersion7(), type, "{}", Now);

    private async Task<IMessageTransport> TransportAsync() => Track(await CreateTransportAsync());

    private IMessageTransport Track(IMessageTransport transport)
    {
        _created.Add(transport);
        return transport;
    }
}
