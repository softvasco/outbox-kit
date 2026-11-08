using Microsoft.Extensions.Time.Testing;
using OutboxKit.Transports;

namespace OutboxKit.Tests.Transports;

public class InMemoryTransportTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 8, 21, 0, 0, TimeSpan.Zero);

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Sent_messages_are_read_back_in_the_order_they_went_out()
    {
        var transport = new InMemoryTransport();
        var first = Message();
        var second = Message();

        await transport.SendAsync(first, Token);
        await transport.SendAsync(second, Token);

        Assert.Equal([first, second], ReadAvailable(transport));
    }

    [Fact]
    public async Task A_reader_waiting_for_a_message_gets_it_when_it_is_sent()
    {
        var transport = new InMemoryTransport();
        var message = Message();

        var waiting = transport.Sent.ReadAsync(Token).AsTask();
        await transport.SendAsync(message, Token);

        Assert.Same(message, await waiting);
    }

    [Fact]
    public async Task A_cancelled_send_keeps_nothing()
    {
        var transport = new InMemoryTransport();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.SendAsync(Message(), new CancellationToken(canceled: true)));

        Assert.Empty(ReadAvailable(transport));
    }

    [Fact]
    public async Task The_dispatcher_delivers_a_claimed_batch_to_it()
    {
        var store = new SingleBatchStore([Message(), Message()]);
        var transport = new InMemoryTransport();
        var dispatcher = new OutboxDispatcher(store, transport, new FakeTimeProvider(Now));

        var sent = await dispatcher.DispatchBatchAsync(10, TimeSpan.FromSeconds(30), Token);

        Assert.Equal(2, sent);
        Assert.Equal(store.Batch, ReadAvailable(transport));
    }

    private static OutboxMessage Message() => new(Guid.CreateVersion7(), "orders.placed", "{}", Now);

    private static List<OutboxMessage> ReadAvailable(InMemoryTransport transport)
    {
        var messages = new List<OutboxMessage>();
        while (transport.Sent.TryRead(out var message))
        {
            messages.Add(message);
        }

        return messages;
    }

    private sealed class SingleBatchStore(IReadOnlyList<OutboxMessage> batch) : IOutboxStore
    {
        public IReadOnlyList<OutboxMessage> Batch { get; } = batch;

        public Task<IReadOnlyList<OutboxMessage>> ClaimAsync(int batchSize, DateTimeOffset now, DateTimeOffset claimedUntil, CancellationToken cancellationToken) =>
            Task.FromResult(Batch);

        public Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task MarkFailedAsync(OutboxMessage message, DateTimeOffset retryAt, string reason, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task MarkDeadLetteredAsync(OutboxMessage message, DateTimeOffset deadLetteredAt, string reason, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
