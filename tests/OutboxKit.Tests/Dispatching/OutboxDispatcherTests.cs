using Microsoft.Extensions.Time.Testing;

namespace OutboxKit.Tests.Dispatching;

public sealed class OutboxDispatcherTests
{
    private readonly InMemoryOutboxStore _store = new();
    private readonly RecordingTransport _transport = new();
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 11, 5, 23, 0, 0, TimeSpan.Zero));
    private readonly OutboxDispatcher _dispatcher;

    public OutboxDispatcherTests()
    {
        _dispatcher = new OutboxDispatcher(_store, _transport, _time);
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Pending_messages_are_sent_in_order_and_marked_dispatched()
    {
        var first = _store.Add();
        var second = _store.Add();

        var sent = await _dispatcher.DispatchBatchAsync(10, Token);

        Assert.Equal(2, sent);
        Assert.Equal([first.Id, second.Id], _transport.Sent);
        Assert.Equal([(first.Id, _time.GetUtcNow()), (second.Id, _time.GetUtcNow())], _store.Dispatched);
        Assert.Empty(_store.PendingIds);
    }

    [Fact]
    public async Task Only_one_batch_is_taken_per_call()
    {
        _store.Add();
        _store.Add();
        var third = _store.Add();

        var sent = await _dispatcher.DispatchBatchAsync(2, Token);

        Assert.Equal(2, sent);
        Assert.Equal(2, _store.LastBatchSize);
        Assert.Equal([third.Id], _store.PendingIds);
    }

    [Fact]
    public async Task A_failed_send_stops_the_batch_and_leaves_the_rest_pending()
    {
        var first = _store.Add(orderingKey: "order-1");
        var second = _store.Add(orderingKey: "order-1");
        var third = _store.Add(orderingKey: "order-1");
        _transport.FailOn(second);

        await Assert.ThrowsAsync<HttpRequestException>(() => _dispatcher.DispatchBatchAsync(10, Token));

        Assert.Equal([first.Id], _transport.Sent);
        Assert.Equal([second.Id, third.Id], _store.PendingIds);
    }

    [Fact]
    public async Task An_empty_outbox_sends_nothing()
    {
        Assert.Equal(0, await _dispatcher.DispatchBatchAsync(10, Token));
        Assert.Empty(_transport.Sent);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task A_batch_size_below_one_is_refused(int batchSize)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _dispatcher.DispatchBatchAsync(batchSize, Token));
    }
}
