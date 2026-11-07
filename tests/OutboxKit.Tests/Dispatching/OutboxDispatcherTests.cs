using Microsoft.Extensions.Time.Testing;

namespace OutboxKit.Tests.Dispatching;

public sealed class OutboxDispatcherTests
{
    private static readonly TimeSpan Claim = TimeSpan.FromSeconds(30);

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

        var sent = await _dispatcher.DispatchBatchAsync(10, Claim, Token);

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

        var sent = await _dispatcher.DispatchBatchAsync(2, Claim, Token);

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

        await Assert.ThrowsAsync<HttpRequestException>(() => _dispatcher.DispatchBatchAsync(10, Claim, Token));

        Assert.Equal([first.Id], _transport.Sent);
        Assert.Equal([second.Id, third.Id], _store.PendingIds);
    }

    [Fact]
    public async Task The_batch_is_claimed_from_now_for_the_claim_duration()
    {
        _store.Add();

        await _dispatcher.DispatchBatchAsync(10, Claim, Token);

        Assert.Equal(_time.GetUtcNow() + Claim, _store.LastClaimedUntil);
    }

    [Fact]
    public async Task Messages_still_claimed_by_another_relay_are_left_alone()
    {
        var other = new OutboxDispatcher(_store, _transport, _time);
        _store.Add();
        await _store.ClaimAsync(10, _time.GetUtcNow(), _time.GetUtcNow() + Claim, Token);

        _time.Advance(Claim - TimeSpan.FromSeconds(1));
        var before = await other.DispatchBatchAsync(10, Claim, Token);
        _time.Advance(TimeSpan.FromSeconds(1));
        var after = await other.DispatchBatchAsync(10, Claim, Token);

        Assert.Equal((0, 1), (before, after));
    }

    [Fact]
    public async Task Sending_stops_when_the_claim_runs_out_halfway_through_the_batch()
    {
        var first = _store.Add();
        var second = _store.Add();
        var slow = new SlowTransport(_time, Claim);
        var dispatcher = new OutboxDispatcher(_store, slow, _time);

        var sent = await dispatcher.DispatchBatchAsync(10, Claim, Token);

        Assert.Equal(1, sent);
        Assert.Equal([first.Id], slow.Sent);
        Assert.Equal([second.Id], _store.PendingIds);
    }

    [Fact]
    public async Task A_failed_send_is_tried_again_after_the_first_delay_not_the_claim()
    {
        var message = _store.Add();
        _transport.FailOn(message, times: 1);
        var dispatcher = Dispatcher(new OutboxRetryPolicy { FirstDelay = TimeSpan.FromSeconds(2) });
        await Assert.ThrowsAsync<HttpRequestException>(() => dispatcher.DispatchBatchAsync(10, Claim, Token));

        _time.Advance(TimeSpan.FromSeconds(1));
        var early = await dispatcher.DispatchBatchAsync(10, Claim, Token);
        _time.Advance(TimeSpan.FromSeconds(1));
        var due = await dispatcher.DispatchBatchAsync(10, Claim, Token);

        Assert.Equal((0, 1), (early, due));
        Assert.Equal("broker unavailable", Assert.Single(_store.Failed).Reason);
    }

    [Fact]
    public async Task Each_failure_is_counted_on_the_message_so_the_wait_grows()
    {
        var message = _store.Add();
        _transport.FailOn(message, times: 2);
        var dispatcher = Dispatcher(new OutboxRetryPolicy { FirstDelay = TimeSpan.FromSeconds(2) });

        await Assert.ThrowsAsync<HttpRequestException>(() => dispatcher.DispatchBatchAsync(10, Claim, Token));
        _time.Advance(TimeSpan.FromSeconds(2));
        await Assert.ThrowsAsync<HttpRequestException>(() => dispatcher.DispatchBatchAsync(10, Claim, Token));

        Assert.Equal(_time.GetUtcNow() + TimeSpan.FromSeconds(4), _store.Failed[1].RetryAt);
    }

    [Fact]
    public async Task A_message_is_parked_when_its_last_allowed_send_fails()
    {
        var message = _store.Add(attempts: 2);
        _transport.FailOn(message);
        var dispatcher = Dispatcher(new OutboxRetryPolicy { MaxAttempts = 3 });

        await Assert.ThrowsAsync<HttpRequestException>(() => dispatcher.DispatchBatchAsync(10, Claim, Token));

        Assert.Equal((message.Id, _time.GetUtcNow(), "broker unavailable"), Assert.Single(_store.DeadLettered));
        Assert.Empty(_store.Failed);
        Assert.Empty(_store.PendingIds);
    }

    [Fact]
    public async Task Cancelling_mid_send_is_not_counted_as_a_failure()
    {
        var message = _store.Add();
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(Token);
        var dispatcher = new OutboxDispatcher(_store, new CancellingTransport(cancel), _time);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => dispatcher.DispatchBatchAsync(10, Claim, cancel.Token));

        Assert.Empty(_store.Failed);
        Assert.Equal([message.Id], _store.PendingIds);
    }

    [Fact]
    public async Task An_empty_outbox_sends_nothing()
    {
        Assert.Equal(0, await _dispatcher.DispatchBatchAsync(10, Claim, Token));
        Assert.Empty(_transport.Sent);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task A_batch_size_below_one_is_refused(int batchSize)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _dispatcher.DispatchBatchAsync(batchSize, Claim, Token));
    }

    [Fact]
    public async Task A_claim_duration_that_is_not_positive_is_refused()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _dispatcher.DispatchBatchAsync(10, TimeSpan.Zero, Token));
    }

    private OutboxDispatcher Dispatcher(OutboxRetryPolicy retry) => new(_store, _transport, _time, retry);

    // the host shuts down while the broker call is in flight
    private sealed class CancellingTransport(CancellationTokenSource cancel) : IMessageTransport
    {
        public async Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
        {
            await cancel.CancelAsync();
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    // each send takes as long as the whole claim
    private sealed class SlowTransport(FakeTimeProvider time, TimeSpan delay) : IMessageTransport
    {
        public List<Guid> Sent { get; } = [];

        public Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
        {
            Sent.Add(message.Id);
            time.Advance(delay);
            return Task.CompletedTask;
        }
    }
}
