using Microsoft.Extensions.Time.Testing;
using OutboxKit.Tests.Dispatching;

namespace OutboxKit.Tests.Stores;

// the dispatcher end to end on a real server: claim, send, mark, retry and park
public abstract partial class OutboxStoreContract
{
    [Fact]
    public async Task Two_dispatchers_taking_turns_send_every_message_once_and_each_key_in_order()
    {
        var expected = new Dictionary<string, List<Guid>>();
        for (var i = 0; i < 24; i++)
        {
            var key = $"order-{i % 3}";
            var id = await Insert(Now.AddSeconds(i - 60), key);
            (expected.TryGetValue(key, out var ids) ? ids : expected[key] = []).Add(id);
        }

        var transport = new RecordingTransport();
        var time = new FakeTimeProvider(Now);
        OutboxDispatcher[] relays = [new(_store, transport, time), new(_store, transport, time)];

        int sent;
        do
        {
            sent = 0;
            foreach (var relay in relays)
            {
                sent += await relay.DispatchBatchAsync(4, Claim, Token);
            }
        }
        while (sent > 0);

        Assert.Equal(24, transport.Sent.Distinct().Count());
        Assert.Equal(24, transport.Sent.Count);
        foreach (var (key, ids) in expected)
        {
            Assert.Equal(ids, transport.Sent.Where(ids.Contains));
        }
    }

    [Fact]
    public async Task A_failed_send_goes_out_once_its_backoff_has_passed()
    {
        var id = await Insert(Now);
        var transport = new RecordingTransport();
        transport.FailOn(new OutboxMessage(id, "orders.placed", "{}", Now), times: 1);
        var time = new FakeTimeProvider(Now);
        var dispatcher = new OutboxDispatcher(_store, transport, time, new OutboxRetryPolicy { FirstDelay = TimeSpan.FromSeconds(2) });

        await Assert.ThrowsAsync<HttpRequestException>(() => dispatcher.DispatchBatchAsync(10, Claim, Token));
        time.Advance(TimeSpan.FromSeconds(1));
        var early = await dispatcher.DispatchBatchAsync(10, Claim, Token);
        time.Advance(TimeSpan.FromSeconds(1));
        var due = await dispatcher.DispatchBatchAsync(10, Claim, Token);

        Assert.Equal((0, 1), (early, due));
        Assert.Equal([id], transport.Sent);
    }

    [Fact]
    public async Task A_message_that_keeps_failing_is_parked_and_the_next_one_for_its_key_goes_out()
    {
        var poison = await Insert(Now.AddMinutes(-1), "order-1");
        var next = await Insert(Now, "order-1");
        var transport = new RecordingTransport();
        transport.FailOn(new OutboxMessage(poison, "orders.placed", "{}", Now));
        var time = new FakeTimeProvider(Now);
        var retry = new OutboxRetryPolicy { MaxAttempts = 2, FirstDelay = TimeSpan.FromSeconds(1) };
        var dispatcher = new OutboxDispatcher(_store, transport, time, retry);

        await Assert.ThrowsAsync<HttpRequestException>(() => dispatcher.DispatchBatchAsync(10, Claim, Token));
        time.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<HttpRequestException>(() => dispatcher.DispatchBatchAsync(10, Claim, Token));
        var afterParking = await dispatcher.DispatchBatchAsync(10, Claim, Token);

        Assert.Equal(1, afterParking);
        Assert.Equal([next], transport.Sent);
        Assert.Equal("broker unavailable", await LastError(poison));
    }
}
