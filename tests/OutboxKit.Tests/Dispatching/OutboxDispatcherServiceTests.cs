using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace OutboxKit.Tests.Dispatching;

public sealed class OutboxDispatcherServiceTests : IAsyncDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(500);

    private readonly InMemoryOutboxStore _store = new();
    private readonly RecordingTransport _transport = new();
    private readonly FakeTimeProvider _time = new();
    private readonly FakeLogger<OutboxDispatcherService> _logger = new();
    private OutboxDispatcherService? _service;

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask DisposeAsync()
    {
        if (_service is not null)
        {
            await _service.StopAsync(CancellationToken.None);
            _service.Dispose();
        }
    }

    [Fact]
    public async Task It_polls_on_start_and_then_once_per_interval()
    {
        await Start(batchSize: 10);
        await NextPoll();

        var waited = await AdvanceUntilNextPoll();

        Assert.True(waited >= Interval, $"polled again after {waited}");
    }

    [Fact]
    public async Task A_full_batch_is_followed_by_the_next_one_without_waiting()
    {
        _store.Add();
        _store.Add();
        var third = _store.Add();
        await Start(batchSize: 2);

        await NextPoll();
        await NextPoll();

        Assert.Contains(third.Id, _transport.Sent);
    }

    [Fact]
    public async Task A_failed_batch_is_logged_and_retried_on_the_next_poll()
    {
        var message = _store.Add();
        _transport.FailOn(message, times: 1);
        await Start(batchSize: 10);
        await NextPoll();

        await AdvanceUntilNextPoll();
        await _service!.StopAsync(Token);

        var error = Assert.Single(_logger.Collector.GetSnapshot(), r => r.Level == LogLevel.Error);
        Assert.IsType<HttpRequestException>(error.Exception);
        Assert.Equal([message.Id], _transport.Sent);
        Assert.Empty(_store.PendingIds);
    }

    [Fact]
    public async Task Stopping_the_host_ends_the_loop_cleanly()
    {
        await Start(batchSize: 10);
        await NextPoll();

        await _service!.StopAsync(Token);

        Assert.True(_service.ExecuteTask!.IsCompletedSuccessfully);
    }

    [Theory]
    [InlineData(0, 10)]
    [InlineData(-1, 10)]
    [InlineData(1000, 0)]
    public void Options_that_would_spin_or_never_send_are_refused(int intervalMs, int batchSize)
    {
        var options = new OutboxDispatcherOptions { PollingInterval = TimeSpan.FromMilliseconds(intervalMs), BatchSize = batchSize };

        Assert.Throws<ArgumentOutOfRangeException>(() => Create(options));
    }

    private async Task Start(int batchSize)
    {
        _service = Create(new OutboxDispatcherOptions { PollingInterval = Interval, BatchSize = batchSize });
        await _service.StartAsync(Token);
    }

    private OutboxDispatcherService Create(OutboxDispatcherOptions options) =>
        new(new OutboxDispatcher(_store, _transport, _time), Options.Create(options), _time, _logger);

    private async Task NextPoll()
    {
        Assert.True(await _store.Polls.WaitAsync(TimeSpan.FromSeconds(10), Token), "the service didn't poll");
    }

    // the service may not have started its delay yet when time moves, so keep nudging the clock until it polls
    private async Task<TimeSpan> AdvanceUntilNextPoll()
    {
        var advanced = TimeSpan.Zero;
        while (!await _store.Polls.WaitAsync(TimeSpan.FromMilliseconds(20), Token))
        {
            Assert.True(advanced < TimeSpan.FromMinutes(1), "the service never polled again");
            _time.Advance(Step);
            advanced += Step;
        }

        return advanced;
    }
}
