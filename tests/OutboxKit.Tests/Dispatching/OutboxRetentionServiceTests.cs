using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace OutboxKit.Tests.Dispatching;

public sealed class OutboxRetentionServiceTests : IAsyncDisposable
{
    private static readonly TimeSpan Interval = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan RetainFor = TimeSpan.FromDays(3);

    private readonly FakeRetentionStore _store = new();
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 11, 9, 0, 0, 0, TimeSpan.Zero));
    private readonly FakeLogger<OutboxRetentionService> _logger = new();
    private OutboxRetentionService? _service;

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
    public async Task It_deletes_what_is_older_than_the_retention_period()
    {
        await Start(batchSize: 100);

        var call = await _store.NextCall();

        Assert.Equal((_time.GetUtcNow() - RetainFor, 100), call);
    }

    [Fact]
    public async Task A_full_batch_is_followed_by_another_with_the_same_cutoff()
    {
        _store.Returns(2, 2, 1);
        await Start(batchSize: 2);

        var calls = new[] { await _store.NextCall(), await _store.NextCall(), await _store.NextCall() };

        Assert.Single(calls.Distinct());
    }

    [Fact]
    public async Task It_runs_again_after_the_interval()
    {
        await Start(batchSize: 100);
        var first = await _store.NextCall();

        var second = await AdvanceUntilNextCall();

        Assert.True(second.Cutoff - first.Cutoff >= Interval, $"ran again after {second.Cutoff - first.Cutoff}");
    }

    [Fact]
    public async Task A_failed_cleanup_is_logged_and_tried_again_after_the_interval()
    {
        _store.FailNext();
        await Start(batchSize: 100);
        await _store.NextCall();

        await AdvanceUntilNextCall();

        var error = Assert.Single(_logger.Collector.GetSnapshot(), r => r.Level == LogLevel.Error);
        Assert.IsType<TimeoutException>(error.Exception);
    }

    [Fact]
    public void It_runs_as_a_hosted_service()
    {
        using var provider = Services().AddOutboxRetention().BuildServiceProvider(validateScopes: true);

        Assert.Single(provider.GetServices<IHostedService>().OfType<OutboxRetentionService>());
    }

    [Theory]
    [InlineData(0, 60, 10)]
    [InlineData(60, 0, 10)]
    [InlineData(60, 60, 0)]
    public void Bad_options_are_refused_on_resolve(int retainSeconds, int intervalSeconds, int batchSize)
    {
        using var provider = Services().AddOutboxRetention(o =>
        {
            o.RetainFor = TimeSpan.FromSeconds(retainSeconds);
            o.Interval = TimeSpan.FromSeconds(intervalSeconds);
            o.BatchSize = batchSize;
        }).BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<OutboxRetentionOptions>>().Value);
    }

    private static IServiceCollection Services() =>
        new ServiceCollection().AddLogging().AddSingleton<IOutboxRetentionStore, FakeRetentionStore>();

    private async Task Start(int batchSize)
    {
        var options = new OutboxRetentionOptions { RetainFor = RetainFor, Interval = Interval, BatchSize = batchSize };
        _service = new OutboxRetentionService(_store, Options.Create(options), _time, _logger);
        await _service.StartAsync(Token);
    }

    // the service may not have reached its delay yet when the test advances, so keep stepping
    private async Task<(DateTimeOffset Cutoff, int BatchSize)> AdvanceUntilNextCall()
    {
        for (var i = 0; i < 500; i++)
        {
            _time.Advance(Interval / 10);
            await Task.Delay(TimeSpan.FromMilliseconds(10), Token);
            if (_store.TryNextCall(out var call))
            {
                return call;
            }
        }

        throw new TimeoutException("The service never ran again.");
    }

    private sealed class FakeRetentionStore : IOutboxRetentionStore
    {
        private readonly Queue<int> _results = new();
        private readonly Channel<(DateTimeOffset Cutoff, int BatchSize)> _calls = Channel.CreateUnbounded<(DateTimeOffset Cutoff, int BatchSize)>();
        private bool _failNext;

        public void Returns(params int[] results) => Array.ForEach(results, _results.Enqueue);

        public void FailNext() => _failNext = true;

        public async Task<(DateTimeOffset Cutoff, int BatchSize)> NextCall()
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            return await _calls.Reader.ReadAsync(timeout.Token);
        }

        public bool TryNextCall(out (DateTimeOffset Cutoff, int BatchSize) call) => _calls.Reader.TryRead(out call);

        public Task<int> DeleteDispatchedAsync(DateTimeOffset cutoff, int batchSize, CancellationToken cancellationToken)
        {
            _calls.Writer.TryWrite((cutoff, batchSize));
            if (_failNext)
            {
                _failNext = false;
                throw new TimeoutException("database unavailable");
            }

            return Task.FromResult(_results.TryDequeue(out var result) ? result : 0);
        }
    }
}
