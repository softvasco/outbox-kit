using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using OutboxKit.Inbox;

namespace OutboxKit.Tests.Inbox;

// the loop itself is covered by the outbox retention tests; these check the inbox wiring
public sealed class InboxRetentionServiceTests : IAsyncDisposable
{
    private static readonly TimeSpan RetainFor = TimeSpan.FromDays(14);

    private readonly FakeRetentionStore _store = new();
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 11, 22, 0, 0, 0, TimeSpan.Zero));
    private readonly FakeLogger<InboxRetentionService> _logger = new();
    private InboxRetentionService? _service;

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
    public async Task It_deletes_rows_handled_before_the_retention_period()
    {
        await Start();

        var call = await _store.NextCall();

        Assert.Equal((_time.GetUtcNow() - RetainFor, 100), call);
    }

    [Fact]
    public async Task A_failed_cleanup_is_logged_as_an_inbox_cleanup()
    {
        _store.FailNext();
        await Start();
        await _store.NextCall();

        var error = await WaitForError();

        Assert.IsType<TimeoutException>(error.Exception);
        Assert.StartsWith("Inbox cleanup failed", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void It_runs_as_a_hosted_service()
    {
        using var provider = Services().AddInboxRetention().BuildServiceProvider(validateScopes: true);

        Assert.Single(provider.GetServices<IHostedService>().OfType<InboxRetentionService>());
    }

    [Fact]
    public void Remembers_handled_messages_for_two_weeks_by_default() =>
        Assert.Equal(TimeSpan.FromDays(14), new InboxRetentionOptions().RetainFor);

    [Theory]
    [InlineData(0, 60, 10)]
    [InlineData(60, 0, 10)]
    [InlineData(60, 60, 0)]
    public void Bad_options_are_refused_on_resolve(int retainSeconds, int intervalSeconds, int batchSize)
    {
        using var provider = Services().AddInboxRetention(o =>
        {
            o.RetainFor = TimeSpan.FromSeconds(retainSeconds);
            o.Interval = TimeSpan.FromSeconds(intervalSeconds);
            o.BatchSize = batchSize;
        }).BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<InboxRetentionOptions>>().Value);
    }

    private static IServiceCollection Services() =>
        new ServiceCollection().AddLogging().AddSingleton<IInboxRetentionStore, FakeRetentionStore>();

    private async Task Start()
    {
        var options = new InboxRetentionOptions { RetainFor = RetainFor, Interval = TimeSpan.FromMinutes(10), BatchSize = 100 };
        _service = new InboxRetentionService(_store, Options.Create(options), _time, _logger);
        await _service.StartAsync(Token);
    }

    // the error is logged on the service's own thread, just after the failed call returns
    private async Task<FakeLogRecord> WaitForError()
    {
        for (var i = 0; i < 500; i++)
        {
            if (_logger.Collector.GetSnapshot().FirstOrDefault(r => r.Level == LogLevel.Error) is { } error)
            {
                return error;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(10), Token);
        }

        throw new TimeoutException("Nothing was logged.");
    }

    private sealed class FakeRetentionStore : IInboxRetentionStore
    {
        private readonly Channel<(DateTimeOffset Cutoff, int BatchSize)> _calls =
            Channel.CreateUnbounded<(DateTimeOffset Cutoff, int BatchSize)>();

        private bool _failNext;

        public void FailNext() => _failNext = true;

        public async Task<(DateTimeOffset Cutoff, int BatchSize)> NextCall()
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            return await _calls.Reader.ReadAsync(timeout.Token);
        }

        public Task<int> DeleteHandledAsync(DateTimeOffset cutoff, int batchSize, CancellationToken cancellationToken)
        {
            _calls.Writer.TryWrite((cutoff, batchSize));
            if (_failNext)
            {
                _failNext = false;
                throw new TimeoutException("database unavailable");
            }

            return Task.FromResult(0);
        }
    }
}
