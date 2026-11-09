using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OutboxKit;

/// <summary>Deletes old dispatched messages in the background, one small batch at a time.</summary>
public sealed partial class OutboxRetentionService : BackgroundService
{
    private readonly IOutboxRetentionStore _store;
    private readonly OutboxRetentionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<OutboxRetentionService> _logger;

    public OutboxRetentionService(
        IOutboxRetentionStore store,
        IOptions<OutboxRetentionOptions> options,
        TimeProvider timeProvider,
        ILogger<OutboxRetentionService> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        _store = store;
        _options = options.Value;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await CleanUpAsync(stoppingToken);
                await Task.Delay(_options.Interval, _timeProvider, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // host shutdown
        }
    }

    private async Task CleanUpAsync(CancellationToken stoppingToken)
    {
        try
        {
            var cutoff = _timeProvider.GetUtcNow() - _options.RetainFor;
            int deleted;
            do
            {
                deleted = await _store.DeleteDispatchedAsync(cutoff, _options.BatchSize, stoppingToken);
            }
            while (deleted == _options.BatchSize);
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            // a failed cleanup costs nothing but disk until the next one
            LogCleanupFailed(exception);
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Outbox cleanup failed; it runs again after the interval.")]
    private partial void LogCleanupFailed(Exception exception);
}
