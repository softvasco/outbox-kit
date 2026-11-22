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

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => RetentionLoop.RunAsync(
        _store.DeleteDispatchedAsync,
        _options.RetainFor,
        _options.Interval,
        _options.BatchSize,
        _timeProvider,
        LogCleanupFailed,
        stoppingToken);

    [LoggerMessage(Level = LogLevel.Error, Message = "Outbox cleanup failed; it runs again after the interval.")]
    private partial void LogCleanupFailed(Exception exception);
}
