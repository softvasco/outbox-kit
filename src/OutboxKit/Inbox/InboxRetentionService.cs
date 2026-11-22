using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OutboxKit.Inbox;

/// <summary>Deletes old inbox rows in the background, one small batch at a time.</summary>
public sealed partial class InboxRetentionService : BackgroundService
{
    private readonly IInboxRetentionStore _store;
    private readonly InboxRetentionOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<InboxRetentionService> _logger;

    public InboxRetentionService(
        IInboxRetentionStore store,
        IOptions<InboxRetentionOptions> options,
        TimeProvider timeProvider,
        ILogger<InboxRetentionService> logger)
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
        _store.DeleteHandledAsync,
        _options.RetainFor,
        _options.Interval,
        _options.BatchSize,
        _timeProvider,
        LogCleanupFailed,
        stoppingToken);

    [LoggerMessage(Level = LogLevel.Error, Message = "Inbox cleanup failed; it runs again after the interval.")]
    private partial void LogCleanupFailed(Exception exception);
}
