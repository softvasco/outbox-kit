using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace OutboxKit;

/// <summary>Runs the dispatcher in the background: batch after batch while the outbox is busy, then one poll per interval.</summary>
public sealed partial class OutboxDispatcherService : BackgroundService
{
    private readonly OutboxDispatcher _dispatcher;
    private readonly OutboxDispatcherOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<OutboxDispatcherService> _logger;

    public OutboxDispatcherService(
        OutboxDispatcher dispatcher,
        IOptions<OutboxDispatcherOptions> options,
        TimeProvider timeProvider,
        ILogger<OutboxDispatcherService> logger)
    {
        ArgumentNullException.ThrowIfNull(dispatcher);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(logger);

        options.Value.Validate();
        _dispatcher = dispatcher;
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
                var sent = await DispatchBatchAsync(stoppingToken);

                // a full batch means more is probably waiting, so only sleep once the outbox is drained
                if (sent < _options.BatchSize)
                {
                    await Task.Delay(_options.PollingInterval, _timeProvider, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // host shutdown
        }
    }

    private async Task<int> DispatchBatchAsync(CancellationToken stoppingToken)
    {
        try
        {
            return await _dispatcher.DispatchBatchAsync(_options.BatchSize, _options.ClaimDuration, stoppingToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            // one bad poll (database or broker down) must not stop the relay; the messages stay pending
            LogDispatchFailed(exception);
            return 0;
        }
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "Outbox dispatch failed; a failed message is retried after its backoff, the rest once their claim runs out.")]
    private partial void LogDispatchFailed(Exception exception);
}
