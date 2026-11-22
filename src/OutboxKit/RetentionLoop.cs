namespace OutboxKit;

// the cleanup loop shared by the outbox and inbox retention services; only the delete and the log differ
internal static class RetentionLoop
{
    public delegate Task<int> DeleteBatch(DateTimeOffset cutoff, int batchSize, CancellationToken cancellationToken);

    public static async Task RunAsync(
        DeleteBatch delete,
        TimeSpan retainFor,
        TimeSpan interval,
        int batchSize,
        TimeProvider timeProvider,
        Action<Exception> logFailure,
        CancellationToken stoppingToken)
    {
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await CleanUpAsync(delete, timeProvider.GetUtcNow() - retainFor, batchSize, logFailure, stoppingToken);
                await Task.Delay(interval, timeProvider, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // host shutdown
        }
    }

    private static async Task CleanUpAsync(
        DeleteBatch delete, DateTimeOffset cutoff, int batchSize, Action<Exception> logFailure, CancellationToken stoppingToken)
    {
        try
        {
            int deleted;
            do
            {
                deleted = await delete(cutoff, batchSize, stoppingToken);
            }
            while (deleted == batchSize);
        }
        catch (Exception exception) when (exception is not OperationCanceledException || !stoppingToken.IsCancellationRequested)
        {
            // a failed cleanup costs nothing but disk until the next one
            logFailure(exception);
        }
    }
}
