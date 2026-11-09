namespace OutboxKit;

/// <summary>Removes rows the dispatcher has finished with.</summary>
public interface IOutboxRetentionStore
{
    /// <summary>Deletes up to a batch of messages dispatched before the cutoff.</summary>
    // returns how many went; dead-lettered messages stay, someone still has to look at them
    Task<int> DeleteDispatchedAsync(DateTimeOffset cutoff, int batchSize, CancellationToken cancellationToken);
}
