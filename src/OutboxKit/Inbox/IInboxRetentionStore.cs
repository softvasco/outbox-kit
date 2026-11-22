namespace OutboxKit.Inbox;

/// <summary>Removes inbox rows old enough that the message can't be delivered again.</summary>
public interface IInboxRetentionStore
{
    /// <summary>Deletes up to a batch of rows handled before the cutoff; returns how many.</summary>
    Task<int> DeleteHandledAsync(DateTimeOffset cutoff, int batchSize, CancellationToken cancellationToken);
}
