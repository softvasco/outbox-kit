namespace OutboxKit;

/// <summary>Hands out pending messages and records what happened to each one.</summary>
public interface IOutboxStore
{
    /// <summary>Claims a batch of the oldest messages that are free, due and not parked.</summary>
    Task<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize, DateTimeOffset now, DateTimeOffset claimedUntil, CancellationToken cancellationToken);

    Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken);

    /// <summary>Counts a failed send; no relay takes the message again before retryAt.</summary>
    Task MarkFailedAsync(OutboxMessage message, DateTimeOffset retryAt, string reason, CancellationToken cancellationToken);

    /// <summary>Counts a failed send and parks the message for good.</summary>
    Task MarkDeadLetteredAsync(
        OutboxMessage message, DateTimeOffset deadLetteredAt, string reason, CancellationToken cancellationToken);
}
