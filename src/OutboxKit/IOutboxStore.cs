namespace OutboxKit;

/// <summary>Hands out pending messages to the dispatcher and records the ones that went out.</summary>
public interface IOutboxStore
{
    /// <summary>Claims up to <paramref name="batchSize"/> pending messages, oldest first, until <paramref name="claimedUntil"/>.</summary>
    /// <remarks>Messages under another live claim are skipped; once a claim runs out they can be claimed again.</remarks>
    Task<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize, DateTimeOffset now, DateTimeOffset claimedUntil, CancellationToken cancellationToken);

    Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken);
}
