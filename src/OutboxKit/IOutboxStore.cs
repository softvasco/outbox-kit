namespace OutboxKit;

/// <summary>Reads pending messages for the dispatcher and records the ones that went out.</summary>
public interface IOutboxStore
{
    /// <summary>Returns up to <paramref name="batchSize"/> messages not yet dispatched, oldest first.</summary>
    Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int batchSize, CancellationToken cancellationToken);

    Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken);
}
