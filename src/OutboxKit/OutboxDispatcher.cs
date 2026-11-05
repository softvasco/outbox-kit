namespace OutboxKit;

/// <summary>Moves one batch of pending messages from the store to the transport.</summary>
public sealed class OutboxDispatcher
{
    private readonly IOutboxStore _store;
    private readonly IMessageTransport _transport;
    private readonly TimeProvider _timeProvider;

    public OutboxDispatcher(IOutboxStore store, IMessageTransport transport, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(timeProvider);

        _store = store;
        _transport = transport;
        _timeProvider = timeProvider;
    }

    /// <summary>Sends the batch in order and returns how many messages went out. A failed send stops the batch.</summary>
    public async Task<int> DispatchBatchAsync(int batchSize, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);

        var batch = await _store.GetPendingAsync(batchSize, cancellationToken);
        foreach (var message in batch)
        {
            // stopping on the first failure keeps later messages for the same key behind it
            await _transport.SendAsync(message, cancellationToken);

            // marked after the send, so a crash in between sends it twice rather than never
            await _store.MarkDispatchedAsync(message, _timeProvider.GetUtcNow(), cancellationToken);
        }

        return batch.Count;
    }
}
