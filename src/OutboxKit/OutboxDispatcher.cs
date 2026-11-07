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

    /// <summary>Claims a batch, sends it in order and returns how many messages went out. A failed send stops the batch.</summary>
    public async Task<int> DispatchBatchAsync(int batchSize, TimeSpan claimDuration, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(claimDuration, TimeSpan.Zero);

        var now = _timeProvider.GetUtcNow();
        var claimedUntil = now + claimDuration;
        var batch = await _store.ClaimAsync(batchSize, now, claimedUntil, cancellationToken);

        var sent = 0;
        foreach (var message in batch)
        {
            // past the claim another relay may own the rest of the batch, and sending it here would double it
            if (_timeProvider.GetUtcNow() >= claimedUntil)
            {
                break;
            }

            // stopping on the first failure keeps later messages for the same key behind it
            await _transport.SendAsync(message, cancellationToken);

            // marked after the send, so a crash in between sends it twice rather than never
            await _store.MarkDispatchedAsync(message, _timeProvider.GetUtcNow(), cancellationToken);
            sent++;
        }

        return sent;
    }
}
