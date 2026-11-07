namespace OutboxKit;

/// <summary>Moves one batch of pending messages from the store to the transport.</summary>
public sealed class OutboxDispatcher
{
    private readonly IOutboxStore _store;
    private readonly IMessageTransport _transport;
    private readonly TimeProvider _timeProvider;
    private readonly OutboxRetryPolicy _retry;

    public OutboxDispatcher(IOutboxStore store, IMessageTransport transport, TimeProvider timeProvider, OutboxRetryPolicy? retry = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(transport);
        ArgumentNullException.ThrowIfNull(timeProvider);

        retry ??= new OutboxRetryPolicy();
        retry.Validate();
        _store = store;
        _transport = transport;
        _timeProvider = timeProvider;
        _retry = retry;
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

            try
            {
                await _transport.SendAsync(message, cancellationToken);
            }
            catch (Exception exception) when (exception is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                // stopping on the first failure keeps later messages for the same key behind it
                await RecordFailureAsync(message, exception, cancellationToken);
                throw;
            }

            // marked after the send, so a crash in between sends it twice rather than never
            await _store.MarkDispatchedAsync(message, _timeProvider.GetUtcNow(), cancellationToken);
            sent++;
        }

        return sent;
    }

    private async Task RecordFailureAsync(OutboxMessage message, Exception exception, CancellationToken cancellationToken)
    {
        var failedAttempts = message.Attempts + 1;
        var now = _timeProvider.GetUtcNow();
        if (_retry.IsExhausted(failedAttempts))
        {
            await _store.MarkDeadLetteredAsync(message, now, exception.Message, cancellationToken);
        }
        else
        {
            await _store.MarkFailedAsync(message, now + _retry.DelayAfter(failedAttempts), exception.Message, cancellationToken);
        }
    }
}
