namespace OutboxKit.Tests.Dispatching;

internal sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly List<OutboxMessage> _pending = [];
    private readonly Dictionary<Guid, DateTimeOffset> _claimedUntil = [];

    public List<(Guid Id, DateTimeOffset At)> Dispatched { get; } = [];

    public List<(Guid Id, DateTimeOffset RetryAt, string Reason)> Failed { get; } = [];

    public List<(Guid Id, DateTimeOffset At, string Reason)> DeadLettered { get; } = [];

    public int? LastBatchSize { get; private set; }

    // released once per ClaimAsync, so a test can wait for the background service to poll
    public SemaphoreSlim Polls { get; } = new(0);

    public IReadOnlyList<Guid> PendingIds => [.. _pending.Select(m => m.Id)];

    public OutboxMessage Add(string type = "orders.placed", string? orderingKey = null, int attempts = 0)
    {
        var message = new OutboxMessage(Guid.CreateVersion7(), type, "{}", DateTimeOffset.UnixEpoch, orderingKey, attempts);
        _pending.Add(message);
        return message;
    }

    public DateTimeOffset? ClaimedUntil(OutboxMessage message) =>
        _claimedUntil.TryGetValue(message.Id, out var until) ? until : null;

    public Task<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize, DateTimeOffset now, DateTimeOffset claimedUntil, CancellationToken cancellationToken)
    {
        LastBatchSize = batchSize;
        IReadOnlyList<OutboxMessage> batch =
            [.. _pending.Where(m => !_claimedUntil.TryGetValue(m.Id, out var until) || until <= now).Take(batchSize)];
        foreach (var message in batch)
        {
            _claimedUntil[message.Id] = claimedUntil;
        }

        Polls.Release();
        return Task.FromResult(batch);
    }

    public Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken)
    {
        Remove(message);
        Dispatched.Add((message.Id, dispatchedAt));
        return Task.CompletedTask;
    }

    public Task MarkFailedAsync(OutboxMessage message, DateTimeOffset retryAt, string reason, CancellationToken cancellationToken)
    {
        var index = _pending.FindIndex(m => m.Id == message.Id);
        _pending[index] = new OutboxMessage(
            message.Id, message.Type, message.Payload, message.CreatedAt, message.OrderingKey, message.Attempts + 1);
        _claimedUntil[message.Id] = retryAt;
        Failed.Add((message.Id, retryAt, reason));
        return Task.CompletedTask;
    }

    public Task MarkDeadLetteredAsync(
        OutboxMessage message, DateTimeOffset deadLetteredAt, string reason, CancellationToken cancellationToken)
    {
        Remove(message);
        DeadLettered.Add((message.Id, deadLetteredAt, reason));
        return Task.CompletedTask;
    }

    private void Remove(OutboxMessage message)
    {
        _pending.RemoveAll(m => m.Id == message.Id);
        _claimedUntil.Remove(message.Id);
    }
}
