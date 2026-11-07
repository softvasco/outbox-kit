namespace OutboxKit.Tests.Dispatching;

internal sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly List<OutboxMessage> _pending = [];
    private readonly Dictionary<Guid, DateTimeOffset> _claimedUntil = [];

    public List<(Guid Id, DateTimeOffset At)> Dispatched { get; } = [];

    public int? LastBatchSize { get; private set; }

    // released once per ClaimAsync, so a test can wait for the background service to poll
    public SemaphoreSlim Polls { get; } = new(0);

    public IReadOnlyList<Guid> PendingIds => [.. _pending.Select(m => m.Id)];

    public OutboxMessage Add(string type = "orders.placed", string? orderingKey = null)
    {
        var message = new OutboxMessage(Guid.CreateVersion7(), type, "{}", DateTimeOffset.UnixEpoch, orderingKey);
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
        _pending.Remove(message);
        _claimedUntil.Remove(message.Id);
        Dispatched.Add((message.Id, dispatchedAt));
        return Task.CompletedTask;
    }
}
