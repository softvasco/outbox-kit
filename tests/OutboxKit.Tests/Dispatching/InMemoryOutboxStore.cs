namespace OutboxKit.Tests.Dispatching;

internal sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly List<OutboxMessage> _pending = [];

    public List<(Guid Id, DateTimeOffset At)> Dispatched { get; } = [];

    public int? LastBatchSize { get; private set; }

    // released once per GetPendingAsync, so a test can wait for the background service to poll
    public SemaphoreSlim Polls { get; } = new(0);

    public IReadOnlyList<Guid> PendingIds => [.. _pending.Select(m => m.Id)];

    public OutboxMessage Add(string type = "orders.placed", string? orderingKey = null)
    {
        var message = new OutboxMessage(Guid.CreateVersion7(), type, "{}", DateTimeOffset.UnixEpoch, orderingKey);
        _pending.Add(message);
        return message;
    }

    public Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int batchSize, CancellationToken cancellationToken)
    {
        LastBatchSize = batchSize;
        IReadOnlyList<OutboxMessage> batch = [.. _pending.Take(batchSize)];
        Polls.Release();
        return Task.FromResult(batch);
    }

    public Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken)
    {
        _pending.Remove(message);
        Dispatched.Add((message.Id, dispatchedAt));
        return Task.CompletedTask;
    }
}
