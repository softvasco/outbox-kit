namespace OutboxKit.Tests.Dispatching;

internal sealed class InMemoryOutboxStore : IOutboxStore
{
    private readonly List<OutboxMessage> _pending = [];

    public List<(Guid Id, DateTimeOffset At)> Dispatched { get; } = [];

    public int? LastBatchSize { get; private set; }

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
        return Task.FromResult<IReadOnlyList<OutboxMessage>>([.. _pending.Take(batchSize)]);
    }

    public Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken)
    {
        _pending.Remove(message);
        Dispatched.Add((message.Id, dispatchedAt));
        return Task.CompletedTask;
    }
}
