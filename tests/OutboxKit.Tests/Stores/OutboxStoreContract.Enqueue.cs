using Microsoft.Extensions.Time.Testing;

namespace OutboxKit.Tests.Stores;

// the SQLite tests cover the logic; these run it on real engines, with a relay polling alongside
public abstract partial class OutboxStoreContract
{
    [Fact]
    public async Task A_message_enqueued_in_a_committed_transaction_is_claimed()
    {
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        OutboxMessage message;
        await using (var transaction = await connection.BeginTransactionAsync(Token))
        {
            message = await Outbox().EnqueueAsync(new OrderPlaced(42), transaction, cancellationToken: Token);
            await transaction.CommitAsync(Token);
        }

        var batch = await _store.ClaimAsync(10, Now, Now + Claim, Token);

        Assert.Equal([message.Id], batch.Select(m => m.Id));
    }

    [Fact]
    public async Task A_rolled_back_transaction_leaves_nothing_to_claim()
    {
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using (var transaction = await connection.BeginTransactionAsync(Token))
        {
            await Outbox().EnqueueAsync(new OrderPlaced(42), transaction, cancellationToken: Token);
            await transaction.RollbackAsync(Token);
        }

        Assert.Empty(await _store.ClaimAsync(10, Now, Now + Claim, Token));
    }

    [Fact]
    public async Task A_relay_polling_during_the_business_transaction_neither_sends_nor_waits_for_its_message()
    {
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        var message = await Outbox().EnqueueAsync(new OrderPlaced(42), transaction, cancellationToken: Token);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var duringTransaction = await _store.ClaimAsync(10, Now, Now + Claim, timeout.Token);
        await transaction.CommitAsync(Token);
        var afterCommit = await _store.ClaimAsync(10, Now, Now + Claim, Token);

        Assert.Empty(duringTransaction);
        Assert.Equal([message.Id], afterCommit.Select(m => m.Id));
    }

    private AdoNetOutbox Outbox()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);
        var clock = new FakeTimeProvider(Now.AddMinutes(-1));
        return new AdoNetOutbox(new OutboxMessageFactory(registry, new JsonMessageSerializer(), clock), _table);
    }

    private sealed record OrderPlaced(int OrderId);
}
