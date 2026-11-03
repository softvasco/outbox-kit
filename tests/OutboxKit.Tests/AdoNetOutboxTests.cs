using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Time.Testing;

namespace OutboxKit.Tests;

// sqlite in memory gives a real transaction; the PostgreSQL and SQL Server runs come with the Testcontainers suite
public sealed class AdoNetOutboxTests : IAsyncLifetime
{
    private sealed record OrderPlaced(int OrderId);

    private readonly SqliteConnection _connection = new("Data Source=:memory:");
    private readonly AdoNetOutbox _outbox;

    public AdoNetOutboxTests()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);
        var factory = new OutboxMessageFactory(registry, new JsonMessageSerializer(), new FakeTimeProvider());
        _outbox = new AdoNetOutbox(factory, new OutboxTable());
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await _connection.OpenAsync(Token);
        await Execute("""
            CREATE TABLE orders (id INTEGER PRIMARY KEY);
            CREATE TABLE outbox_messages (
                id BLOB PRIMARY KEY, type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, ordering_key TEXT NULL);
            """);
    }

    public async ValueTask DisposeAsync() => await _connection.DisposeAsync();

    [Fact]
    public async Task The_message_is_saved_when_the_business_transaction_commits()
    {
        await using (var transaction = await _connection.BeginTransactionAsync(Token))
        {
            await Execute("INSERT INTO orders (id) VALUES (42)", transaction);
            await _outbox.EnqueueAsync(new OrderPlaced(42), transaction, "order-42", Token);
            await transaction.CommitAsync(Token);
        }

        Assert.Equal(["orders.placed|{\"orderId\":42}|order-42"], await OutboxRows());
    }

    [Fact]
    public async Task Nothing_is_saved_when_the_business_transaction_rolls_back()
    {
        await using (var transaction = await _connection.BeginTransactionAsync(Token))
        {
            await Execute("INSERT INTO orders (id) VALUES (42)", transaction);
            await _outbox.EnqueueAsync(new OrderPlaced(42), transaction, cancellationToken: Token);
            await transaction.RollbackAsync(Token);
        }

        Assert.Empty(await OutboxRows());
    }

    [Fact]
    public async Task A_message_without_an_ordering_key_is_stored_with_null()
    {
        await using (var transaction = await _connection.BeginTransactionAsync(Token))
        {
            await _outbox.EnqueueAsync(new OrderPlaced(7), transaction, cancellationToken: Token);
            await transaction.CommitAsync(Token);
        }

        Assert.Equal(["orders.placed|{\"orderId\":7}|<null>"], await OutboxRows());
    }

    [Fact]
    public async Task A_transaction_that_already_committed_is_refused()
    {
        await using var transaction = await _connection.BeginTransactionAsync(Token);
        await transaction.CommitAsync(Token);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _outbox.EnqueueAsync(new OrderPlaced(42), transaction, cancellationToken: Token));
        Assert.Empty(await OutboxRows());
    }

    private async Task Execute(string sql, System.Data.Common.DbTransaction? transaction = null)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = (SqliteTransaction?)transaction;
        await command.ExecuteNonQueryAsync(Token);
    }

    private async Task<List<string>> OutboxRows()
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT type, payload, ordering_key FROM outbox_messages";
        await using var reader = await command.ExecuteReaderAsync(Token);

        var rows = new List<string>();
        while (await reader.ReadAsync(Token))
        {
            var key = reader.IsDBNull(2) ? "<null>" : reader.GetString(2);
            rows.Add($"{reader.GetString(0)}|{reader.GetString(1)}|{key}");
        }

        return rows;
    }
}
