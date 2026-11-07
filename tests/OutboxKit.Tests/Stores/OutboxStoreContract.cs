using System.Collections.Concurrent;
using System.Data.Common;
using OutboxKit.Stores;

namespace OutboxKit.Tests.Stores;

/// <summary>What every database store must do; each engine runs these against a real server.</summary>
public abstract class OutboxStoreContract : IAsyncLifetime
{
    private static readonly DateTimeOffset Now = new(2026, 11, 7, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Claim = TimeSpan.FromSeconds(30);

    private readonly OutboxTable _table = new($"outbox_{Guid.NewGuid():N}");
    private SqlOutboxStore _store = null!;

    protected static CancellationToken Token => TestContext.Current.CancellationToken;

    protected abstract DbDataSource DataSource { get; }

    public async ValueTask InitializeAsync()
    {
        await using var create = DataSource.CreateCommand(CreateTableSql(_table.Name));
        await create.ExecuteNonQueryAsync(Token);
        _store = CreateStore(_table);
    }

    public async ValueTask DisposeAsync()
    {
        await using var drop = DataSource.CreateCommand($"DROP TABLE {_table.Name}");
        await drop.ExecuteNonQueryAsync(CancellationToken.None);
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task The_oldest_messages_are_claimed_first_up_to_the_batch_size()
    {
        var newest = await Insert(Now.AddMinutes(-1));
        var oldest = await Insert(Now.AddMinutes(-3));
        var middle = await Insert(Now.AddMinutes(-2));

        var batch = await _store.ClaimAsync(2, Now, Now + Claim, Token);

        Assert.Equal([oldest, middle], batch.Select(m => m.Id));
        Assert.Equal([newest], (await _store.ClaimAsync(10, Now, Now + Claim, Token)).Select(m => m.Id));
    }

    [Fact]
    public async Task A_claimed_message_comes_back_only_once_its_claim_runs_out()
    {
        var id = await Insert(Now);
        await _store.ClaimAsync(10, Now, Now + Claim, Token);

        var whileClaimed = await _store.ClaimAsync(10, Now + Claim - TimeSpan.FromSeconds(1), Now + (Claim * 2), Token);
        var afterwards = await _store.ClaimAsync(10, Now + Claim, Now + (Claim * 2), Token);

        Assert.Empty(whileClaimed);
        Assert.Equal([id], afterwards.Select(m => m.Id));
    }

    [Fact]
    public async Task A_dispatched_message_is_never_claimed_again()
    {
        await Insert(Now);
        var message = Assert.Single(await _store.ClaimAsync(10, Now, Now + Claim, Token));

        await _store.MarkDispatchedAsync(message, Now, Token);

        Assert.Empty(await _store.ClaimAsync(10, Now.AddDays(1), Now.AddDays(1) + Claim, Token));
    }

    [Fact]
    public async Task A_failed_message_waits_for_its_retry_time_and_comes_back_with_the_attempt_counted()
    {
        await Insert(Now);
        var message = Assert.Single(await _store.ClaimAsync(10, Now, Now + Claim, Token));
        var retryAt = Now.AddMinutes(1);

        await _store.MarkFailedAsync(message, retryAt, "broker unavailable", Token);

        Assert.Empty(await _store.ClaimAsync(10, retryAt.AddSeconds(-1), retryAt + Claim, Token));
        var retried = Assert.Single(await _store.ClaimAsync(10, retryAt, retryAt + Claim, Token));
        Assert.Equal(1, retried.Attempts);
        Assert.Equal("broker unavailable", await LastError(message.Id));
    }

    [Fact]
    public async Task A_dead_lettered_message_is_never_claimed_again_and_keeps_why()
    {
        await Insert(Now);
        var message = Assert.Single(await _store.ClaimAsync(10, Now, Now + Claim, Token));

        await _store.MarkDeadLetteredAsync(message, Now, "payload rejected", Token);

        Assert.Empty(await _store.ClaimAsync(10, Now.AddDays(1), Now.AddDays(1) + Claim, Token));
        Assert.Equal("payload rejected", await LastError(message.Id));
    }

    [Fact]
    public async Task The_message_comes_back_as_it_was_saved()
    {
        var saved = new OutboxMessage(Guid.CreateVersion7(), "orders.placed", """{"id":42}""", Now, "order-42");
        await Insert(saved);

        var claimed = Assert.Single(await _store.ClaimAsync(10, Now, Now + Claim, Token));

        Assert.Equivalent(saved, claimed);
    }

    [Fact]
    public async Task Relays_claiming_at_the_same_time_never_get_the_same_message()
    {
        const int Messages = 300;
        for (var i = 0; i < Messages; i++)
        {
            await Insert(Now.AddMilliseconds(-i));
        }

        var claimed = new ConcurrentBag<Guid>();
        var relays = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            IReadOnlyList<OutboxMessage> batch;
            while ((batch = await _store.ClaimAsync(7, Now, Now + Claim, Token)).Count > 0)
            {
                foreach (var message in batch)
                {
                    claimed.Add(message.Id);
                }
            }
        }));
        await Task.WhenAll(relays);

        Assert.Equal(Messages, claimed.Count);
        Assert.Equal(Messages, claimed.Distinct().Count());
    }

    [Fact]
    public async Task A_row_another_transaction_holds_is_skipped_rather_than_waited_for()
    {
        var held = await Insert(Now.AddMinutes(-1));
        var free = await Insert(Now);
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        await using var hold = connection.CreateCommand();
        hold.Transaction = transaction;
        hold.CommandText = $"UPDATE {_table.Name} SET payload = payload WHERE id = @id";
        Parameter(hold, "@id", held);
        await hold.ExecuteNonQueryAsync(Token);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var batch = await _store.ClaimAsync(10, Now, Now + Claim, timeout.Token);

        Assert.Equal([free], batch.Select(m => m.Id));
    }

    protected abstract SqlOutboxStore CreateStore(OutboxTable table);

    protected abstract string CreateTableSql(string table);

    private async Task<Guid> Insert(DateTimeOffset createdAt)
    {
        var message = new OutboxMessage(Guid.CreateVersion7(), "orders.placed", "{}", createdAt);
        await Insert(message);
        return message.Id;
    }

    private async Task Insert(OutboxMessage message)
    {
        await using var insert = DataSource.CreateCommand(
            $"INSERT INTO {_table.Name} (id, type, payload, created_at, ordering_key) "
            + "VALUES (@id, @type, @payload, @created_at, @ordering_key)");
        Parameter(insert, "@id", message.Id);
        Parameter(insert, "@type", message.Type);
        Parameter(insert, "@payload", message.Payload);
        Parameter(insert, "@created_at", message.CreatedAt);
        Parameter(insert, "@ordering_key", message.OrderingKey);
        await insert.ExecuteNonQueryAsync(Token);
    }

    private async Task<string?> LastError(Guid id)
    {
        await using var select = DataSource.CreateCommand($"SELECT last_error FROM {_table.Name} WHERE id = @id");
        Parameter(select, "@id", id);
        return await select.ExecuteScalarAsync(Token) as string;
    }

    private static void Parameter(DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
