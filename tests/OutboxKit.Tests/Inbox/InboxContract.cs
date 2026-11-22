using System.Data.Common;
using Microsoft.Extensions.Time.Testing;
using OutboxKit.Inbox;

namespace OutboxKit.Tests.Inbox;

/// <summary>What every database inbox must do; each engine runs these against a real server.</summary>
public abstract class InboxContract : IAsyncLifetime
{
    private const string Consumer = "billing";
    private static readonly DateTimeOffset Now = new(2026, 11, 22, 0, 0, 0, TimeSpan.Zero);

    private readonly InboxTable _table = new($"inbox_{Guid.NewGuid():N}");
    private IInbox _inbox = null!;
    private IInboxRetentionStore _retention = null!;

    protected static CancellationToken Token => TestContext.Current.CancellationToken;

    protected abstract DbDataSource DataSource { get; }

    protected abstract IInbox CreateInbox(InboxTable table, TimeProvider clock);

    protected abstract IInboxRetentionStore CreateRetentionStore(InboxTable table);

    protected abstract string CreateTableSql(string table);

    public async ValueTask InitializeAsync()
    {
        await using var create = DataSource.CreateCommand(CreateTableSql(_table.Name));
        await create.ExecuteNonQueryAsync(Token);
        _inbox = CreateInbox(_table, new FakeTimeProvider(Now));
        _retention = CreateRetentionStore(_table);
    }

    public async ValueTask DisposeAsync()
    {
        await using var drop = DataSource.CreateCommand($"DROP TABLE {_table.Name}");
        await drop.ExecuteNonQueryAsync(CancellationToken.None);
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task A_message_is_recorded_once_and_its_redelivery_is_refused()
    {
        var first = await RecordAndCommit("msg-1", Consumer);
        var redelivery = await RecordAndCommit("msg-1", Consumer);

        Assert.True(first);
        Assert.False(redelivery);
    }

    [Fact]
    public async Task Each_consumer_handles_the_same_message_once()
    {
        Assert.True(await RecordAndCommit("msg-1", "billing"));
        Assert.True(await RecordAndCommit("msg-1", "shipping"));
        Assert.False(await RecordAndCommit("msg-1", "shipping"));
    }

    [Fact]
    public async Task A_handler_that_rolls_back_leaves_the_message_to_be_handled_again()
    {
        await using (var connection = await DataSource.OpenConnectionAsync(Token))
        await using (var transaction = await connection.BeginTransactionAsync(Token))
        {
            Assert.True(await _inbox.TryRecordAsync("msg-1", Consumer, transaction, Token));
            await transaction.RollbackAsync(Token);
        }

        Assert.True(await RecordAndCommit("msg-1", Consumer));
    }

    [Fact]
    public async Task Two_deliveries_at_once_record_the_message_once()
    {
        await using var firstConnection = await DataSource.OpenConnectionAsync(Token);
        await using var first = await firstConnection.BeginTransactionAsync(Token);
        Assert.True(await _inbox.TryRecordAsync("msg-1", Consumer, first, Token));

        // the second delivery has to wait for the first transaction, otherwise both handlers would run
        var second = RecordAndCommit("msg-1", Consumer);
        await first.CommitAsync(Token);

        Assert.False(await second);
    }

    [Fact]
    public async Task A_delivery_waiting_on_one_that_rolls_back_records_the_message_itself()
    {
        await using var firstConnection = await DataSource.OpenConnectionAsync(Token);
        await using var first = await firstConnection.BeginTransactionAsync(Token);
        Assert.True(await _inbox.TryRecordAsync("msg-1", Consumer, first, Token));

        var second = RecordAndCommit("msg-1", Consumer);
        await first.RollbackAsync(Token);

        Assert.True(await second);
    }

    [Fact]
    public async Task Only_rows_handled_before_the_cutoff_are_deleted()
    {
        await Insert("old", Now.AddDays(-31));
        await Insert("recent", Now.AddDays(-1));

        var deleted = await _retention.DeleteHandledAsync(Now.AddDays(-30), 10, Token);

        Assert.Equal(1, deleted);
        Assert.Equal(["recent"], await MessageIds());
    }

    [Fact]
    public async Task A_delete_stops_at_the_batch_size()
    {
        for (var i = 0; i < 5; i++)
        {
            await Insert($"msg-{i}", Now.AddDays(-31));
        }

        var first = await _retention.DeleteHandledAsync(Now, 3, Token);
        var second = await _retention.DeleteHandledAsync(Now, 3, Token);

        Assert.Equal((3, 2), (first, second));
    }

    [Fact]
    public async Task A_row_another_transaction_holds_is_skipped_by_the_delete()
    {
        await Insert("held", Now.AddDays(-31));
        await Insert("free", Now.AddDays(-31));
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        await using var hold = connection.CreateCommand();
        hold.Transaction = transaction;
        hold.CommandText = $"UPDATE {_table.Name} SET handled_at = handled_at WHERE message_id = 'held'";
        await hold.ExecuteNonQueryAsync(Token);

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        var deleted = await _retention.DeleteHandledAsync(Now, 10, timeout.Token);
        await transaction.RollbackAsync(Token);

        Assert.Equal(1, deleted);
        Assert.Equal(["held"], await MessageIds());
    }

    private async Task Insert(string messageId, DateTimeOffset handledAt)
    {
        await using var insert = DataSource.CreateCommand(
            $"INSERT INTO {_table.Name} (message_id, consumer, handled_at) VALUES (@message_id, @consumer, @handled_at)");
        Parameter(insert, "@message_id", messageId);
        Parameter(insert, "@consumer", Consumer);
        Parameter(insert, "@handled_at", handledAt);
        await insert.ExecuteNonQueryAsync(Token);
    }

    private async Task<List<string>> MessageIds()
    {
        await using var select = DataSource.CreateCommand($"SELECT message_id FROM {_table.Name} ORDER BY message_id");
        await using var reader = await select.ExecuteReaderAsync(Token);
        var ids = new List<string>();
        while (await reader.ReadAsync(Token))
        {
            ids.Add(reader.GetString(0));
        }

        return ids;
    }

    private static void Parameter(DbCommand command, string name, object value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    private async Task<bool> RecordAndCommit(string messageId, string consumer)
    {
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        var recorded = await _inbox.TryRecordAsync(messageId, consumer, transaction, Token);
        await transaction.CommitAsync(Token);
        return recorded;
    }
}
