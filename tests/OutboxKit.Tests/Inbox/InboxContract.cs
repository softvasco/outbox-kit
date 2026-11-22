using System.Data.Common;
using Microsoft.Extensions.Time.Testing;
using OutboxKit.Inbox;

namespace OutboxKit.Tests.Inbox;

/// <summary>What every database inbox must do; each engine runs these against a real server.</summary>
public abstract class InboxContract : IAsyncLifetime
{
    private const string Consumer = "billing";

    private readonly InboxTable _table = new($"inbox_{Guid.NewGuid():N}");
    private IInbox _inbox = null!;

    protected static CancellationToken Token => TestContext.Current.CancellationToken;

    protected abstract DbDataSource DataSource { get; }

    protected abstract IInbox CreateInbox(InboxTable table, TimeProvider clock);

    protected abstract string CreateTableSql(string table);

    public async ValueTask InitializeAsync()
    {
        await using var create = DataSource.CreateCommand(CreateTableSql(_table.Name));
        await create.ExecuteNonQueryAsync(Token);
        _inbox = CreateInbox(_table, new FakeTimeProvider(new DateTimeOffset(2026, 11, 22, 0, 0, 0, TimeSpan.Zero)));
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

    private async Task<bool> RecordAndCommit(string messageId, string consumer)
    {
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        var recorded = await _inbox.TryRecordAsync(messageId, consumer, transaction, Token);
        await transaction.CommitAsync(Token);
        return recorded;
    }
}
