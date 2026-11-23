using System.Data.Common;
using OutboxKit.Inbox;

namespace OutboxKit.Tests.Inbox;

/// <summary>What every parked message store must do, run against a real server.</summary>
public abstract class ParkedMessagesContract : IAsyncLifetime
{
    private const string Consumer = "billing";
    private static readonly DateTimeOffset Now = new(2026, 11, 23, 0, 0, 0, TimeSpan.Zero);

    private readonly ParkedMessageTable _table = new($"parked_{Guid.NewGuid():N}");
    private IParkedMessages _parked = null!;

    protected static CancellationToken Token => TestContext.Current.CancellationToken;

    protected abstract DbDataSource DataSource { get; }

    protected abstract IParkedMessages CreateStore(ParkedMessageTable table);

    protected abstract string CreateTableSql(string table);

    public async ValueTask InitializeAsync()
    {
        await using var create = DataSource.CreateCommand(CreateTableSql(_table.Name));
        await create.ExecuteNonQueryAsync(Token);
        _parked = CreateStore(_table);
    }

    public async ValueTask DisposeAsync()
    {
        await using var drop = DataSource.CreateCommand($"DROP TABLE {_table.Name}");
        await drop.ExecuteNonQueryAsync(CancellationToken.None);
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task A_parked_message_is_listed_with_everything_needed_to_replay_it()
    {
        await ParkAndCommit(Message("msg-1", parkedAt: Now));

        var listed = Assert.Single(await _parked.ListAsync(Consumer, 10, Token));

        Assert.Equal(
            ("msg-1", Consumer, "order-placed", """{"orderId":7}""", "customer 7 not found", Now),
            (listed.MessageId, listed.Consumer, listed.Type, listed.Payload, listed.Error, listed.ParkedAt));
    }

    [Fact]
    public async Task Parking_the_same_message_again_keeps_the_first_error()
    {
        await ParkAndCommit(Message("msg-1", error: "first"));
        await ParkAndCommit(Message("msg-1", error: "second"));

        var listed = Assert.Single(await _parked.ListAsync(Consumer, 10, Token));
        Assert.Equal("first", listed.Error);
    }

    [Fact]
    public async Task A_park_that_rolls_back_leaves_nothing_parked()
    {
        await using (var connection = await DataSource.OpenConnectionAsync(Token))
        await using (var transaction = await connection.BeginTransactionAsync(Token))
        {
            await _parked.ParkAsync(Message("msg-1"), transaction, Token);
            await transaction.RollbackAsync(Token);
        }

        Assert.Empty(await _parked.ListAsync(Consumer, 10, Token));
    }

    [Fact]
    public async Task The_list_has_only_this_consumer_oldest_first_up_to_the_limit()
    {
        await ParkAndCommit(Message("newest", parkedAt: Now.AddMinutes(2)));
        await ParkAndCommit(Message("oldest", parkedAt: Now));
        await ParkAndCommit(Message("middle", parkedAt: Now.AddMinutes(1)));
        await ParkAndCommit(Message("other", consumer: "shipping"));

        var listed = await _parked.ListAsync(Consumer, 2, Token);

        Assert.Equal(["oldest", "middle"], listed.Select(message => message.MessageId));
    }

    [Fact]
    public async Task Taking_a_message_removes_it_and_a_second_take_finds_nothing()
    {
        await ParkAndCommit(Message("msg-1"));

        var taken = await TakeAndCommit("msg-1");
        var again = await TakeAndCommit("msg-1");

        Assert.Equal("msg-1", taken?.MessageId);
        Assert.Null(again);
        Assert.Empty(await _parked.ListAsync(Consumer, 10, Token));
    }

    [Fact]
    public async Task A_take_that_rolls_back_leaves_the_message_parked()
    {
        await ParkAndCommit(Message("msg-1"));

        await using (var connection = await DataSource.OpenConnectionAsync(Token))
        await using (var transaction = await connection.BeginTransactionAsync(Token))
        {
            Assert.NotNull(await _parked.TakeAsync("msg-1", Consumer, transaction, Token));
            await transaction.RollbackAsync(Token);
        }

        Assert.Single(await _parked.ListAsync(Consumer, 10, Token));
    }

    [Fact]
    public async Task Two_takes_at_once_hand_the_message_to_one_of_them()
    {
        await ParkAndCommit(Message("msg-1"));
        await using var firstConnection = await DataSource.OpenConnectionAsync(Token);
        await using var first = await firstConnection.BeginTransactionAsync(Token);
        Assert.NotNull(await _parked.TakeAsync("msg-1", Consumer, first, Token));

        // two people pressing replay must not run the handler twice
        var second = TakeAndCommit("msg-1");
        await first.CommitAsync(Token);

        Assert.Null(await second);
    }

    private static ParkedMessage Message(
        string messageId, string consumer = Consumer, string error = "customer 7 not found", DateTimeOffset? parkedAt = null) =>
        new(messageId, consumer, "order-placed", """{"orderId":7}""", error, parkedAt ?? Now);

    private async Task ParkAndCommit(ParkedMessage message)
    {
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        await _parked.ParkAsync(message, transaction, Token);
        await transaction.CommitAsync(Token);
    }

    private async Task<ParkedMessage?> TakeAndCommit(string messageId)
    {
        await using var connection = await DataSource.OpenConnectionAsync(Token);
        await using var transaction = await connection.BeginTransactionAsync(Token);
        var taken = await _parked.TakeAsync(messageId, Consumer, transaction, Token);
        await transaction.CommitAsync(Token);
        return taken;
    }
}
