using System.Data.Common;
using Microsoft.Data.Sqlite;
using OutboxKit.Inbox;

namespace OutboxKit.Tests.Inbox;

public sealed class ParkedMessagesExtensionsTests : IAsyncDisposable
{
    private static readonly ParkedMessage Parked =
        new("msg-1", "billing", "order-placed", """{"orderId":7}""", "customer 7 not found", DateTimeOffset.UnixEpoch);

    private readonly SqliteConnection _connection = new("Data Source=:memory:");
    private readonly List<string> _calls = [];

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public ValueTask DisposeAsync() => _connection.DisposeAsync();

    [Fact]
    public async Task A_replay_takes_the_message_records_it_and_runs_the_handler_in_one_transaction()
    {
        var transaction = await Transaction();
        var seen = new List<DbTransaction>();

        var ran = await new FakeParking(Parked, _calls, seen).ReplayAsync(
            new FakeInbox(alreadyHandled: false, _calls, seen), "msg-1", "billing", transaction, Handle(seen), Token);

        Assert.True(ran);
        Assert.Equal(["take msg-1", "record msg-1 billing", "handle order-placed"], _calls);
        Assert.All(seen, tx => Assert.Same(transaction, tx));
    }

    [Fact]
    public async Task A_message_that_is_not_parked_runs_nothing()
    {
        var ran = await new FakeParking(null, _calls, []).ReplayAsync(
            new FakeInbox(alreadyHandled: false, _calls, []), "msg-1", "billing", await Transaction(), Handle([]), Token);

        Assert.False(ran);
        Assert.Equal(["take msg-1"], _calls);
    }

    [Fact]
    public async Task A_message_handled_since_it_was_parked_is_removed_without_running_the_handler()
    {
        var ran = await new FakeParking(Parked, _calls, []).ReplayAsync(
            new FakeInbox(alreadyHandled: true, _calls, []), "msg-1", "billing", await Transaction(), Handle([]), Token);

        Assert.False(ran);
        Assert.Equal(["take msg-1", "record msg-1 billing"], _calls);
    }

    private Func<ParkedMessage, DbTransaction, CancellationToken, Task> Handle(List<DbTransaction> seen) => (message, tx, _) =>
    {
        _calls.Add($"handle {message.Type}");
        seen.Add(tx);
        return Task.CompletedTask;
    };

    private async Task<DbTransaction> Transaction()
    {
        await _connection.OpenAsync(Token);
        return await _connection.BeginTransactionAsync(Token);
    }

    private sealed class FakeParking(ParkedMessage? parked, List<string> calls, List<DbTransaction> seen) : IParkedMessages
    {
        public Task ParkAsync(ParkedMessage message, DbTransaction transaction, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<ParkedMessage>> ListAsync(string consumer, int limit, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public Task<ParkedMessage?> TakeAsync(
            string messageId, string consumer, DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            calls.Add($"take {messageId}");
            seen.Add(transaction);
            return Task.FromResult(parked);
        }
    }

    private sealed class FakeInbox(bool alreadyHandled, List<string> calls, List<DbTransaction> seen) : IInbox
    {
        public Task<bool> TryRecordAsync(string messageId, string consumer, DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            calls.Add($"record {messageId} {consumer}");
            seen.Add(transaction);
            return Task.FromResult(!alreadyHandled);
        }
    }
}
