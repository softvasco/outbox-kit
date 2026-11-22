using System.Data.Common;
using Microsoft.Data.Sqlite;
using OutboxKit.Inbox;

namespace OutboxKit.Tests.Inbox;

public sealed class InboxExtensionsTests : IAsyncDisposable
{
    private readonly SqliteConnection _connection = new("Data Source=:memory:");

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public ValueTask DisposeAsync() => _connection.DisposeAsync();

    [Fact]
    public async Task A_new_message_runs_the_handler_after_it_is_recorded()
    {
        var calls = new List<string>();
        var inbox = new FakeInbox(alreadyHandled: false, calls);

        var ran = await inbox.HandleOnceAsync("msg-1", "billing", await Transaction(), (_, _) =>
        {
            calls.Add("handle");
            return Task.CompletedTask;
        }, Token);

        Assert.True(ran);
        Assert.Equal(["record msg-1 billing", "handle"], calls);
    }

    [Fact]
    public async Task A_message_already_handled_skips_the_handler()
    {
        var calls = new List<string>();
        var inbox = new FakeInbox(alreadyHandled: true, calls);

        var ran = await inbox.HandleOnceAsync("msg-1", "billing", await Transaction(), (_, _) =>
        {
            calls.Add("handle");
            return Task.CompletedTask;
        }, Token);

        Assert.False(ran);
        Assert.Equal(["record msg-1 billing"], calls);
    }

    [Fact]
    public async Task The_handler_gets_the_transaction_the_message_was_recorded_in()
    {
        var transaction = await Transaction();
        DbTransaction? seen = null;

        await new FakeInbox(alreadyHandled: false, []).HandleOnceAsync("msg-1", "billing", transaction, (tx, _) =>
        {
            seen = tx;
            return Task.CompletedTask;
        }, Token);

        Assert.Same(transaction, seen);
    }

    private async Task<DbTransaction> Transaction()
    {
        await _connection.OpenAsync(Token);
        return await _connection.BeginTransactionAsync(Token);
    }

    private sealed class FakeInbox(bool alreadyHandled, List<string> calls) : IInbox
    {
        public Task<bool> TryRecordAsync(string messageId, string consumer, DbTransaction transaction, CancellationToken cancellationToken = default)
        {
            calls.Add($"record {messageId} {consumer}");
            return Task.FromResult(!alreadyHandled);
        }
    }
}
