using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>The part of a database inbox that is the same on every engine; only the insert differs.</summary>
public abstract class SqlInbox : IInbox
{
    private readonly TimeProvider _clock;
    private readonly string _record;

    // recordSql builds an insert that affects no row when the message is already there, instead of failing
    private protected SqlInbox(InboxTable table, TimeProvider clock, Func<string, string> recordSql)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(clock);

        _clock = clock;
        _record = recordSql(table.Name);
    }

    public async Task<bool> TryRecordAsync(
        string messageId, string consumer, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ArgumentNullException.ThrowIfNull(transaction);

        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The transaction has already completed, so the message can't be recorded with it.");

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = _record;
        command.Add("@message_id", messageId);
        command.Add("@consumer", consumer);
        command.Add("@handled_at", _clock.GetUtcNow());

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
