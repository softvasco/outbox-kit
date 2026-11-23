using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>The part of the parked message store that is the same on every engine.</summary>
public abstract class SqlParkedMessages : IParkedMessages
{
    private const string Columns = "message_id, consumer, type, payload, error, parked_at";

    private readonly DbDataSource _dataSource;
    private readonly string _park;
    private readonly string _list;
    private readonly string _take;

    private protected SqlParkedMessages(
        DbDataSource dataSource,
        ParkedMessageTable table,
        Func<string, string, string> parkSql,
        Func<string, string, string> listSql,
        Func<string, string, string> takeSql)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(table);

        _dataSource = dataSource;
        _park = parkSql(table.Name, Columns);
        _list = listSql(table.Name, Columns);
        _take = takeSql(table.Name, Columns);
    }

    public async Task ParkAsync(ParkedMessage message, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        await using var command = CommandFor(transaction, _park);
        command.Add("@message_id", message.MessageId);
        command.Add("@consumer", message.Consumer);
        command.Add("@type", message.Type);
        command.Add("@payload", message.Payload);
        command.Add("@error", message.Error);
        command.Add("@parked_at", message.ParkedAt);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<ParkedMessage>> ListAsync(string consumer, int limit, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);

        await using var command = _dataSource.CreateCommand(_list);
        command.Add("@consumer", consumer);
        command.Add("@limit", limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var messages = new List<ParkedMessage>();
        while (await reader.ReadAsync(cancellationToken))
        {
            messages.Add(Read(reader));
        }

        return messages;
    }

    public async Task<ParkedMessage?> TakeAsync(
        string messageId, string consumer, DbTransaction transaction, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumer);

        await using var command = CommandFor(transaction, _take);
        command.Add("@message_id", messageId);
        command.Add("@consumer", consumer);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? Read(reader) : null;
    }

    private static DbCommand CommandFor(DbTransaction transaction, string sql)
    {
        ArgumentNullException.ThrowIfNull(transaction);

        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The transaction has already completed, so the parked message can't be used with it.");

        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        return command;
    }

    private static ParkedMessage Read(DbDataReader reader) => new(
        reader.GetString(0),
        reader.GetString(1),
        reader.GetString(2),
        reader.GetString(3),
        reader.GetString(4),
        reader.GetFieldValue<DateTimeOffset>(5));
}
