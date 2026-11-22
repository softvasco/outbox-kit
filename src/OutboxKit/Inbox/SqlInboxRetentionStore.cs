using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>Inbox cleanup that is the same on every engine; only the delete differs.</summary>
public abstract class SqlInboxRetentionStore : IInboxRetentionStore
{
    private readonly DbDataSource _dataSource;
    private readonly string _deleteHandled;

    private protected SqlInboxRetentionStore(DbDataSource dataSource, InboxTable table, Func<string, string> deleteHandledSql)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(table);

        _dataSource = dataSource;
        _deleteHandled = deleteHandledSql(table.Name);
    }

    public async Task<int> DeleteHandledAsync(DateTimeOffset cutoff, int batchSize, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);

        await using var command = _dataSource.CreateCommand(_deleteHandled);
        command.Add("@cutoff", cutoff);
        command.Add("@batch_size", batchSize);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
