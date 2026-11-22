using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>Deletes old inbox rows on SQL Server.</summary>
public sealed class SqlServerInboxRetentionStore(DbDataSource dataSource, InboxTable table)
    : SqlInboxRetentionStore(dataSource, table, DeleteHandledSql)
{
    // small batches and ROWLOCK keep it from escalating to a table lock while consumers insert
    private static string DeleteHandledSql(string table) => $"""
        DELETE TOP (@batch_size) FROM {table} WITH (ROWLOCK, READPAST)
        WHERE handled_at < @cutoff
        """;
}
