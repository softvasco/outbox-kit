using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>Deletes old inbox rows on PostgreSQL. Pass an NpgsqlDataSource.</summary>
public sealed class PostgreSqlInboxRetentionStore(DbDataSource dataSource, InboxTable table)
    : SqlInboxRetentionStore(dataSource, table, DeleteHandledSql)
{
    // DELETE has no LIMIT on PostgreSQL, so the batch comes from a subquery
    private static string DeleteHandledSql(string table) => $"""
        DELETE FROM {table}
        WHERE (message_id, consumer) IN (
            SELECT message_id, consumer FROM {table}
            WHERE handled_at < @cutoff
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED)
        """;
}
