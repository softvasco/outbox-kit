using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>Parked messages on PostgreSQL.</summary>
public sealed class PostgreSqlParkedMessages(DbDataSource dataSource, ParkedMessageTable table)
    : SqlParkedMessages(dataSource, table, ParkSql, ListSql, TakeSql)
{
    // a unique violation would abort the caller's transaction, so parking twice is skipped instead
    private static string ParkSql(string table, string columns) => $"""
        INSERT INTO {table} ({columns})
        VALUES (@message_id, @consumer, @type, @payload, @error, @parked_at)
        ON CONFLICT (message_id, consumer) DO NOTHING
        """;

    private static string ListSql(string table, string columns) => $"""
        SELECT {columns} FROM {table}
        WHERE consumer = @consumer
        ORDER BY parked_at, message_id
        LIMIT @limit
        """;

    private static string TakeSql(string table, string columns) => $"""
        DELETE FROM {table}
        WHERE message_id = @message_id AND consumer = @consumer
        RETURNING {columns}
        """;
}
