using System.Data.Common;

namespace OutboxKit.Inbox;

/// <summary>Parked messages on SQL Server.</summary>
public sealed class SqlServerParkedMessages(DbDataSource dataSource, ParkedMessageTable table)
    : SqlParkedMessages(dataSource, table, ParkSql, ListSql, TakeSql)
{
    // same locking as the inbox insert, so parking one message twice at once can't hit the key
    private static string ParkSql(string table, string columns) => $"""
        INSERT INTO {table} ({columns})
        SELECT @message_id, @consumer, @type, @payload, @error, @parked_at
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} WITH (UPDLOCK, HOLDLOCK)
            WHERE message_id = @message_id AND consumer = @consumer)
        """;

    private static string ListSql(string table, string columns) => $"""
        SELECT TOP (@limit) {columns} FROM {table}
        WHERE consumer = @consumer
        ORDER BY parked_at, message_id
        """;

    private static string TakeSql(string table, string columns) => $"""
        DELETE FROM {table}
        OUTPUT {string.Join(", ", columns.Split(", ").Select(column => "deleted." + column))}
        WHERE message_id = @message_id AND consumer = @consumer
        """;
}
