namespace OutboxKit.Inbox;

/// <summary>Records handled messages on SQL Server.</summary>
public sealed class SqlServerInbox(InboxTable table, TimeProvider clock) : SqlInbox(table, clock, RecordSql)
{
    // HOLDLOCK locks the key even when the row isn't there yet, so a second delivery waits here instead of
    // reading past the first one under read committed snapshot and failing on the primary key
    private static string RecordSql(string table) => $"""
        INSERT INTO {table} (message_id, consumer, handled_at)
        SELECT @message_id, @consumer, @handled_at
        WHERE NOT EXISTS (
            SELECT 1 FROM {table} WITH (UPDLOCK, HOLDLOCK)
            WHERE message_id = @message_id AND consumer = @consumer)
        """;
}
