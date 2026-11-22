namespace OutboxKit.Inbox;

/// <summary>Records handled messages on PostgreSQL.</summary>
public sealed class PostgreSqlInbox(InboxTable table, TimeProvider clock) : SqlInbox(table, clock, RecordSql)
{
    // a unique violation would abort the caller's whole transaction on PostgreSQL, so the conflict is skipped instead
    private static string RecordSql(string table) => $"""
        INSERT INTO {table} (message_id, consumer, handled_at)
        VALUES (@message_id, @consumer, @handled_at)
        ON CONFLICT (message_id, consumer) DO NOTHING
        """;
}
