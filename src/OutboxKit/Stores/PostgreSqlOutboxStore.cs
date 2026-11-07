using System.Data.Common;

namespace OutboxKit.Stores;

/// <summary>Claims batches on PostgreSQL. Pass an NpgsqlDataSource.</summary>
public sealed class PostgreSqlOutboxStore(DbDataSource dataSource, OutboxTable table) : SqlOutboxStore(dataSource, table, ClaimSql)
{
    // without SKIP LOCKED two relays polling at once pick the same ids, or queue behind each other
    private static string ClaimSql(string table) => $"""
        UPDATE {table} SET claimed_until = @claimed_until
        WHERE id IN (
            SELECT id FROM {table}
            WHERE dispatched_at IS NULL AND dead_lettered_at IS NULL AND (claimed_until IS NULL OR claimed_until <= @now)
            ORDER BY created_at, id
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED)
        RETURNING id, type, payload, created_at, ordering_key, attempts
        """;
}
