using System.Data.Common;

namespace OutboxKit.Stores;

/// <summary>Claims batches on PostgreSQL. Pass an NpgsqlDataSource.</summary>
public sealed class PostgreSqlOutboxStore(DbDataSource dataSource, OutboxTable table)
    : SqlOutboxStore(dataSource, table, ClaimSql, DeleteDispatchedSql)
{
    // DELETE has no LIMIT on PostgreSQL, so the batch comes from a subquery
    private static string DeleteDispatchedSql(string table) => $"""
        DELETE FROM {table}
        WHERE id IN (
            SELECT id FROM {table}
            WHERE dispatched_at < @cutoff
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED)
        """;

    // without SKIP LOCKED two relays polling at once pick the same ids, or queue behind each other
    private static string ClaimSql(string table) => $"""
        UPDATE {table} SET claimed_until = @claimed_until
        WHERE id IN (
            SELECT id FROM {table} m
            WHERE dispatched_at IS NULL AND dead_lettered_at IS NULL AND (claimed_until IS NULL OR claimed_until <= @now)
                AND NOT EXISTS ({SqlOutboxStore.EarlierPendingForKey(table)})
            ORDER BY created_at, id
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED)
        RETURNING id, type, payload, created_at, ordering_key, attempts
        """;
}
