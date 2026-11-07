using System.Data.Common;

namespace OutboxKit.Stores;

/// <summary>Claims batches on SQL Server. Pass <c>SqlClientFactory.Instance.CreateDataSource(connectionString)</c>.</summary>
public sealed class SqlServerOutboxStore(DbDataSource dataSource, OutboxTable table) : SqlOutboxStore(dataSource, table, ClaimSql)
{
    // UPDLOCK holds what it reads, READPAST skips rows another relay holds, ROWLOCK avoids page locks
    private static string ClaimSql(string table) => $"""
        WITH batch AS (
            SELECT TOP (@batch_size) id, type, payload, created_at, ordering_key, attempts, claimed_until
            FROM {table} m WITH (UPDLOCK, READPAST, ROWLOCK)
            WHERE dispatched_at IS NULL AND dead_lettered_at IS NULL AND (claimed_until IS NULL OR claimed_until <= @now)
                AND NOT EXISTS ({SqlOutboxStore.EarlierPendingForKey(table)})
            ORDER BY created_at, id)
        UPDATE batch SET claimed_until = @claimed_until
        OUTPUT inserted.id, inserted.type, inserted.payload, inserted.created_at, inserted.ordering_key, inserted.attempts
        """;
}
