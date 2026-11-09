using System.Data.Common;

namespace OutboxKit.Stores;

/// <summary>The part of a database store that is the same on every engine; only the claim query differs.</summary>
public abstract class SqlOutboxStore : IOutboxStore, IOutboxRetentionStore
{
    private readonly DbDataSource _dataSource;
    private readonly string _claim;
    private readonly string _deleteDispatched;
    private readonly string _markDispatched;
    private readonly string _markFailed;
    private readonly string _markDeadLettered;

    // claimSql builds one statement that sets claimed_until on the batch and returns its rows
    private protected SqlOutboxStore(
        DbDataSource dataSource, OutboxTable table, Func<string, string> claimSql, Func<string, string> deleteDispatchedSql)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(table);

        _dataSource = dataSource;
        _claim = claimSql(table.Name);
        _deleteDispatched = deleteDispatchedSql(table.Name);
        _markDispatched = $"UPDATE {table.Name} SET dispatched_at = @dispatched_at, claimed_until = NULL WHERE id = @id";

        // the retry time goes in claimed_until, so the claim query needs no second condition for it
        _markFailed = $"UPDATE {table.Name} SET attempts = attempts + 1, claimed_until = @retry_at, last_error = @error WHERE id = @id";
        _markDeadLettered = $"UPDATE {table.Name} SET attempts = attempts + 1, dead_lettered_at = @dead_lettered_at, "
            + "claimed_until = NULL, last_error = @error WHERE id = @id";
    }

    // claimed or not, an older pending message holds its key, so two relays never share one
    private protected static string EarlierPendingForKey(string table) => $"""
        SELECT 1 FROM {table} earlier
                    WHERE earlier.ordering_key = m.ordering_key
                        AND earlier.dispatched_at IS NULL AND earlier.dead_lettered_at IS NULL
                        AND (earlier.created_at < m.created_at OR (earlier.created_at = m.created_at AND earlier.id < m.id))
        """;

    public async Task<IReadOnlyList<OutboxMessage>> ClaimAsync(
        int batchSize, DateTimeOffset now, DateTimeOffset claimedUntil, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);

        await using var command = _dataSource.CreateCommand(_claim);
        command.Add("@batch_size", batchSize);
        command.Add("@now", now);
        command.Add("@claimed_until", claimedUntil);

        var batch = new List<OutboxMessage>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            batch.Add(new OutboxMessage(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetFieldValue<DateTimeOffset>(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetInt32(5)));
        }

        // neither RETURNING nor OUTPUT promises an order
        return [.. batch.OrderBy(m => m.CreatedAt).ThenBy(m => m.Id)];
    }

    public async Task<int> DeleteDispatchedAsync(DateTimeOffset cutoff, int batchSize, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);

        await using var command = _dataSource.CreateCommand(_deleteDispatched);
        command.Add("@cutoff", cutoff);
        command.Add("@batch_size", batchSize);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        await using var command = _dataSource.CreateCommand(_markDispatched);
        command.Add("@dispatched_at", dispatchedAt);
        command.Add("@id", message.Id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkFailedAsync(OutboxMessage message, DateTimeOffset retryAt, string reason, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(reason);

        await using var command = _dataSource.CreateCommand(_markFailed);
        command.Add("@retry_at", retryAt);
        command.Add("@error", reason);
        command.Add("@id", message.Id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task MarkDeadLetteredAsync(
        OutboxMessage message, DateTimeOffset deadLetteredAt, string reason, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(reason);

        await using var command = _dataSource.CreateCommand(_markDeadLettered);
        command.Add("@dead_lettered_at", deadLetteredAt);
        command.Add("@error", reason);
        command.Add("@id", message.Id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
