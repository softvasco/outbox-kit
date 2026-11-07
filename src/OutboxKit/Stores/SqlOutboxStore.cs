using System.Data.Common;

namespace OutboxKit.Stores;

/// <summary>The part of a database store that is the same on every engine; only the claim query differs.</summary>
public abstract class SqlOutboxStore : IOutboxStore
{
    private readonly DbDataSource _dataSource;
    private readonly string _claim;
    private readonly string _markDispatched;

    // claimSql builds one statement that sets claimed_until on the batch and returns its rows
    private protected SqlOutboxStore(DbDataSource dataSource, OutboxTable table, Func<string, string> claimSql)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(table);

        _dataSource = dataSource;
        _claim = claimSql(table.Name);
        _markDispatched = $"UPDATE {table.Name} SET dispatched_at = @dispatched_at, claimed_until = NULL WHERE id = @id";
    }

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
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }

        // neither RETURNING nor OUTPUT promises an order
        return [.. batch.OrderBy(m => m.CreatedAt).ThenBy(m => m.Id)];
    }

    public async Task MarkDispatchedAsync(OutboxMessage message, DateTimeOffset dispatchedAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);

        await using var command = _dataSource.CreateCommand(_markDispatched);
        command.Add("@dispatched_at", dispatchedAt);
        command.Add("@id", message.Id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
