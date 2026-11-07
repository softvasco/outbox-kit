using System.Data.Common;

namespace OutboxKit;

/// <summary>Writes outbox rows with plain ADO.NET on the connection and transaction the caller already has.</summary>
public sealed class AdoNetOutbox : IOutbox
{
    private readonly OutboxMessageFactory _factory;
    private readonly string _insert;

    public AdoNetOutbox(OutboxMessageFactory factory, OutboxTable table)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(table);

        _factory = factory;
        // @name parameters work on both SqlClient and Npgsql
        _insert = $"INSERT INTO {table.Name} (id, type, payload, created_at, ordering_key) "
            + "VALUES (@id, @type, @payload, @created_at, @ordering_key)";
    }

    public async Task<OutboxMessage> EnqueueAsync(
        object message, DbTransaction transaction, string? orderingKey = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(transaction);

        // a committed or rolled back transaction has no connection; writing outside it would lose the guarantee
        var connection = transaction.Connection
            ?? throw new InvalidOperationException("The transaction has already completed, so the message can't be saved with it.");

        var outboxMessage = _factory.Create(message, orderingKey);

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = _insert;
        command.Add("@id", outboxMessage.Id);
        command.Add("@type", outboxMessage.Type);
        command.Add("@payload", outboxMessage.Payload);
        command.Add("@created_at", outboxMessage.CreatedAt);
        command.Add("@ordering_key", outboxMessage.OrderingKey);

        await command.ExecuteNonQueryAsync(cancellationToken);
        return outboxMessage;
    }
}
