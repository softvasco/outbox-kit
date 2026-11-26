using Npgsql;

namespace OutboxKit.CrashHost;

public sealed record OrderPlaced(int OrderId);

/// <summary>Stands in for the broker: every send is a row, so a test can count what arrived and how often.</summary>
public sealed class BrokerTableTransport(NpgsqlDataSource dataSource, string table) : IMessageTransport
{
    public async Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand($"INSERT INTO {table} (message_id, type) VALUES (@id, @type)");
        command.Parameters.AddWithValue("id", message.Id);
        command.Parameters.AddWithValue("type", message.Type);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}

/// <summary>Lets the broker have the message, then takes the process down before the row is marked as sent.</summary>
public sealed class KillAfterSend(IMessageTransport inner) : IMessageTransport
{
    public async Task SendAsync(OutboxMessage message, CancellationToken cancellationToken)
    {
        await inner.SendAsync(message, cancellationToken);
        Environment.FailFast("crash host: killed after the send, before the row was marked");
    }
}
