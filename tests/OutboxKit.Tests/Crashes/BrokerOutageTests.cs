using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using OutboxKit.RabbitMQ;
using OutboxKit.Stores;
using OutboxKit.Tests.Stores;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace OutboxKit.Tests.Crashes;

// docker pause freezes the broker, so sends hang instead of failing fast
public sealed class BrokerOutageTests(PostgreSqlServer server) : IClassFixture<PostgreSqlServer>, IAsyncLifetime
{
    private const int MessagesPerKey = 10;
    private static readonly string[] Keys = ["account-1", "account-2", "account-3"];

    private readonly RabbitMqContainer _broker = new RabbitMqBuilder("rabbitmq:4-alpine").Build();
    private readonly string _table = $"outbox_{Guid.NewGuid():N}";
    private readonly string _queue = $"ledger-{Guid.NewGuid():N}";
    private IConnection _connection = null!;

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        await _broker.StartAsync(Token);
        _connection = await new ConnectionFactory { Uri = new Uri(_broker.GetConnectionString()) }.CreateConnectionAsync(Token);
        await using (var channel = await _connection.CreateChannelAsync(cancellationToken: Token))
        {
            await channel.QueueDeclareAsync(_queue, durable: true, exclusive: false, autoDelete: false, cancellationToken: Token);
        }

        await Execute($"""
            CREATE TABLE {_table} (
                id uuid PRIMARY KEY, type varchar(256) NOT NULL, payload text NOT NULL, created_at timestamptz NOT NULL,
                ordering_key varchar(256), claimed_until timestamptz, dispatched_at timestamptz,
                attempts int NOT NULL DEFAULT 0, dead_lettered_at timestamptz, last_error text);
            """);
    }

    public async ValueTask DisposeAsync()
    {
        await Execute($"DROP TABLE {_table};");
        await _connection.DisposeAsync();
        await _broker.DisposeAsync();
    }

    [Fact]
    public async Task After_the_broker_comes_back_every_message_goes_out_in_order_per_key()
    {
        var saved = await SaveInterleavedMessages();
        await _broker.PauseAsync(Token);
        await using var transport = new RabbitMqTransport(
            _connection, new RabbitMqTransportOptions { RoutingKey = _ => _queue, ConfirmTimeout = TimeSpan.FromSeconds(1) });
        using var relay = Relay(transport);

        await relay.StartAsync(Token);
        await WaitUntil(async () => await Scalar($"SELECT coalesce(sum(attempts), 0) FROM {_table}") >= 5, TimeSpan.FromSeconds(30));
        Assert.Equal(0L, await Scalar($"SELECT count(*) FROM {_table} WHERE dispatched_at IS NOT NULL"));

        // long enough for the first message of each key to fail several times
        await Task.Delay(TimeSpan.FromSeconds(10), Token);
        await _broker.UnpauseAsync(Token);
        await WaitUntil(async () => await Scalar($"SELECT count(*) FROM {_table} WHERE dispatched_at IS NULL") == 0, TimeSpan.FromSeconds(60));
        await relay.StopAsync(Token);

        var received = await Drain();
        Assert.Equal(0L, await Scalar($"SELECT count(*) FROM {_table} WHERE dead_lettered_at IS NOT NULL"));
        Assert.Equal(saved.Select(m => m.Id).Order(), received.Select(m => m.Id).Distinct().Order());
        foreach (var key in Keys)
        {
            // a timed-out send may still reach the broker: a repeat is fine, an overtake isn't
            var firstArrivals = received.Where(m => m.Key == key).Select(m => m.Id).Distinct();
            Assert.Equal(saved.Where(m => m.Key == key).Select(m => m.Id), firstArrivals);
        }
    }

    private OutboxDispatcherService Relay(IMessageTransport transport)
    {
        var options = new OutboxDispatcherOptions
        {
            PollingInterval = TimeSpan.FromMilliseconds(100),
            BatchSize = 10,
            ClaimDuration = TimeSpan.FromSeconds(10),
            Retry = new OutboxRetryPolicy { MaxAttempts = 100, FirstDelay = TimeSpan.FromMilliseconds(200), MaxDelay = TimeSpan.FromSeconds(2) },
        };
        var dispatcher = new OutboxDispatcher(
            new PostgreSqlOutboxStore(server.DataSource, new OutboxTable(_table)), transport, TimeProvider.System, options.Retry);
        return new OutboxDispatcherService(
            dispatcher, Options.Create(options), TimeProvider.System, NullLogger<OutboxDispatcherService>.Instance);
    }

    private async Task<List<(Guid Id, string Key)>> SaveInterleavedMessages()
    {
        var saved = new List<(Guid, string)>();
        var createdAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        for (var i = 0; i < MessagesPerKey; i++)
        {
            foreach (var key in Keys)
            {
                var id = Guid.CreateVersion7();
                createdAt = createdAt.AddMilliseconds(1);
                await using var command = server.DataSource.CreateCommand(
                    $"INSERT INTO {_table} (id, type, payload, created_at, ordering_key) VALUES ($1, 'balance.changed', $2, $3, $4)");
                command.Parameters.AddWithValue(id);
                command.Parameters.AddWithValue($$"""{"seq":{{i}}}""");
                command.Parameters.AddWithValue(createdAt);
                command.Parameters.AddWithValue(key);
                await command.ExecuteNonQueryAsync(Token);
                saved.Add((id, key));
            }
        }

        return saved;
    }

    private async Task<List<(Guid Id, string Key)>> Drain()
    {
        await using var channel = await _connection.CreateChannelAsync(cancellationToken: Token);
        var received = new List<(Guid, string)>();
        while (await channel.BasicGetAsync(_queue, autoAck: true, Token) is { } message)
        {
            var key = Encoding.UTF8.GetString((byte[])message.BasicProperties.Headers![RabbitMqTransport.OrderingKeyHeader]!);
            received.Add((Guid.Parse(message.BasicProperties.MessageId!), key));
        }

        return received;
    }

    private async Task WaitUntil(Func<Task<bool>> condition, TimeSpan limit)
    {
        var deadline = DateTimeOffset.UtcNow + limit;
        while (!await condition())
        {
            Assert.True(DateTimeOffset.UtcNow < deadline, await Describe());
            await Task.Delay(TimeSpan.FromMilliseconds(200), Token);
        }
    }

    private async Task<string> Describe()
    {
        await using var command = server.DataSource.CreateCommand(
            $"SELECT ordering_key, attempts, dispatched_at IS NOT NULL, claimed_until, last_error FROM {_table} ORDER BY created_at LIMIT 6");
        await using var reader = await command.ExecuteReaderAsync(Token);
        var rows = new StringBuilder("timed out; first rows: ");
        while (await reader.ReadAsync(Token))
        {
            rows.Append(System.Globalization.CultureInfo.InvariantCulture, $"[{reader.GetString(0)} attempts={reader.GetInt32(1)} sent={reader.GetBoolean(2)} claimed={(reader.IsDBNull(3) ? "-" : reader.GetDateTime(3).ToString("O", System.Globalization.CultureInfo.InvariantCulture))} error={(reader.IsDBNull(4) ? "-" : reader.GetString(4))}] ");
        }

        return rows.ToString();
    }

    private async Task<long> Scalar(string sql)
    {
        await using var command = server.DataSource.CreateCommand(sql);
        return Convert.ToInt64(await command.ExecuteScalarAsync(Token), System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task Execute(string sql)
    {
        await using var command = server.DataSource.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(Token);
    }
}
