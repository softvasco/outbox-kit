using Npgsql;
using OutboxKit;
using OutboxKit.CrashHost;
using OutboxKit.Stores;

// usage: OutboxKit.CrashHost <after-commit|after-send> <connection string> <outbox table> <orders table> <broker table>
if (args.Length != 5)
{
    Console.Error.WriteLine("usage: OutboxKit.CrashHost <after-commit|after-send> <connection string> <outbox table> <orders table> <broker table>");
    return 2;
}

var (mode, connectionString, outboxTable, ordersTable, brokerTable) = (args[0], args[1], new OutboxTable(args[2]), args[3], args[4]);
await using var dataSource = NpgsqlDataSource.Create(connectionString);

switch (mode)
{
    case "after-commit":
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);
        var outbox = new AdoNetOutbox(new OutboxMessageFactory(registry, new JsonMessageSerializer(), TimeProvider.System), outboxTable);

        await using (var connection = await dataSource.OpenConnectionAsync())
        await using (var transaction = await connection.BeginTransactionAsync())
        {
            await using var insert = new NpgsqlCommand($"INSERT INTO {ordersTable} (id) VALUES (42)", connection, transaction);
            await insert.ExecuteNonQueryAsync();
            await outbox.EnqueueAsync(new OrderPlaced(42), transaction);
            await transaction.CommitAsync();
        }

        Environment.FailFast("crash host: killed after commit, before anything was sent");
        break;

    case "after-send":
        var transport = new KillAfterSend(new BrokerTableTransport(dataSource, brokerTable));
        var dispatcher = new OutboxDispatcher(new PostgreSqlOutboxStore(dataSource, outboxTable), transport, TimeProvider.System);
        await dispatcher.DispatchBatchAsync(10, TimeSpan.FromSeconds(30), CancellationToken.None);
        break;

    default:
        Console.Error.WriteLine($"unknown mode {mode}");
        return 2;
}

// reaching this line means nothing was claimed
Console.Error.WriteLine("crash host: nothing to do");
return 3;
