using System.Data.Common;
using Npgsql;
using OutboxKit.Stores;
using Testcontainers.PostgreSql;

namespace OutboxKit.Tests.Stores;

public sealed class PostgreSqlServer : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:18-alpine").Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        DataSource = NpgsqlDataSource.Create(_container.GetConnectionString());
    }

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public sealed class PostgreSqlOutboxStoreTests(PostgreSqlServer server) : OutboxStoreContract, IClassFixture<PostgreSqlServer>
{
    protected override DbDataSource DataSource => server.DataSource;

    protected override SqlOutboxStore CreateStore(OutboxTable table) => new PostgreSqlOutboxStore(server.DataSource, table);

    protected override string CreateTableSql(string table) => $"""
        CREATE TABLE {table} (
            id uuid PRIMARY KEY,
            type varchar(256) NOT NULL,
            payload text NOT NULL,
            created_at timestamptz NOT NULL,
            ordering_key varchar(256),
            claimed_until timestamptz,
            dispatched_at timestamptz,
            attempts int NOT NULL DEFAULT 0,
            dead_lettered_at timestamptz,
            last_error text)
        """;
}
