using System.Data.Common;
using Npgsql;
using OutboxKit.Stores;
using Testcontainers.PostgreSql;

namespace OutboxKit.Benchmarks;

public class PostgreSqlDispatchBenchmarks : DispatchBenchmarks
{
    private PostgreSqlContainer _container = null!;
    private NpgsqlDataSource _dataSource = null!;

    protected override async Task<DbDataSource> StartAsync()
    {
        _container = new PostgreSqlBuilder("postgres:18-alpine").Build();
        await _container.StartAsync();
        _dataSource = NpgsqlDataSource.Create(_container.GetConnectionString());
        return _dataSource;
    }

    protected override async Task StopAsync()
    {
        await _dataSource.DisposeAsync();
        await _container.DisposeAsync();
    }

    protected override IOutboxStore CreateStore(DbDataSource dataSource, OutboxTable table) =>
        new PostgreSqlOutboxStore(dataSource, table);

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
