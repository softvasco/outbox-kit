using System.Data.Common;
using Microsoft.Data.SqlClient;
using OutboxKit.Stores;
using Testcontainers.MsSql;

namespace OutboxKit.Benchmarks;

public class SqlServerDispatchBenchmarks : DispatchBenchmarks
{
    private const string Database = "outbox";

    private MsSqlContainer _container = null!;
    private DbDataSource _dataSource = null!;

    protected override async Task<DbDataSource> StartAsync()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
        await _container.StartAsync();

        // same setting as the store tests, since most SQL Server shops run with it on
        await using (var master = new SqlConnection(_container.GetConnectionString()))
        {
            await master.OpenAsync();
            await using var create = new SqlCommand(
                $"CREATE DATABASE {Database}; ALTER DATABASE {Database} SET READ_COMMITTED_SNAPSHOT ON;", master);
            await create.ExecuteNonQueryAsync();
        }

        var connectionString = new SqlConnectionStringBuilder(_container.GetConnectionString()) { InitialCatalog = Database };
        _dataSource = SqlClientFactory.Instance.CreateDataSource(connectionString.ConnectionString);
        return _dataSource;
    }

    protected override async Task StopAsync()
    {
        await _dataSource.DisposeAsync();
        await _container.DisposeAsync();
    }

    protected override IOutboxStore CreateStore(DbDataSource dataSource, OutboxTable table) =>
        new SqlServerOutboxStore(dataSource, table);

    protected override string CreateTableSql(string table) => $"""
        CREATE TABLE {table} (
            id uniqueidentifier PRIMARY KEY,
            type nvarchar(256) NOT NULL,
            payload nvarchar(max) NOT NULL,
            created_at datetimeoffset NOT NULL,
            ordering_key nvarchar(256),
            claimed_until datetimeoffset,
            dispatched_at datetimeoffset,
            attempts int NOT NULL DEFAULT 0,
            dead_lettered_at datetimeoffset,
            last_error nvarchar(max))
        """;
}
