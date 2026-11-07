using System.Data.Common;
using Microsoft.Data.SqlClient;
using OutboxKit.Stores;
using Testcontainers.MsSql;

namespace OutboxKit.Tests.Stores;

public sealed class SqlServer : IAsyncLifetime
{
    private const string Database = "outbox";

    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();

    public DbDataSource DataSource { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();

        // most teams on SQL Server turn read committed snapshot on, and READPAST has to work there too
        await using (var master = new SqlConnection(_container.GetConnectionString()))
        {
            await master.OpenAsync();
            await using var create = new SqlCommand(
                $"CREATE DATABASE {Database}; ALTER DATABASE {Database} SET READ_COMMITTED_SNAPSHOT ON;", master);
            await create.ExecuteNonQueryAsync();
        }

        var connectionString = new SqlConnectionStringBuilder(_container.GetConnectionString()) { InitialCatalog = Database };
        DataSource = SqlClientFactory.Instance.CreateDataSource(connectionString.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }
}

public sealed class SqlServerOutboxStoreTests(SqlServer server) : OutboxStoreContract, IClassFixture<SqlServer>
{
    protected override DbDataSource DataSource => server.DataSource;

    protected override SqlOutboxStore CreateStore(OutboxTable table) => new SqlServerOutboxStore(server.DataSource, table);

    protected override string CreateTableSql(string table) => $"""
        CREATE TABLE {table} (
            id uniqueidentifier PRIMARY KEY,
            type nvarchar(256) NOT NULL,
            payload nvarchar(max) NOT NULL,
            created_at datetimeoffset NOT NULL,
            ordering_key nvarchar(256),
            claimed_until datetimeoffset,
            dispatched_at datetimeoffset)
        """;
}
