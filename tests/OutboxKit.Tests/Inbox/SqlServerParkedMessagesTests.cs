using System.Data.Common;
using OutboxKit.Inbox;
using OutboxKit.Tests.Stores;

namespace OutboxKit.Tests.Inbox;

public sealed class SqlServerParkedMessagesTests(SqlServer server) : ParkedMessagesContract, IClassFixture<SqlServer>
{
    protected override DbDataSource DataSource => server.DataSource;

    protected override IParkedMessages CreateStore(ParkedMessageTable table) => new SqlServerParkedMessages(server.DataSource, table);

    protected override string CreateTableSql(string table) => $"""
        CREATE TABLE {table} (
            message_id nvarchar(256) NOT NULL,
            consumer nvarchar(256) NOT NULL,
            type nvarchar(256) NOT NULL,
            payload nvarchar(max) NOT NULL,
            error nvarchar(max) NOT NULL,
            parked_at datetimeoffset NOT NULL,
            PRIMARY KEY (message_id, consumer))
        """;
}
