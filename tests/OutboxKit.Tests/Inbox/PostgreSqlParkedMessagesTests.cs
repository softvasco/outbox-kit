using System.Data.Common;
using OutboxKit.Inbox;
using OutboxKit.Tests.Stores;

namespace OutboxKit.Tests.Inbox;

public sealed class PostgreSqlParkedMessagesTests(PostgreSqlServer server) : ParkedMessagesContract, IClassFixture<PostgreSqlServer>
{
    protected override DbDataSource DataSource => server.DataSource;

    protected override IParkedMessages CreateStore(ParkedMessageTable table) => new PostgreSqlParkedMessages(server.DataSource, table);

    protected override string CreateTableSql(string table) => $"""
        CREATE TABLE {table} (
            message_id varchar(256) NOT NULL,
            consumer varchar(256) NOT NULL,
            type varchar(256) NOT NULL,
            payload text NOT NULL,
            error text NOT NULL,
            parked_at timestamptz NOT NULL,
            PRIMARY KEY (message_id, consumer))
        """;
}
