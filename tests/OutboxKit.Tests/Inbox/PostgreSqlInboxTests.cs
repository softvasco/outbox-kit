using System.Data.Common;
using OutboxKit.Inbox;
using OutboxKit.Tests.Stores;

namespace OutboxKit.Tests.Inbox;

public sealed class PostgreSqlInboxTests(PostgreSqlServer server) : InboxContract, IClassFixture<PostgreSqlServer>
{
    protected override DbDataSource DataSource => server.DataSource;

    protected override IInbox CreateInbox(InboxTable table, TimeProvider clock) => new PostgreSqlInbox(table, clock);

    protected override IInboxRetentionStore CreateRetentionStore(InboxTable table) =>
        new PostgreSqlInboxRetentionStore(server.DataSource, table);

    protected override string CreateTableSql(string table) => $"""
        CREATE TABLE {table} (
            message_id varchar(256) NOT NULL,
            consumer varchar(256) NOT NULL,
            handled_at timestamptz NOT NULL,
            PRIMARY KEY (message_id, consumer))
        """;
}
