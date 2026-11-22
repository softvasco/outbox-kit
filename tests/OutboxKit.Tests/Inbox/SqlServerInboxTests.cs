using System.Data.Common;
using OutboxKit.Inbox;
using OutboxKit.Tests.Stores;

namespace OutboxKit.Tests.Inbox;

public sealed class SqlServerInboxTests(SqlServer server) : InboxContract, IClassFixture<SqlServer>
{
    protected override DbDataSource DataSource => server.DataSource;

    protected override IInbox CreateInbox(InboxTable table, TimeProvider clock) => new SqlServerInbox(table, clock);

    protected override IInboxRetentionStore CreateRetentionStore(InboxTable table) =>
        new SqlServerInboxRetentionStore(server.DataSource, table);

    protected override string CreateTableSql(string table) => $"""
        CREATE TABLE {table} (
            message_id nvarchar(256) NOT NULL,
            consumer nvarchar(256) NOT NULL,
            handled_at datetimeoffset NOT NULL,
            PRIMARY KEY (message_id, consumer))
        """;
}
