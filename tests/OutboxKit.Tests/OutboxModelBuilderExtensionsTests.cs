using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using OutboxKit.EntityFrameworkCore;

namespace OutboxKit.Tests;

public sealed class OutboxModelBuilderExtensionsTests
{
    [Fact]
    public void Messages_map_to_the_same_table_and_columns_as_the_ado_net_outbox()
    {
        var entity = Map(table: null);

        Assert.Equal(OutboxTable.DefaultName, entity.GetTableName());
        Assert.Null(entity.GetSchema());
        Assert.Equal(
            ["claimed_until", "created_at", "dispatched_at", "id", "ordering_key", "payload", "type"],
            entity.GetProperties().Select(p => p.GetColumnName()).Order());
    }

    [Fact]
    public void The_columns_only_the_stores_use_are_nullable_and_hidden_from_the_message()
    {
        var entity = Map(table: null);

        Assert.All(
            ["ClaimedUntil", "DispatchedAt"],
            name => Assert.True(entity.FindProperty(name) is { IsNullable: true } property && property.IsShadowProperty(), name));
    }

    [Fact]
    public void The_id_is_the_key_and_is_never_generated_by_the_database()
    {
        var id = Map(table: null).FindProperty(nameof(OutboxMessage.Id))!;

        Assert.True(id.IsPrimaryKey());
        Assert.Equal(ValueGenerated.Never, id.ValueGenerated);
    }

    [Fact]
    public void A_schema_prefix_becomes_the_table_schema()
    {
        var entity = Map(new OutboxTable("messaging.outbox"));

        Assert.Equal("messaging", entity.GetSchema());
        Assert.Equal("outbox", entity.GetTableName());
    }

    [Fact]
    public void Text_columns_keep_the_limits_the_message_enforces()
    {
        var entity = Map(table: null);

        Assert.Equal(OutboxMessage.MaxTypeLength, entity.FindProperty(nameof(OutboxMessage.Type))!.GetMaxLength());
        Assert.Equal(OutboxMessage.MaxOrderingKeyLength, entity.FindProperty(nameof(OutboxMessage.OrderingKey))!.GetMaxLength());
        Assert.True(entity.FindProperty(nameof(OutboxMessage.OrderingKey))!.IsNullable);
        Assert.False(entity.FindProperty(nameof(OutboxMessage.Payload))!.IsNullable);
    }

    private static IMutableEntityType Map(OutboxTable? table)
    {
        var builder = new ModelBuilder();
        builder.AddOutboxMessages(table);
        return builder.Model.FindEntityType(typeof(OutboxMessage))!;
    }
}
