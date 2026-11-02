namespace OutboxKit.Tests;

public class OutboxMessageTests
{
    private static readonly DateTimeOffset CreatedAt = new(2026, 11, 2, 20, 30, 0, TimeSpan.Zero);

    [Fact]
    public void Keeps_the_values_it_was_created_with()
    {
        var id = Guid.CreateVersion7(CreatedAt);

        var message = new OutboxMessage(id, "orders.placed", """{"orderId":42}""", CreatedAt, "order-42");

        Assert.Equal(id, message.Id);
        Assert.Equal("orders.placed", message.Type);
        Assert.Equal("""{"orderId":42}""", message.Payload);
        Assert.Equal(CreatedAt, message.CreatedAt);
        Assert.Equal("order-42", message.OrderingKey);
    }

    [Fact]
    public void Ordering_key_is_optional()
    {
        var message = new OutboxMessage(Guid.CreateVersion7(), "orders.placed", "{}", CreatedAt);

        Assert.Null(message.OrderingKey);
    }

    [Fact]
    public void Rejects_an_empty_id()
    {
        Assert.Throws<ArgumentException>(() => new OutboxMessage(Guid.Empty, "orders.placed", "{}", CreatedAt));
    }

    [Theory]
    [InlineData("")]
    [InlineData("  ")]
    public void Rejects_a_blank_type(string type)
    {
        Assert.Throws<ArgumentException>(() => new OutboxMessage(Guid.CreateVersion7(), type, "{}", CreatedAt));
    }

    [Fact]
    public void Rejects_a_type_longer_than_the_column()
    {
        var type = new string('a', OutboxMessage.MaxTypeLength + 1);

        Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxMessage(Guid.CreateVersion7(), type, "{}", CreatedAt));
    }

    [Fact]
    public void Accepts_an_empty_payload()
    {
        var message = new OutboxMessage(Guid.CreateVersion7(), "ping", string.Empty, CreatedAt);

        Assert.Empty(message.Payload);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void Rejects_a_blank_ordering_key(string orderingKey)
    {
        Assert.Throws<ArgumentException>(() => new OutboxMessage(Guid.CreateVersion7(), "orders.placed", "{}", CreatedAt, orderingKey));
    }

    [Fact]
    public void Rejects_an_ordering_key_longer_than_the_column()
    {
        var key = new string('k', OutboxMessage.MaxOrderingKeyLength + 1);

        Assert.Throws<ArgumentOutOfRangeException>(() => new OutboxMessage(Guid.CreateVersion7(), "orders.placed", "{}", CreatedAt, key));
    }
}
