namespace OutboxKit.Tests;

public class MessageTypeRegistryTests
{
    private sealed record OrderPlaced(int OrderId);

    private sealed record OrderCancelled(int OrderId);

    [Fact]
    public void Maps_a_type_to_its_name_and_back()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);

        Assert.Equal("orders.placed", registry.GetName(typeof(OrderPlaced)));
        Assert.Equal(typeof(OrderPlaced), registry.GetClrType("orders.placed"));
    }

    [Fact]
    public void Names_are_case_sensitive()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);

        Assert.False(registry.TryGetClrType("Orders.Placed", out _));
    }

    [Fact]
    public void Unknown_type_throws_a_specific_exception()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);

        Assert.Throws<UnknownMessageTypeException>(() => registry.GetName(typeof(OrderCancelled)));
    }

    [Fact]
    public void Unknown_name_throws_from_get_and_returns_false_from_try_get()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);

        Assert.Throws<UnknownMessageTypeException>(() => registry.GetClrType("orders.shipped"));
        Assert.False(registry.TryGetClrType("orders.shipped", out var type));
        Assert.Null(type);
    }

    [Fact]
    public void Rejects_the_same_type_registered_twice()
    {
        Assert.Throws<ArgumentException>(() => new MessageTypeRegistry(
        [
            MessageTypeRegistration.For<OrderPlaced>("orders.placed"),
            MessageTypeRegistration.For<OrderPlaced>("orders.placed.v2"),
        ]));
    }

    [Fact]
    public void Rejects_one_name_for_two_types()
    {
        var error = Assert.Throws<ArgumentException>(() => new MessageTypeRegistry(
        [
            MessageTypeRegistration.For<OrderPlaced>("orders.event"),
            MessageTypeRegistration.For<OrderCancelled>("orders.event"),
        ]));

        Assert.Contains(nameof(OrderPlaced), error.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(OrderCancelled), error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Rejects_a_blank_name(string name)
    {
        Assert.Throws<ArgumentException>(() => new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>(name)]));
    }
}
