using Microsoft.Extensions.Time.Testing;

namespace OutboxKit.Tests;

public class OutboxMessageFactoryTests
{
    private interface IOrderEvent;

    private sealed record OrderPlaced(int OrderId) : IOrderEvent;

    private sealed record OrderShipped(int OrderId);

    private static readonly DateTimeOffset Now = new(2026, 11, 2, 20, 45, 0, TimeSpan.Zero);

    private readonly FakeTimeProvider _time = new(Now);
    private readonly OutboxMessageFactory _factory;

    public OutboxMessageFactoryTests()
    {
        var registry = new MessageTypeRegistry([MessageTypeRegistration.For<OrderPlaced>("orders.placed")]);
        _factory = new OutboxMessageFactory(registry, new JsonMessageSerializer(), _time);
    }

    [Fact]
    public void Stores_the_registered_name_and_the_serialized_payload()
    {
        var message = _factory.Create(new OrderPlaced(42), "order-42");

        Assert.Equal("orders.placed", message.Type);
        Assert.Equal("""{"orderId":42}""", message.Payload);
        Assert.Equal("order-42", message.OrderingKey);
    }

    [Fact]
    public void Takes_the_time_from_the_time_provider()
    {
        var message = _factory.Create(new OrderPlaced(42));

        Assert.Equal(Now, message.CreatedAt);
    }

    [Fact]
    public void Ids_are_version_7_and_sort_by_creation_time()
    {
        var first = _factory.Create(new OrderPlaced(1));
        _time.Advance(TimeSpan.FromMilliseconds(5));
        var second = _factory.Create(new OrderPlaced(2));

        Assert.Equal(7, first.Id.Version);
        Assert.True(first.Id.CompareTo(second.Id) < 0);
    }

    [Fact]
    public void Looks_up_the_runtime_type_when_passed_an_interface()
    {
        IOrderEvent orderEvent = new OrderPlaced(7);

        var message = _factory.Create(orderEvent);

        Assert.Equal("orders.placed", message.Type);
        Assert.Equal("""{"orderId":7}""", message.Payload);
    }

    [Fact]
    public void Unregistered_messages_are_rejected()
    {
        Assert.Throws<UnknownMessageTypeException>(() => _factory.Create(new OrderShipped(42)));
    }
}
