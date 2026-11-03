namespace OutboxKit.Tests;

public class OutboxTableTests
{
    [Theory]
    [InlineData("outbox_messages")]
    [InlineData("messaging.outbox")]
    [InlineData("_Outbox2")]
    public void Accepts_plain_names_with_an_optional_schema(string name) =>
        Assert.Equal(name, new OutboxTable(name).Name);

    [Theory]
    [InlineData("outbox; DROP TABLE orders")]
    [InlineData("[outbox]")]
    [InlineData("\"outbox\"")]
    [InlineData("a.b.c")]
    [InlineData("1outbox")]
    public void Refuses_anything_that_is_not_a_plain_identifier(string name) =>
        Assert.Throws<ArgumentException>(() => new OutboxTable(name));

    [Fact]
    public void Defaults_to_outbox_messages() => Assert.Equal("outbox_messages", new OutboxTable().Name);
}
