using OutboxKit.Inbox;

namespace OutboxKit.Tests.Inbox;

public class InboxTableTests
{
    [Fact]
    public void Defaults_to_inbox_messages() => Assert.Equal("inbox_messages", new InboxTable().Name);

    [Theory]
    [InlineData("inbox; DROP TABLE orders")]
    [InlineData("a.b.c")]
    public void Refuses_anything_that_is_not_a_plain_identifier(string name) =>
        Assert.Throws<ArgumentException>(() => new InboxTable(name));
}
