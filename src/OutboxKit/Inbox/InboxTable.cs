namespace OutboxKit.Inbox;

/// <summary>Where handled message ids go. The name is put straight into SQL, so only plain identifiers are accepted.</summary>
public sealed class InboxTable(string name = InboxTable.DefaultName)
{
    public const string DefaultName = "inbox_messages";

    public string Name { get; } = TableName.Validate(name, nameof(name));
}
