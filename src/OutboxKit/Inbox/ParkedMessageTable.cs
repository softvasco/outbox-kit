namespace OutboxKit.Inbox;

/// <summary>Where parked messages go. The name goes into SQL, so plain identifiers only.</summary>
public sealed class ParkedMessageTable(string name = ParkedMessageTable.DefaultName)
{
    public const string DefaultName = "parked_messages";

    public string Name { get; } = TableName.Validate(name, nameof(name));
}
