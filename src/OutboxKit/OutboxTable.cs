namespace OutboxKit;

/// <summary>Where outbox rows go. The name is put straight into SQL, so only plain identifiers are accepted.</summary>
public sealed class OutboxTable(string name = OutboxTable.DefaultName)
{
    public const string DefaultName = "outbox_messages";

    public string Name { get; } = TableName.Validate(name, nameof(name));
}
