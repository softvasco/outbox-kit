using System.Text.RegularExpressions;

namespace OutboxKit;

/// <summary>Where outbox rows go. The name is put straight into SQL, so only plain identifiers are accepted.</summary>
public sealed partial class OutboxTable
{
    public const string DefaultName = "outbox_messages";

    public OutboxTable(string name = DefaultName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (!Identifier().IsMatch(name))
        {
            throw new ArgumentException(
                $"'{name}' is not a plain table name. Use letters, digits and underscores, with an optional schema prefix.", nameof(name));
        }

        Name = name;
    }

    public string Name { get; }

    [GeneratedRegex(@"^([A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex Identifier();
}
