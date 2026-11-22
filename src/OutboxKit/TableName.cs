using System.Text.RegularExpressions;

namespace OutboxKit;

// table names go straight into SQL, so only plain identifiers get through
internal static partial class TableName
{
    public static string Validate(string name, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name, parameterName);
        if (!Identifier().IsMatch(name))
        {
            throw new ArgumentException(
                $"'{name}' is not a plain table name. Use letters, digits and underscores, with an optional schema prefix.", parameterName);
        }

        return name;
    }

    [GeneratedRegex(@"^([A-Za-z_][A-Za-z0-9_]*\.)?[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex Identifier();
}
