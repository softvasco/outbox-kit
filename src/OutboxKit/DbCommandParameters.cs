using System.Data.Common;

namespace OutboxKit;

internal static class DbCommandParameters
{
    public static void Add(this DbCommand command, string name, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
