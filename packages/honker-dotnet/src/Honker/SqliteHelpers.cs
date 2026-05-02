using Microsoft.Data.Sqlite;

namespace Honker;

internal static class SqliteHelpers
{
    public static SqliteCommand CreateCommand(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string sql,
        IReadOnlyList<object?> args)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Transaction = transaction;

        for (var i = 0; i < args.Count; i += 1)
        {
            command.Parameters.AddWithValue($"@p{i}", args[i] ?? DBNull.Value);
        }

        return command;
    }
}
