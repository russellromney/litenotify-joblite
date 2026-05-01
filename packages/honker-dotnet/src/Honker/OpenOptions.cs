using Microsoft.Data.Sqlite;

namespace Honker;

public sealed class OpenOptions
{
    public string? ExtensionPath { get; init; }
    public TimeSpan UpdatePollInterval { get; init; } = TimeSpan.FromMilliseconds(5);

    internal string BuildConnectionString(string path)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Default,
            Pooling = false,
        };

        return builder.ToString();
    }
}
