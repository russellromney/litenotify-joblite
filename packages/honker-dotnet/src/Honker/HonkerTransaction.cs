using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace Honker;

public sealed class HonkerTransaction : IDisposable
{
    private readonly Database _database;
    private bool _completed;

    internal HonkerTransaction(Database database, SqliteTransaction inner)
    {
        _database = database;
        Inner = inner;
    }

    internal SqliteTransaction Inner { get; }
    internal SqliteConnection Connection => Inner.Connection!;

    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Query(string sql, params object?[] args)
    {
        return _database.QueryInternal(sql, this, args);
    }

    public void Execute(string sql, params object?[] args)
    {
        _database.ExecuteNonQuery(sql, this, args);
    }

    public long Notify(string channel, object? payload)
    {
        return Convert.ToInt64(_database.ExecuteScalar(
            "SELECT notify(@p0, @p1)",
            this,
            channel,
            JsonSerializer.Serialize(payload)
        ) ?? 0L);
    }

    public void Commit()
    {
        _database.WithLock(() =>
        {
            Inner.Commit();
            _completed = true;
        });
    }

    public void Rollback()
    {
        _database.WithLock(() =>
        {
            Inner.Rollback();
            _completed = true;
        });
    }

    public void Dispose()
    {
        _database.WithLock(() =>
        {
            if (!_completed)
            {
                Inner.Rollback();
            }

            Inner.Dispose();
        });
    }
}
