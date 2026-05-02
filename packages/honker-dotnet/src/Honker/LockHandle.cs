namespace Honker;

public sealed class LockHandle : IDisposable
{
    private readonly Database _database;
    private bool _disposed;

    internal LockHandle(Database database, string name, string owner)
    {
        _database = database;
        Name = name;
        Owner = owner;
    }

    public string Name { get; }
    public string Owner { get; }
    public bool Acquired => !_disposed;

    public bool Heartbeat(int ttlSeconds)
    {
        return _database.ExecuteScalarInt64(
            "SELECT honker_lock_acquire(@p0, @p1, @p2)",
            null,
            Name,
            Owner,
            ttlSeconds
        ) == 1;
    }

    public bool Release()
    {
        if (_disposed)
        {
            return false;
        }

        var released = _database.ExecuteScalarInt64(
            "SELECT honker_lock_release(@p0, @p1)",
            null,
            Name,
            Owner
        ) == 1;
        _disposed = true;
        return released;
    }

    public void Dispose()
    {
        Release();
    }
}
