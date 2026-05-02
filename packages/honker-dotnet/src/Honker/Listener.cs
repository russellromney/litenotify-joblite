using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace Honker;

public sealed class Listener : IAsyncEnumerable<Notification>, IAsyncEnumerator<Notification>
{
    private readonly Database _database;
    private readonly string _channel;
    private readonly UpdatePoller _poller;
    private readonly SqliteConnection _readConnection;
    private readonly Queue<Notification> _buffer = new();
    private CancellationToken _cancellationToken;
    private bool _enumeratorIssued;
    private long _lastSeen;

    internal Listener(Database database, string channel)
    {
        _database = database;
        _channel = channel;
        _poller = database.CreatePoller();
        _readConnection = database.CreateReadConnection();
        using var command = _readConnection.CreateCommand();
        command.CommandText = "SELECT COALESCE(MAX(id), 0) FROM _honker_notifications WHERE channel=@p0";
        command.Parameters.AddWithValue("@p0", channel);
        _lastSeen = Convert.ToInt64(command.ExecuteScalar() ?? 0L);
    }

    public Notification Current { get; private set; } = null!;

    public IAsyncEnumerator<Notification> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        if (_enumeratorIssued)
        {
            return new Listener(_database, _channel).GetAsyncEnumerator(cancellationToken);
        }

        _enumeratorIssued = true;
        _cancellationToken = cancellationToken;
        return this;
    }

    public async ValueTask<bool> MoveNextAsync()
    {
        while (true)
        {
            _cancellationToken.ThrowIfCancellationRequested();

            if (_buffer.Count > 0)
            {
                Current = _buffer.Dequeue();
                return true;
            }

            using var command = _readConnection.CreateCommand();
            command.CommandText =
                """
                SELECT id, channel, payload, created_at
                FROM _honker_notifications
                WHERE channel=@p0 AND id > @p1
                ORDER BY id
                """;
            command.Parameters.AddWithValue("@p0", _channel);
            command.Parameters.AddWithValue("@p1", _lastSeen);
            using var reader = command.ExecuteReader();
            var gotRows = false;
            while (reader.Read())
            {
                gotRows = true;
                _lastSeen = reader.GetInt64(0);
                _buffer.Enqueue(new Notification(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetInt64(3)
                ));
            }

            if (gotRows)
            {
                continue;
            }

            await _poller.WaitForChangeAsync(TimeSpan.FromSeconds(15), _cancellationToken);
        }
    }

    public ValueTask DisposeAsync()
    {
        _poller.Dispose();
        _readConnection.Dispose();
        return ValueTask.CompletedTask;
    }
}
