using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;

namespace Honker;

public sealed class Stream
{
    private readonly Database _database;

    internal Stream(Database database, string name)
    {
        _database = database;
        Name = name;
    }

    public string Name { get; }

    public long Publish(object? payload, string? key = null, HonkerTransaction? transaction = null)
    {
        var payloadJson = JsonSerializer.Serialize(payload);
        var raw = _database.ExecuteScalar(
            "SELECT honker_stream_publish(@p0, @p1, @p2)",
            transaction,
            Name,
            key,
            payloadJson
        );
        return Convert.ToInt64(raw ?? 0L);
    }

    public IReadOnlyList<Event> ReadSince(long offset, int limit = 1000)
    {
        using var connection = _database.CreateReadConnection();
        return ReadSince(connection, offset, limit);
    }

    public void SaveOffset(string consumer, long offset, HonkerTransaction? transaction = null)
    {
        _database.ExecuteScalar(
            "SELECT honker_stream_save_offset(@p0, @p1, @p2)",
            transaction,
            consumer,
            Name,
            offset
        );
    }

    public long GetOffset(string consumer)
    {
        return Convert.ToInt64(_database.ExecuteScalar(
            "SELECT honker_stream_get_offset(@p0, @p1)",
            null,
            consumer,
            Name
        ) ?? 0L);
    }

    public IAsyncEnumerable<Event> Subscribe(
        string? consumer = null,
        long? fromOffset = null,
        int saveEveryN = 1000,
        double saveEverySeconds = 1.0,
        CancellationToken cancellationToken = default)
    {
        var offset = fromOffset ?? (consumer is null ? 0 : GetOffset(consumer));
        return new Subscription(this, offset, consumer, Math.Max(0, saveEveryN), Math.Max(0.0, saveEverySeconds), cancellationToken);
    }

    private static Event ToEvent(StreamRow row)
    {
        return new Event(row.Offset, row.Topic, row.Key, row.Payload, row.CreatedAt);
    }

    private sealed class Subscription : IAsyncEnumerable<Event>, IAsyncEnumerator<Event>
    {
        private readonly Stream _stream;
        private readonly CancellationToken _cancellationToken;
        private readonly UpdatePoller _poller;
        private readonly SqliteConnection _readConnection;
        private readonly string? _consumer;
        private readonly int _saveEveryN;
        private readonly double _saveEverySeconds;
        private readonly Queue<Event> _buffer = new();
        private long _offset;
        private long _pendingSaveOffset;
        private int _eventsSinceSave;
        private long _lastSaveAtTicks;

        public Subscription(
            Stream stream,
            long fromOffset,
            string? consumer,
            int saveEveryN,
            double saveEverySeconds,
            CancellationToken cancellationToken)
        {
            _stream = stream;
            _offset = fromOffset;
            _consumer = consumer;
            _saveEveryN = saveEveryN;
            _saveEverySeconds = saveEverySeconds;
            _cancellationToken = cancellationToken;
            _poller = stream._database.CreatePoller();
            _readConnection = stream._database.CreateReadConnection();
            _lastSaveAtTicks = Stopwatch.GetTimestamp();
        }

        public Event Current { get; private set; } = null!;

        public IAsyncEnumerator<Event> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (cancellationToken != default && cancellationToken != _cancellationToken)
            {
                return new Subscription(_stream, _offset, _consumer, _saveEveryN, _saveEverySeconds, cancellationToken);
            }

            return this;
        }

        public async ValueTask<bool> MoveNextAsync()
        {
            while (true)
            {
                _cancellationToken.ThrowIfCancellationRequested();

                if (_buffer.Count > 0)
                {
                    MaybeSaveOffset();
                    Current = _buffer.Dequeue();
                    _offset = Current.Offset;
                    _pendingSaveOffset = _offset;
                    _eventsSinceSave += 1;
                    return true;
                }

                var rows = _stream.ReadSince(_readConnection, _offset);
                if (rows.Count > 0)
                {
                    foreach (var row in rows)
                    {
                        _buffer.Enqueue(row);
                    }

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

        private void MaybeSaveOffset()
        {
            if (_consumer is null || _pendingSaveOffset <= 0)
            {
                return;
            }

            var countHit = _saveEveryN > 0 && _eventsSinceSave >= _saveEveryN;
            var elapsed = Stopwatch.GetElapsedTime(_lastSaveAtTicks).TotalSeconds;
            var timeHit = _saveEverySeconds > 0 && elapsed >= _saveEverySeconds;
            if (!countHit && !timeHit)
            {
                return;
            }

            _stream.SaveOffset(_consumer, _pendingSaveOffset);
            _eventsSinceSave = 0;
            _lastSaveAtTicks = Stopwatch.GetTimestamp();
        }
    }

    private IReadOnlyList<Event> ReadSince(SqliteConnection connection, long offset, int limit = 1000)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT honker_stream_read_since(@p0, @p1, @p2)";
        command.Parameters.AddWithValue("@p0", Name);
        command.Parameters.AddWithValue("@p1", offset);
        command.Parameters.AddWithValue("@p2", limit);
        var rowsJson = Convert.ToString(command.ExecuteScalar()) ?? "[]";

        var rows = JsonSerializer.Deserialize<List<StreamRow>>(rowsJson) ?? [];
        return rows.Select(ToEvent).ToList();
    }

    private sealed class StreamRow
    {
        [JsonPropertyName("offset")]
        public long Offset { get; init; }

        [JsonPropertyName("topic")]
        public string Topic { get; init; } = "";

        [JsonPropertyName("key")]
        public string? Key { get; init; }

        [JsonPropertyName("payload")]
        public string Payload { get; init; } = "null";

        [JsonPropertyName("created_at")]
        public long CreatedAt { get; init; }
    }
}
