using Microsoft.Data.Sqlite;

namespace Honker;

internal sealed class UpdatePoller : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly TimeSpan _pollInterval;
    private long _lastSeenVersion;

    public UpdatePoller(string connectionString, TimeSpan pollInterval)
    {
        _pollInterval = pollInterval <= TimeSpan.Zero ? TimeSpan.FromMilliseconds(1) : pollInterval;
        _connection = new SqliteConnection(connectionString);
        _connection.Open();
        _lastSeenVersion = ReadDataVersion();
    }

    public async Task WaitForChangeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (timeout <= TimeSpan.Zero)
        {
            return;
        }

        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var current = ReadDataVersion();
            if (current != _lastSeenVersion)
            {
                _lastSeenVersion = current;
                return;
            }

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                return;
            }

            await Task.Delay(remaining < _pollInterval ? remaining : _pollInterval, cancellationToken);
        }
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    private long ReadDataVersion()
    {
        using var command = _connection.CreateCommand();
        command.CommandText = "PRAGMA data_version";
        return Convert.ToInt64(command.ExecuteScalar());
    }
}
