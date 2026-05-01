using Microsoft.Data.Sqlite;
using System.Text.Json;
using System.Runtime.InteropServices;

namespace Honker;

public sealed class Database : IDisposable
{
    private readonly object _gate = new();
    private readonly SqliteConnection _connection;
    private readonly Dictionary<(string Name, QueueOptions Options), Queue> _queues = new();
    private readonly Dictionary<string, Stream> _streams = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Outbox> _outboxes = new(StringComparer.Ordinal);
    private readonly Scheduler _scheduler;
    private readonly string _extensionPath;

    private Database(string path, OpenOptions options, SqliteConnection connection, string extensionPath)
    {
        Path = System.IO.Path.GetFullPath(path);
        Options = options;
        ConnectionString = options.BuildConnectionString(Path);
        _connection = connection;
        _scheduler = new Scheduler(this);
        _extensionPath = extensionPath;
    }

    public string Path { get; }
    public string ConnectionString { get; }
    public OpenOptions Options { get; }
    public SqliteConnection RawConnection => _connection;

    public Scheduler Scheduler => _scheduler;

    public static Database Open(string path, OpenOptions? options = null)
    {
        options ??= new OpenOptions();
        var extensionPath = ResolveExtensionPath(options);

        var connection = new SqliteConnection(options.BuildConnectionString(path));
        connection.Open();
        var resolvedExtensionPath = System.IO.Path.GetFullPath(extensionPath);
        InstallConnection(connection, resolvedExtensionPath, bootstrap: true);
        return new Database(path, options, connection, resolvedExtensionPath);
    }

    public Queue Queue(string name, QueueOptions? options = null)
    {
        var resolved = options ?? new QueueOptions();
        lock (_gate)
        {
            if (_queues.TryGetValue((name, resolved), out var existing))
            {
                return existing;
            }
        }

        var created = new Queue(this, name, resolved);
        lock (_gate)
        {
            if (_queues.TryGetValue((name, resolved), out var existing))
            {
                return existing;
            }

            _queues[(name, resolved)] = created;
            return created;
        }
    }

    public Stream Stream(string name)
    {
        lock (_gate)
        {
            if (_streams.TryGetValue(name, out var existing))
            {
                return existing;
            }
        }

        var created = new Stream(this, name);
        lock (_gate)
        {
            if (_streams.TryGetValue(name, out var existing))
            {
                return existing;
            }

            _streams[name] = created;
            return created;
        }
    }

    public Listener Listen(string channel)
    {
        return new Listener(this, channel);
    }

    public Outbox Outbox(string name, Func<JsonElement, CancellationToken, Task> delivery, OutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        return GetOrCreateOutbox(name, delivery, delivery, options ?? new OutboxOptions());
    }

    public Outbox Outbox(string name, Func<JsonElement, Task> delivery, OutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        return GetOrCreateOutbox(
            name,
            delivery,
            async (payload, _) => await delivery(payload),
            options ?? new OutboxOptions()
        );
    }

    public Outbox Outbox(string name, Action<JsonElement> delivery, OutboxOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(delivery);
        return GetOrCreateOutbox(
            name,
            delivery,
            (payload, _) =>
            {
                delivery(payload);
                return Task.CompletedTask;
            },
            options ?? new OutboxOptions()
        );
    }

    public HonkerTransaction BeginTransaction()
    {
        lock (_gate)
        {
            return new HonkerTransaction(this, _connection.BeginTransaction(deferred: false));
        }
    }

    public IReadOnlyList<IReadOnlyDictionary<string, object?>> Query(string sql, params object?[] args)
    {
        return QueryInternal(sql, null, args);
    }

    public void Execute(string sql, params object?[] args)
    {
        ExecuteNonQuery(sql, null, args);
    }

    public long Notify(string channel, object? payload)
    {
        using var tx = BeginTransaction();
        var id = tx.Notify(channel, payload);
        tx.Commit();
        return id;
    }

    public int PruneNotifications(int? olderThanSeconds = null, int? maxKeep = null)
    {
        var clauses = new List<string>();
        var args = new List<object?>();
        if (olderThanSeconds is not null)
        {
            clauses.Add("(unixepoch() - created_at) > @p0");
            args.Add(olderThanSeconds.Value);
        }

        if (maxKeep is not null)
        {
            var index = args.Count;
            clauses.Add($"id <= (SELECT COALESCE(MAX(id), 0) FROM _honker_notifications) - @p{index}");
            args.Add(maxKeep.Value);
        }

        if (clauses.Count == 0)
        {
            return 0;
        }

        var sql = $"DELETE FROM _honker_notifications WHERE {string.Join(" OR ", clauses)}";
        lock (_gate)
        {
            using var command = SqliteHelpers.CreateCommand(_connection, null, sql, args);
            return command.ExecuteNonQuery();
        }
    }

    public bool TryRateLimit(string name, int limit, int perSeconds)
    {
        if (limit <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), "limit must be positive");
        }

        if (perSeconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(perSeconds), "perSeconds must be positive");
        }

        return ExecuteScalarInt64(
            "SELECT honker_rate_limit_try(@p0, @p1, @p2)",
            null,
            name,
            limit,
            perSeconds
        ) == 1;
    }

    public int SweepRateLimits(int olderThanSeconds)
    {
        if (olderThanSeconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(olderThanSeconds), "olderThanSeconds must be positive");
        }

        return Convert.ToInt32(ExecuteScalar(
            "SELECT honker_rate_limit_sweep(@p0)",
            null,
            olderThanSeconds
        ) ?? 0);
    }

    public LockHandle? TryLock(string name, string owner, int ttlSeconds)
    {
        var acquired = ExecuteScalarInt64(
            "SELECT honker_lock_acquire(@p0, @p1, @p2)",
            null,
            name,
            owner,
            ttlSeconds
        );
        return acquired == 1 ? new LockHandle(this, name, owner) : null;
    }

    public LockHandle Lock(string name, int ttlSeconds, string? owner = null)
    {
        var resolvedOwner = string.IsNullOrWhiteSpace(owner) ? Guid.NewGuid().ToString("N") : owner;
        return TryLock(name, resolvedOwner, ttlSeconds) ?? throw new LockHeldException(name);
    }

    internal UpdatePoller CreatePoller()
    {
        return new UpdatePoller(ConnectionString, Options.UpdatePollInterval);
    }

    internal SqliteConnection CreateReadConnection()
    {
        var connection = new SqliteConnection(ConnectionString);
        connection.Open();
        InstallConnection(connection, _extensionPath, bootstrap: false);
        return connection;
    }

    internal long ExecuteScalarInt64(string sql, HonkerTransaction? transaction, params object?[] args)
    {
        return (long)ExecuteScalar(sql, transaction, args)!;
    }

    internal object? ExecuteScalar(string sql, HonkerTransaction? transaction, params object?[] args)
    {
        lock (_gate)
        {
            using var command = SqliteHelpers.CreateCommand(_connection, transaction?.Inner, sql, args);
            return command.ExecuteScalar();
        }
    }

    internal IReadOnlyList<IReadOnlyDictionary<string, object?>> QueryInternal(string sql, HonkerTransaction? transaction, params object?[] args)
    {
        lock (_gate)
        {
            using var command = SqliteHelpers.CreateCommand(_connection, transaction?.Inner, sql, args);
            using var reader = command.ExecuteReader();
            var rows = new List<IReadOnlyDictionary<string, object?>>();
            while (reader.Read())
            {
                var row = new Dictionary<string, object?>(reader.FieldCount, StringComparer.Ordinal);
                for (var i = 0; i < reader.FieldCount; i += 1)
                {
                    var value = reader.GetValue(i);
                    row[reader.GetName(i)] = value is DBNull ? null : value;
                }

                rows.Add(row);
            }

            return rows;
        }
    }

    internal void ExecuteNonQuery(string sql, HonkerTransaction? transaction, params object?[] args)
    {
        lock (_gate)
        {
            using var command = SqliteHelpers.CreateCommand(_connection, transaction?.Inner, sql, args);
            command.ExecuteNonQuery();
        }
    }

    internal void WithLock(Action action)
    {
        lock (_gate)
        {
            action();
        }
    }

    public void Dispose()
    {
        _connection.Dispose();
    }

    public bool FunctionExists(string name)
    {
        var found = Convert.ToInt64(ExecuteScalar(
            "SELECT COUNT(*) FROM pragma_function_list WHERE name = @p0",
            null,
            name
        ) ?? 0L);
        return found > 0;
    }

    private Outbox GetOrCreateOutbox(
        string name,
        Delegate originalDelivery,
        Func<JsonElement, CancellationToken, Task> normalizedDelivery,
        OutboxOptions options)
    {
        lock (_gate)
        {
            if (_outboxes.TryGetValue(name, out var existing))
            {
                if (!Delegate.Equals(existing.OriginalDelivery, originalDelivery) || existing.Options != options)
                {
                    throw new InvalidOperationException($"outbox '{name}' is already registered with a different delivery delegate");
                }

                return existing;
            }
        }

        var created = new Outbox(this, name, originalDelivery, normalizedDelivery, options);
        lock (_gate)
        {
            if (_outboxes.TryGetValue(name, out var existing))
            {
                if (!Delegate.Equals(existing.OriginalDelivery, originalDelivery) || existing.Options != options)
                {
                    throw new InvalidOperationException($"outbox '{name}' is already registered with a different delivery delegate");
                }

                return existing;
            }

            _outboxes[name] = created;
            return created;
        }
    }

    private static void InstallConnection(SqliteConnection connection, string extensionPath, bool bootstrap)
    {
        ApplyDefaultPragmas(connection);
        connection.EnableExtensions(true);
        connection.LoadExtension(extensionPath, "sqlite3_honkerext_init");
        connection.EnableExtensions(false);

        if (!bootstrap)
        {
            return;
        }

        using var bootstrapCommand = connection.CreateCommand();
        bootstrapCommand.CommandText = "SELECT honker_bootstrap()";
        bootstrapCommand.ExecuteScalar();
    }

    private static void ApplyDefaultPragmas(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText =
            """
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA busy_timeout = 5000;
            PRAGMA foreign_keys = ON;
            PRAGMA cache_size = -32000;
            PRAGMA temp_store = MEMORY;
            PRAGMA wal_autocheckpoint = 10000;
            """;
        command.ExecuteNonQuery();
    }

    private static string ResolveExtensionPath(OpenOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.ExtensionPath))
        {
            return options.ExtensionPath!;
        }

        var envPath = Environment.GetEnvironmentVariable("HONKER_EXTENSION_PATH");
        if (!string.IsNullOrWhiteSpace(envPath))
        {
            return envPath;
        }

        var fileName = ExtensionFileName();
        foreach (var baseDir in CandidateBaseDirectories())
        {
            var direct = System.IO.Path.Combine(baseDir, fileName);
            if (File.Exists(direct))
            {
                return direct;
            }

            var runtimeSpecific = System.IO.Path.Combine(baseDir, "runtimes", CurrentRid(), "native", fileName);
            if (File.Exists(runtimeSpecific))
            {
                return runtimeSpecific;
            }
        }

        throw new InvalidOperationException(
            "Could not locate honker native extension. Set OpenOptions.ExtensionPath or HONKER_EXTENSION_PATH, or ensure the NuGet package's native asset is present."
        );
    }

    private static IEnumerable<string> CandidateBaseDirectories()
    {
        yield return AppContext.BaseDirectory;

        var assemblyDir = System.IO.Path.GetDirectoryName(typeof(Database).Assembly.Location);
        if (!string.IsNullOrWhiteSpace(assemblyDir) && !string.Equals(assemblyDir, AppContext.BaseDirectory, StringComparison.Ordinal))
        {
            yield return assemblyDir;
        }
    }

    private static string CurrentRid()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return RuntimeInformation.ProcessArchitecture switch
            {
                Architecture.Arm64 => "win-arm64",
                _ => "win-x64",
            };
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return RuntimeInformation.ProcessArchitecture switch
            {
                Architecture.Arm64 => "osx-arm64",
                _ => "osx-x64",
            };
        }

        return RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.Arm64 => "linux-arm64",
            _ => "linux-x64",
        };
    }

    private static string ExtensionFileName()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return "honker_ext.dll";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return "libhonker_ext.dylib";
        }

        return "libhonker_ext.so";
    }
}
