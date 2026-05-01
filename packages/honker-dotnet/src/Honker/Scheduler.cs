using System.Text.Json;
using System.Text.Json.Serialization;

namespace Honker;

public sealed class Scheduler
{
    private readonly Database _database;
    private readonly HashSet<string> _registered = [];

    internal Scheduler(Database database)
    {
        _database = database;
    }

    public void Add(ScheduledTask task, HonkerTransaction? transaction = null)
    {
        var expr = task.Schedule ?? task.Cron;
        if (string.IsNullOrWhiteSpace(expr))
        {
            throw new ArgumentException("ScheduledTask requires Schedule or Cron", nameof(task));
        }

        _database.ExecuteScalar(
            "SELECT honker_scheduler_register(@p0, @p1, @p2, @p3, @p4, @p5)",
            transaction,
            task.Name,
            task.Queue,
            expr,
            JsonSerializer.Serialize(task.Payload),
            task.Priority,
            task.ExpiresSeconds
        );
        _registered.Add(task.Name);
    }

    public int Remove(string name)
    {
        var removed = Convert.ToInt32(_database.ExecuteScalar(
            "SELECT honker_scheduler_unregister(@p0)",
            null,
            name
        ) ?? 0);
        _registered.Remove(name);
        return removed;
    }

    public long Soonest()
    {
        return Convert.ToInt64(_database.ExecuteScalar(
            "SELECT honker_scheduler_soonest()",
            null
        ) ?? 0L);
    }

    public IReadOnlyList<ScheduledFire> Tick(long? nowUnix = null)
    {
        var rowsJson = Convert.ToString(_database.ExecuteScalar(
            "SELECT honker_scheduler_tick(@p0)",
            null,
            nowUnix ?? DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        )) ?? "[]";

        var rows = JsonSerializer.Deserialize<List<ScheduledFireRow>>(rowsJson) ?? [];
        return rows.Select(row => new ScheduledFire(row.Name, row.Queue, row.FireAt, row.JobId)).ToList();
    }

    public async Task RunAsync(string owner, CancellationToken cancellationToken = default)
    {
        using var poller = _database.CreatePoller();
        const int lockTtlSeconds = 60;
        var heartbeatInterval = TimeSpan.FromSeconds(30);

        if (_registered.Count == 0)
        {
            var rows = _database.Query("SELECT COUNT(*) AS n FROM _honker_scheduler_tasks");
            if (rows.Count == 0 || Convert.ToInt64(rows[0]["n"] ?? 0L) == 0)
            {
                throw new InvalidOperationException(
                    "Scheduler.RunAsync() called with no registered tasks. Call Add(...) first or pre-populate _honker_scheduler_tasks.");
            }
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            using var lockHandle = _database.TryLock("honker-scheduler", owner, lockTtlSeconds);
            if (lockHandle is null)
            {
                await poller.WaitForChangeAsync(TimeSpan.FromSeconds(5), cancellationToken);
                continue;
            }

            await LeaderLoopAsync(lockHandle, heartbeatInterval, cancellationToken, poller);
        }
    }

    private async Task LeaderLoopAsync(
        LockHandle lockHandle,
        TimeSpan heartbeatInterval,
        CancellationToken cancellationToken,
        UpdatePoller poller)
    {
        var lastHeartbeat = DateTime.UtcNow;
        while (!cancellationToken.IsCancellationRequested)
        {
            long soonest;
            using (var tx = _database.BeginTransaction())
            {
                tx.Query("SELECT honker_scheduler_tick(@p0)", DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                var rows = tx.Query("SELECT honker_scheduler_soonest() AS t");
                soonest = Convert.ToInt64(rows[0]["t"] ?? 0L);
                tx.Commit();
            }

            if (DateTime.UtcNow - lastHeartbeat >= heartbeatInterval)
            {
                if (!lockHandle.Heartbeat(60))
                {
                    return;
                }

                lastHeartbeat = DateTime.UtcNow;
            }

            if (soonest == 0)
            {
                return;
            }

            var waitFor = TimeSpan.FromSeconds(Math.Max(0.1, soonest - DateTimeOffset.UtcNow.ToUnixTimeSeconds()));
            if (waitFor > heartbeatInterval - (DateTime.UtcNow - lastHeartbeat))
            {
                waitFor = heartbeatInterval - (DateTime.UtcNow - lastHeartbeat);
                if (waitFor < TimeSpan.Zero)
                {
                    waitFor = TimeSpan.Zero;
                }
            }

            await poller.WaitForChangeAsync(waitFor, cancellationToken);
        }
    }

    private sealed class ScheduledFireRow
    {
        [JsonPropertyName("name")]
        public string Name { get; init; } = "";

        [JsonPropertyName("queue")]
        public string Queue { get; init; } = "";

        [JsonPropertyName("fire_at")]
        public long FireAt { get; init; }

        [JsonPropertyName("job_id")]
        public long JobId { get; init; }
    }
}
