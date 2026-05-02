using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;

namespace Honker;

public sealed class Queue
{
    private readonly Database _database;
    private readonly QueueOptions _options;

    internal Queue(Database database, string name, QueueOptions options)
    {
        _database = database;
        _options = options;
        Name = name;
        EnsureBindingSchema();
    }

    public string Name { get; }

    public long Enqueue(object? payload, EnqueueOptions? options = null, HonkerTransaction? transaction = null)
    {
        options ??= new EnqueueOptions();
        var payloadJson = JsonSerializer.Serialize(payload);
        long? delaySeconds = options.DelaySeconds is null ? null : Convert.ToInt64(options.DelaySeconds.Value);
        long? expiresSeconds = options.ExpiresSeconds is null ? null : Convert.ToInt64(options.ExpiresSeconds.Value);
        var id = Convert.ToInt64(_database.ExecuteScalar(
            "SELECT honker_enqueue(@p0, @p1, @p2, @p3, @p4, @p5, @p6)",
            transaction,
            Name,
            payloadJson,
            options.RunAtUnix,
            delaySeconds,
            options.Priority,
            options.MaxAttempts ?? _options.MaxAttempts,
            expiresSeconds
        ));
        return id;
    }

    public IReadOnlyList<Job> ClaimBatch(string workerId, int batchSize)
    {
        var rowsJson = Convert.ToString(_database.ExecuteScalar(
            "SELECT honker_claim_batch(@p0, @p1, @p2, @p3)",
            null,
            Name,
            workerId,
            batchSize,
            _options.VisibilityTimeoutSeconds
        )) ?? "[]";

        var rows = JsonSerializer.Deserialize<List<ClaimedJobRow>>(rowsJson) ?? [];
        return rows
            .Select(row => new Job(this, row.Id, row.Queue, row.Payload, row.WorkerId, row.Attempts, row.ClaimExpiresAt, row.State ?? "processing"))
            .ToList();
    }

    public Job? ClaimOne(string workerId)
    {
        return ClaimBatch(workerId, 1).FirstOrDefault();
    }

    public async IAsyncEnumerable<Job> ClaimAsync(
        string workerId,
        TimeSpan? idlePoll = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var poller = _database.CreatePoller();
        var fallback = idlePoll ?? TimeSpan.FromSeconds(5);

        while (!cancellationToken.IsCancellationRequested)
        {
            var job = ClaimOne(workerId);
            if (job is not null)
            {
                yield return job;
                continue;
            }

            var waitFor = fallback;
            var nextClaimAt = NextClaimAt();
            if (nextClaimAt > 0)
            {
                var untilNext = TimeSpan.FromSeconds(nextClaimAt - DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                if (untilNext <= TimeSpan.Zero)
                {
                    continue;
                }

                if (untilNext < waitFor)
                {
                    waitFor = untilNext;
                }
            }

            await poller.WaitForChangeAsync(waitFor, cancellationToken);
        }
    }

    public long NextClaimAt()
    {
        if (!_database.FunctionExists("honker_queue_next_claim_at"))
        {
            return 0;
        }

        try
        {
            return Convert.ToInt64(_database.ExecuteScalar(
                "SELECT honker_queue_next_claim_at(@p0)",
                null,
                Name
            ) ?? 0L);
        }
        catch (SqliteException ex) when (ex.Message.Contains("no such function", StringComparison.OrdinalIgnoreCase))
        {
            return 0;
        }
    }

    public int AckBatch(IEnumerable<long> ids, string workerId)
    {
        var json = JsonSerializer.Serialize(ids);
        return Convert.ToInt32(_database.ExecuteScalar(
            "SELECT honker_ack_batch(@p0, @p1)",
            null,
            json,
            workerId
        ) ?? 0);
    }

    public bool Ack(long jobId, string workerId)
    {
        return Convert.ToInt64(_database.ExecuteScalar(
            "SELECT honker_ack(@p0, @p1)",
            null,
            jobId,
            workerId
        ) ?? 0L) == 1;
    }

    public bool Retry(long jobId, string workerId, int delaySeconds, string error)
    {
        return Convert.ToInt64(_database.ExecuteScalar(
            "SELECT honker_retry(@p0, @p1, @p2, @p3)",
            null,
            jobId,
            workerId,
            delaySeconds,
            error
        ) ?? 0L) == 1;
    }

    public bool Fail(long jobId, string workerId, string error)
    {
        return Convert.ToInt64(_database.ExecuteScalar(
            "SELECT honker_fail(@p0, @p1, @p2)",
            null,
            jobId,
            workerId,
            error
        ) ?? 0L) == 1;
    }

    public bool Heartbeat(long jobId, string workerId, int extendSeconds)
    {
        return Heartbeat(jobId, workerId, (int?)extendSeconds);
    }

    public bool Heartbeat(long jobId, string workerId, int? extendSeconds = null)
    {
        return Convert.ToInt64(_database.ExecuteScalar(
            "SELECT honker_heartbeat(@p0, @p1, @p2)",
            null,
            jobId,
            workerId,
            extendSeconds ?? _options.VisibilityTimeoutSeconds
        ) ?? 0L) == 1;
    }

    public void SaveResult(long jobId, object? value, int ttlSeconds = 0, HonkerTransaction? transaction = null)
    {
        _database.ExecuteScalar(
            "SELECT honker_result_save(@p0, @p1, @p2)",
            transaction,
            jobId,
            JsonSerializer.Serialize(value),
            ttlSeconds
        );
    }

    public (bool Found, JsonElement? Value) GetResult(long jobId)
    {
        var raw = _database.ExecuteScalar(
            "SELECT honker_result_get(@p0)",
            null,
            jobId
        );

        if (raw is null or DBNull)
        {
            return (false, null);
        }

        var text = Convert.ToString(raw)!;
        using var doc = JsonDocument.Parse(text);
        return (true, doc.RootElement.Clone());
    }

    public T? GetResult<T>(long jobId, out bool found)
    {
        var result = GetResult(jobId);
        found = result.Found;
        if (!result.Found || result.Value is null)
        {
            return default;
        }

        return JsonSerializer.Deserialize<T>(result.Value.Value.GetRawText());
    }

    public int SweepResults()
    {
        return Convert.ToInt32(_database.ExecuteScalar(
            "SELECT honker_result_sweep()",
            null
        ) ?? 0);
    }

    public async Task<JsonElement?> WaitResult(long jobId, TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        using var poller = _database.CreatePoller();
        var deadline = timeout is null ? (DateTime?)null : DateTime.UtcNow + timeout.Value;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var result = GetResult(jobId);
            if (result.Found)
            {
                return result.Value;
            }

            var waitFor = deadline is null
                ? TimeSpan.FromSeconds(15)
                : deadline.Value - DateTime.UtcNow;
            if (deadline is not null && waitFor <= TimeSpan.Zero)
            {
                throw new TimeoutException($"WaitResult({jobId}) timed out");
            }

            if (waitFor < TimeSpan.Zero)
            {
                waitFor = TimeSpan.Zero;
            }

            await poller.WaitForChangeAsync(waitFor, cancellationToken);
        }
    }

    private void EnsureBindingSchema()
    {
        using var tx = _database.BeginTransaction();
        tx.Execute("DROP VIEW IF EXISTS _honker_jobs");
        tx.Execute(
            """
            CREATE VIEW _honker_jobs AS
              SELECT id, queue, payload, state, priority, run_at,
                     worker_id, claim_expires_at,
                     attempts, max_attempts, NULL AS last_error, created_at
                FROM _honker_live
              UNION ALL
              SELECT id, queue, payload, 'dead' AS state, priority,
                     run_at, NULL, NULL,
                     attempts, max_attempts, last_error, created_at
                FROM _honker_dead
            """
        );
        tx.Execute("DROP INDEX IF EXISTS _honker_jobs_claim");
        tx.Execute("DROP INDEX IF EXISTS _honker_jobs_claim_v2");
        tx.Execute("DROP INDEX IF EXISTS _honker_pending_claim");
        tx.Execute("DROP INDEX IF EXISTS _honker_processing_reclaim");
        tx.Execute("DROP TABLE IF EXISTS _honker_pending");
        tx.Execute("DROP TABLE IF EXISTS _honker_processing");
        tx.Commit();
    }

    private sealed class ClaimedJobRow
    {
        [JsonPropertyName("id")]
        public long Id { get; init; }

        [JsonPropertyName("queue")]
        public string Queue { get; init; } = "";

        [JsonPropertyName("payload")]
        public string Payload { get; init; } = "";

        [JsonPropertyName("worker_id")]
        public string WorkerId { get; init; } = "";

        [JsonPropertyName("attempts")]
        public long Attempts { get; init; }

        [JsonPropertyName("claim_expires_at")]
        public long ClaimExpiresAt { get; init; }

        [JsonPropertyName("state")]
        public string? State { get; init; }
    }
}
