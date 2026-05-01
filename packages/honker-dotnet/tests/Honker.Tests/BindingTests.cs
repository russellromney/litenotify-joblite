using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text.Json;

namespace Honker.Tests;

public sealed class BindingTests
{
    [Fact]
    public void EnqueueAndClaimRoundTrip()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");

        queue.Enqueue(new { k = 1 });
        var job = queue.ClaimOne("w1");

        Assert.NotNull(job);
        Assert.Equal(1, job!.Payload.GetProperty("k").GetInt32());
        Assert.Equal("processing", job.State);
        Assert.Equal(1, job.Attempts);
        Assert.True(job.Ack());

        var rows = db.Query("SELECT state FROM _honker_jobs WHERE id=@p0", job.Id);
        Assert.Empty(rows);
    }

    [Fact]
    public void TwoWorkersClaimReturnsExactlyOneWinner()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");
        queue.Enqueue(new { n = 1 });

        var winners = new ConcurrentBag<Job>();
        var barrier = new Barrier(8);
        var threads = Enumerable.Range(0, 8).Select(i => new Thread(() =>
        {
            barrier.SignalAndWait();
            var job = queue.ClaimOne($"w-{i}");
            if (job is not null)
            {
                winners.Add(job);
            }
        })).ToList();

        foreach (var thread in threads) thread.Start();
        foreach (var thread in threads) thread.Join();

        Assert.Single(winners);
        Assert.Equal(1, winners.Single().Attempts);
    }

    [Fact]
    public void ExpiredClaimCannotAckAndIsReclaimable()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work", new QueueOptions(VisibilityTimeoutSeconds: 1));

        queue.Enqueue(new { n = 1 });
        var slow = queue.ClaimOne("slow");
        Assert.NotNull(slow);

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("UPDATE _honker_live SET claim_expires_at = unixepoch() - 1 WHERE id=@p0", slow!.Id);
            tx.Commit();
        }

        var fast = queue.ClaimOne("fast");
        Assert.NotNull(fast);
        Assert.Equal(slow!.Id, fast!.Id);
        Assert.Equal(2, fast.Attempts);
        Assert.False(slow.Ack());
        Assert.True(fast.Ack());
    }

    [Fact]
    public void HeartbeatOnlyExtendsMatchingWorker()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work", new QueueOptions(VisibilityTimeoutSeconds: 2));

        queue.Enqueue(new { n = 1 });
        var job = queue.ClaimOne("owner");
        Assert.NotNull(job);

        Assert.False(queue.Heartbeat(job!.Id, "imposter", 60));
        var before = ScalarInt64(db, "SELECT claim_expires_at FROM _honker_jobs WHERE id=@p0", job.Id);
        Thread.Sleep(50);
        Assert.True(queue.Heartbeat(job.Id, "owner", 600));
        var after = ScalarInt64(db, "SELECT claim_expires_at FROM _honker_jobs WHERE id=@p0", job.Id);
        Assert.True(after > before);
    }

    [Fact]
    public async Task BeginImmediateUnderConcurrentReaders()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");
        using var stop = new CancellationTokenSource();

        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                db.Query("SELECT COUNT(*) AS c FROM _honker_jobs");
            }
        }, stop.Token)).ToList();

        try
        {
            for (var i = 0; i < 200; i += 1)
            {
                queue.Enqueue(new { i });
            }
        }
        finally
        {
            stop.Cancel();
            await Task.WhenAll(readers);
        }

        var count = ScalarInt64(db, "SELECT COUNT(*) AS c FROM _honker_jobs");
        Assert.Equal(200, count);
    }

    [Fact]
    public void RollbackOfBusinessTransactionDropsEnqueue()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");

        InvalidOperationException? rollbackError = null;
        try
        {
            using var tx = db.BeginTransaction();
            tx.Execute("CREATE TABLE orders (id INTEGER)");
            tx.Execute("INSERT INTO orders (id) VALUES (@p0)", 1);
            queue.Enqueue(new { order = 1 }, transaction: tx);
            throw new InvalidOperationException("business error");
        }
        catch (InvalidOperationException ex)
        {
            rollbackError = ex;
        }
        Assert.NotNull(rollbackError);

        Assert.ThrowsAny<Exception>(() => db.Query("SELECT * FROM orders"));
        Assert.Equal(0, ScalarInt64(db, "SELECT COUNT(*) AS c FROM _honker_jobs"));
    }

    [Fact]
    public void EnqueueInRequestTransactionCommitsAtomically()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("CREATE TABLE orders (id INTEGER)");
            tx.Execute("INSERT INTO orders (id) VALUES (@p0)", 1);
            queue.Enqueue(new { order = 1 }, transaction: tx);
            tx.Commit();
        }

        Assert.Equal(1, ScalarInt64(db, "SELECT id FROM orders"));
        Assert.Equal(1, ScalarInt64(db, "SELECT COUNT(*) AS c FROM _honker_jobs"));
    }

    [Fact]
    public void PriorityOrderingMatchesPython()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");

        queue.Enqueue(new { n = 1 }, new EnqueueOptions(Priority: 0));
        queue.Enqueue(new { n = 2 }, new EnqueueOptions(Priority: 10));
        queue.Enqueue(new { n = 3 }, new EnqueueOptions(Priority: 5));

        var got = new List<int>();
        for (var i = 0; i < 3; i += 1)
        {
            var job = queue.ClaimOne("w");
            Assert.NotNull(job);
            got.Add(job!.Payload.GetProperty("n").GetInt32());
        }

        Assert.Equal([2, 3, 1], got);
    }

    [Fact]
    public void DelayedRunAtIsNotClaimedUntilDue()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");
        var future = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 3600;

        queue.Enqueue(new { n = 1 }, new EnqueueOptions(RunAtUnix: future));
        Assert.Null(queue.ClaimOne("w"));

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("UPDATE _honker_live SET run_at = unixepoch() - 1 WHERE payload = @p0", "{\"n\":1}");
            tx.Commit();
        }

        var job = queue.ClaimOne("w");
        Assert.NotNull(job);
        Assert.Equal(1, job!.Payload.GetProperty("n").GetInt32());
    }

    [Fact]
    public void MaxAttemptsTransitionsToDead()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work", new QueueOptions(MaxAttempts: 2));

        queue.Enqueue(new { n = 1 });

        var first = queue.ClaimOne("w");
        Assert.NotNull(first);
        Assert.Equal(1, first!.Attempts);
        Assert.True(first.Retry(0, "try1"));

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("UPDATE _honker_live SET run_at = unixepoch() - 1");
            tx.Commit();
        }

        var second = queue.ClaimOne("w");
        Assert.NotNull(second);
        Assert.Equal(2, second!.Attempts);
        Assert.True(second.Retry(0, "try2"));

        var rows = db.Query("SELECT state, last_error FROM _honker_jobs");
        Assert.Equal("dead", rows[0]["state"]);
        Assert.Equal("try2", rows[0]["last_error"]);
    }

    [Fact]
    public void FailGoesStraightToDead()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work", new QueueOptions(MaxAttempts: 10));

        queue.Enqueue(new { n = 1 });
        var job = queue.ClaimOne("w");
        Assert.NotNull(job);
        Assert.True(job!.Fail("bad payload"));

        var state = Convert.ToString(db.Query("SELECT state FROM _honker_jobs")[0]["state"]);
        Assert.Equal("dead", state);
    }

    [Fact]
    public async Task WorkerWakesOnNotifyFastPath()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var loopStart = Stopwatch.GetTimestamp();
        var task = Task.Run(async () =>
        {
            await foreach (var job in queue.ClaimAsync("w", TimeSpan.FromSeconds(5), cts.Token))
            {
                job.Ack();
                return Stopwatch.GetElapsedTime(loopStart);
            }

            throw new InvalidOperationException("claim loop exited unexpectedly");
        });

        await Task.Delay(100, cts.Token);
        var enqueueAt = Stopwatch.GetTimestamp();
        queue.Enqueue(new { n = 1 });
        var elapsed = await task;
        var wakeDelay = Stopwatch.GetElapsedTime(enqueueAt, Stopwatch.GetTimestamp());
        Assert.True(elapsed < TimeSpan.FromSeconds(2), $"worker wake was too slow: {elapsed}");
        Assert.True(wakeDelay < TimeSpan.FromSeconds(2), $"enqueue wake was too slow: {wakeDelay}");
    }

    [Fact]
    public void EnqueueReturnsIncreasingIds()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("idq");

        var first = queue.Enqueue(new { a = 1 });
        var second = queue.Enqueue(new { a = 2 });

        Assert.True(first > 0);
        Assert.True(second > first);
    }

    [Fact]
    public void QueueInstanceIsMemoized()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        var q1 = db.Queue("work");
        var q2 = db.Queue("work");

        Assert.Same(q1, q2);
    }

    [Fact]
    public void OpenResolvesBundledExtensionFromRuntimeFolder()
    {
        using var harness = TestHarness.Create();
        var previous = Environment.GetEnvironmentVariable("HONKER_EXTENSION_PATH");
        Environment.SetEnvironmentVariable("HONKER_EXTENSION_PATH", null);
        harness.StageBundledExtension();

        try
        {
            using var db = Database.Open(harness.DatabasePath);
            var queue = db.Queue("bundled");
            queue.Enqueue(new { ok = true });
            var job = queue.ClaimOne("w");
            Assert.NotNull(job);
            Assert.True(job!.Ack());
        }
        finally
        {
            Environment.SetEnvironmentVariable("HONKER_EXTENSION_PATH", previous);
        }
    }

    [Fact]
    public void EnqueueInTransactionReturnsInsertedId()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("tx-id");

        long id;
        using (var tx = db.BeginTransaction())
        {
            id = queue.Enqueue(new { a = 1 }, transaction: tx);
            tx.Commit();
        }

        Assert.Equal(id, ScalarInt64(db, "SELECT id FROM _honker_live WHERE queue='tx-id'"));
    }

    [Fact]
    public void RetryResetsWorkerAndClaim()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("retry");

        queue.Enqueue(new { n = 1 });
        var job = queue.ClaimOne("w");

        Assert.NotNull(job);
        Assert.True(job!.Retry(0, "e"));

        var row = db.Query("SELECT worker_id, claim_expires_at, state FROM _honker_jobs")[0];
        Assert.Null(row["worker_id"]);
        Assert.Null(row["claim_expires_at"]);
        Assert.Equal("pending", row["state"]);
    }

    [Fact]
    public void SaveAndGetResultRoundTrip()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("res");

        queue.SaveResult(42, new { status = "ok" });
        var (found, value) = queue.GetResult(42);

        Assert.True(found);
        Assert.NotNull(value);
        Assert.Equal("ok", value!.Value.GetProperty("status").GetString());
    }

    [Fact]
    public void GetResultMissingIsDisambiguated()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("missing");

        var (found, value) = queue.GetResult(999);
        Assert.False(found);
        Assert.Null(value);
    }

    [Fact]
    public void SaveResultReplacesExistingValue()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("replace");

        queue.SaveResult(1, "first");
        queue.SaveResult(1, "second");
        var value = queue.GetResult<string>(1, out var found);

        Assert.True(found);
        Assert.Equal("second", value);
    }

    [Fact]
    public void SaveResultInTransactionRollsBackWithTransaction()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("rollback");

        InvalidOperationException? rollbackError = null;
        try
        {
            using var tx = db.BeginTransaction();
            queue.SaveResult(50, new { v = 1 }, transaction: tx);
            throw new InvalidOperationException("boom");
        }
        catch (InvalidOperationException ex)
        {
            rollbackError = ex;
        }
        Assert.NotNull(rollbackError);

        var (found, _) = queue.GetResult(50);
        Assert.False(found);
    }

    [Fact]
    public async Task WaitResultReturnsImmediatelyIfPresent()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("wait-immediate");
        queue.SaveResult(1, new { answer = 42 });

        var value = await queue.WaitResult(1);
        Assert.NotNull(value);
        Assert.Equal(42, value!.Value.GetProperty("answer").GetInt32());
    }

    [Fact]
    public async Task WaitResultBlocksUntilSaved()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("wait-blocks");

        var saveTask = Task.Run(async () =>
        {
            await Task.Delay(100);
            queue.SaveResult(7, "done");
        });

        var value = await queue.WaitResult(7);
        Assert.NotNull(value);
        Assert.Equal("done", value!.Value.GetString());
        await saveTask;
    }

    [Fact]
    public async Task WaitResultObservesSaveRacingTheCall()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("wait-race");
        var jobId = queue.Enqueue(new { k = 1 });

        var saver = new Thread(() =>
        {
            Thread.Sleep(50);
            queue.SaveResult(jobId, new { ok = true });
        });
        saver.Start();

        var watch = Stopwatch.StartNew();
        var value = await queue.WaitResult(jobId, TimeSpan.FromSeconds(2));
        saver.Join();

        Assert.NotNull(value);
        Assert.True(value!.Value.GetProperty("ok").GetBoolean());
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1), $"wait_result resolved too slowly: {watch.Elapsed}");
    }

    [Fact]
    public async Task WaitResultTimesOut()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("wait-timeout");

        await Assert.ThrowsAsync<TimeoutException>(() => queue.WaitResult(123, TimeSpan.FromMilliseconds(200)));
    }

    [Fact]
    public void SchedulerAddRegistersTask()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        db.Scheduler.Add(new ScheduledTask(
            Name: "nightly",
            Queue: "backups",
            Schedule: "0 3 * * *"
        ));

        var rows = db.Query("SELECT name, queue, cron_expr FROM _honker_scheduler_tasks WHERE name='nightly'");
        Assert.Single(rows);
        Assert.Equal("backups", rows[0]["queue"]);
        Assert.Equal("0 3 * * *", rows[0]["cron_expr"]);
    }

    [Fact]
    public void SchedulerAddReplacesByName()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        db.Scheduler.Add(new ScheduledTask(Name: "t", Queue: "a", Schedule: "* * * * *"));
        db.Scheduler.Add(new ScheduledTask(Name: "t", Queue: "b", Schedule: "* * * * *"));
        var rows = db.Query("SELECT queue FROM _honker_scheduler_tasks WHERE name='t'");

        Assert.Single(rows);
        Assert.Equal("b", rows[0]["queue"]);
    }

    [Fact]
    public void SchedulerTickEnqueuesOnBoundary()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        db.Queue("hourly-q");

        db.Scheduler.Add(new ScheduledTask(
            Name: "hourly",
            Queue: "hourly-q",
            Schedule: "0 * * * *",
            Payload: new { ping = true }
        ));

        var boundary = ScalarInt64(db, "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='hourly'");
        var fires = db.Scheduler.Tick(boundary + 1);

        Assert.Single(fires);
        Assert.Equal("hourly", fires[0].Name);
        Assert.Equal("hourly-q", fires[0].Queue);
        Assert.Equal(boundary, fires[0].FireAt);
        Assert.Equal(1, ScalarInt64(db, "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='hourly-q'"));
        Assert.Equal(boundary + 3600, ScalarInt64(db, "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='hourly'"));
    }

    [Fact]
    public void SchedulerTickSkipsAlreadyFired()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        db.Queue("no-dup");

        db.Scheduler.Add(new ScheduledTask(Name: "t", Queue: "no-dup", Schedule: "* * * * *"));
        var boundary = ScalarInt64(db, "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name='t'");
        var first = db.Scheduler.Tick(boundary + 1);
        var second = db.Scheduler.Tick(boundary + 1);

        Assert.Single(first);
        Assert.Empty(second);
        Assert.Equal(1, ScalarInt64(db, "SELECT COUNT(*) AS c FROM _honker_live WHERE queue='no-dup'"));
    }

    [Fact]
    public async Task ClaimAsyncWakesWhenRunAtDeadlineArrives()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        if (!harness.SupportsQueueNextClaimAt())
        {
            return;
        }

        var queue = db.Queue("runat");
        var runAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + 1;
        queue.Enqueue(new { x = 1 }, new EnqueueOptions(RunAtUnix: runAt));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var watch = Stopwatch.StartNew();
        await using var enumerator = queue.ClaimAsync("worker-1", TimeSpan.FromSeconds(30), cts.Token).GetAsyncEnumerator(cts.Token);
        Assert.True(await enumerator.MoveNextAsync());

        var job = enumerator.Current;
        Assert.True(watch.Elapsed >= TimeSpan.FromMilliseconds(700), $"run_at wake came too early: {watch.Elapsed}");
        Assert.True(watch.Elapsed <= TimeSpan.FromMilliseconds(2500), $"run_at wake came too late: {watch.Elapsed}");
        Assert.NotNull(job);
        Assert.True(job.Ack());
    }

    [Fact]
    public async Task ClaimAsyncObservesEnqueueRacingConstruction()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var queue = db.Queue("work");
        var iterator = queue.ClaimAsync("w1", TimeSpan.FromSeconds(30));

        var enqueuer = new Thread(() =>
        {
            Thread.Sleep(50);
            queue.Enqueue(new { n = 1 });
        });
        enqueuer.Start();

        var watch = Stopwatch.StartNew();
        await using var enumerator = iterator.GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        enqueuer.Join();

        var job = enumerator.Current;
        Assert.Equal(1, job.Payload.GetProperty("n").GetInt32());
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1), $"claim iterator woke too slowly: {watch.Elapsed}");
        Assert.True(job.Ack());
    }

    [Fact]
    public void SchedulerAcceptsEverySecondAndLegacyCronAlias()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        if (!harness.SupportsEverySecondSchedules())
        {
            return;
        }

        db.Scheduler.Add(new ScheduledTask(Name: "fast", Queue: "beats", Payload: new { ok = true }, Schedule: Schedules.EverySeconds(1)));
        Assert.True(db.Scheduler.Soonest() > 0);

        var fires = db.Scheduler.Tick(db.Scheduler.Soonest());
        Assert.Single(fires);
        Assert.Equal("fast", fires[0].Name);

        db.Scheduler.Add(new ScheduledTask(Name: "legacy", Queue: "beats", Payload: new { ok = true }, Cron: "@every 1s"));
        Assert.True(db.Scheduler.Soonest() > 0);
    }

    private static long ScalarInt64(Database db, string sql, params object?[] args)
    {
        var rows = db.Query(sql, args);
        Assert.NotEmpty(rows);
        var first = rows[0].Values.First();
        return Convert.ToInt64(first);
    }

    private sealed class TestHarness : IDisposable
    {
        private readonly string _dir;

        private TestHarness(string dir, string extensionPath)
        {
            _dir = dir;
            ExtensionPath = extensionPath;
            DatabasePath = Path.Combine(dir, "test.db");
        }

        public string ExtensionPath { get; }
        public string DatabasePath { get; }

        public static TestHarness Create()
        {
            var root = FindRepoRoot();
            var extensionPath = FindExtension(root);
            var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            return new TestHarness(dir, extensionPath);
        }

        public Database Open()
        {
            return Database.Open(DatabasePath, new OpenOptions
            {
                ExtensionPath = ExtensionPath,
                UpdatePollInterval = TimeSpan.FromMilliseconds(5),
            });
        }

        public void StageBundledExtension()
        {
            var dir = Path.Combine(AppContext.BaseDirectory, "runtimes", CurrentRid(), "native");
            Directory.CreateDirectory(dir);
            File.Copy(ExtensionPath, Path.Combine(dir, ExtensionFileName()), overwrite: true);
        }

        public bool SupportsQueueNextClaimAt()
        {
            using var db = Open();
            return db.FunctionExists("honker_queue_next_claim_at");
        }

        public bool SupportsEverySecondSchedules()
        {
            using var db = Open();
            try
            {
                using var command = db.RawConnection.CreateCommand();
                command.CommandText = "SELECT honker_cron_next_after(@expr, @from)";
                command.Parameters.AddWithValue("@expr", "@every 1s");
                command.Parameters.AddWithValue("@from", 0);
                command.ExecuteScalar();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(_dir, recursive: true);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }

        private static string FindRepoRoot()
        {
            var current = AppContext.BaseDirectory;
            while (!string.IsNullOrEmpty(current))
            {
                if (Directory.Exists(Path.Combine(current, "honker-core")) &&
                    File.Exists(Path.Combine(current, "Cargo.toml")))
                {
                    return current;
                }

                current = Path.GetDirectoryName(current) ?? "";
            }

            throw new DirectoryNotFoundException("could not locate honker repo root from test base directory");
        }

        private static string FindExtension(string root)
        {
            var candidates = new[]
            {
                Path.Combine(root, "target", "debug", ExtensionFileName()),
                Path.Combine(root, "target", "release", ExtensionFileName()),
            };

            var found = candidates.FirstOrDefault(File.Exists);
            if (found is null)
            {
                throw new FileNotFoundException($"expected built honker extension at one of: {string.Join(", ", candidates)}");
            }

            return found;
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

        private static string CurrentRid()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "win-arm64" : "win-x64";
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "osx-arm64" : "osx-x64";
            }

            return RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "linux-arm64" : "linux-x64";
        }
    }
}
