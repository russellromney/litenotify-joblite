using System.Runtime.InteropServices;
using System.Text.Json;

namespace Honker.Tests;

public sealed class OutboxLockTests
{
    [Fact]
    public void LockAcquireAndRelease()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        using (var handle = db.Lock("backup", 60))
        {
            Assert.True(handle.Acquired);
            var rows = db.Query("SELECT name, owner FROM _honker_locks");
            Assert.Single(rows);
            Assert.Equal("backup", rows[0]["name"]);
        }

        Assert.Equal(0, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_locks"));
    }

    [Fact]
    public void LockAlreadyHeldRaises()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        using var handle = db.Lock("singleton", 60);
        Assert.Throws<LockHeldException>(() => db.Lock("singleton", 60));
    }

    [Fact]
    public void LockExpiredIsReclaimable()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        using (var tx = db.BeginTransaction())
        {
            tx.Execute(
                "INSERT INTO _honker_locks (name, owner, expires_at) VALUES ('stale', 'crashed-owner', unixepoch() - 3600)"
            );
            tx.Commit();
        }

        using var handle = db.Lock("stale", 60);
        Assert.True(handle.Acquired);
        var rows = db.Query("SELECT owner FROM _honker_locks WHERE name='stale'");
        Assert.Single(rows);
        Assert.NotEqual("crashed-owner", rows[0]["owner"]);
    }

    [Fact]
    public async Task OutboxDeliveryCalledAndAcked()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var delivered = new List<string>();

        var outbox = db.Outbox("webhook", async payload =>
        {
            delivered.Add(payload.GetProperty("body").GetString() ?? "");
            await Task.CompletedTask;
        });
        outbox.Enqueue(new { url = "https://a.com", body = "1" });
        outbox.Enqueue(new { url = "https://b.com", body = "2" });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var worker = Task.Run(() => outbox.RunWorker("w1", cts.Token), cts.Token);
        try
        {
            await WaitUntilAsync(() => delivered.Count == 2, TimeSpan.FromSeconds(2), cts.Token);
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(worker.ContinueWith(_ => Task.CompletedTask));
        }

        Assert.Equal(["1", "2"], delivered);
        Assert.Equal(0, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_jobs WHERE queue=@p0", "_outbox:webhook"));
    }

    [Fact]
    public async Task OutboxRetriesOnException()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var calls = new List<int>();

        var outbox = db.Outbox(
            "retry",
            async payload =>
            {
                calls.Add(payload.GetProperty("n").GetInt32());
                await Task.CompletedTask;
                if (calls.Count < 3)
                {
                    throw new InvalidOperationException("transient");
                }
            },
            new OutboxOptions(MaxAttempts: 5, BaseBackoffSeconds: 0)
        );
        outbox.Enqueue(new { n = 1 });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var worker = Task.Run(() => outbox.RunWorker("w1", cts.Token), cts.Token);
        try
        {
            await WaitUntilAsync(() => calls.Count >= 3, TimeSpan.FromSeconds(3), cts.Token, async () =>
            {
                using var tx = db.BeginTransaction();
                tx.Execute("UPDATE _honker_live SET run_at=unixepoch() - 1 WHERE queue=@p0", "_outbox:retry");
                tx.Commit();
                await Task.CompletedTask;
            });
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(worker.ContinueWith(_ => Task.CompletedTask));
        }

        Assert.True(calls.Count >= 3);
        Assert.Equal(0, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_jobs WHERE queue=@p0", "_outbox:retry"));
    }

    [Fact]
    public void OutboxRollbackDropsEnqueue()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var delivered = new List<int>();

        var outbox = db.Outbox("tx-rollback", payload =>
        {
            delivered.Add(payload.GetProperty("order").GetInt32());
        });

        Assert.Throws<InvalidOperationException>((Action)(() =>
        {
            using var tx = db.BeginTransaction();
            tx.Execute("CREATE TABLE orders (id INTEGER)");
            tx.Execute("INSERT INTO orders (id) VALUES (@p0)", 1);
            outbox.Enqueue(new { order = 1 }, transaction: tx);
            throw new InvalidOperationException("business error");
        }));

        Assert.Equal(0, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_jobs WHERE queue=@p0", "_outbox:tx-rollback"));
        Assert.Empty(delivered);
    }

    [Fact]
    public async Task OutboxEnqueueInTransactionCommitsAtomically()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var delivered = new List<int>();

        var outbox = db.Outbox("tx-commit", payload =>
        {
            delivered.Add(payload.GetProperty("order").GetInt32());
        });

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("CREATE TABLE orders (id INTEGER)");
            tx.Execute("INSERT INTO orders (id) VALUES (@p0)", 1);
            outbox.Enqueue(new { order = 1 }, transaction: tx);
            tx.Commit();
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var worker = Task.Run(() => outbox.RunWorker("w1", cts.Token), cts.Token);
        try
        {
            await WaitUntilAsync(() => delivered.Count == 1, TimeSpan.FromSeconds(2), cts.Token);
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(worker.ContinueWith(_ => Task.CompletedTask));
        }

        Assert.Equal([1], delivered);
        Assert.Equal(1, Scalar(db, "SELECT COUNT(*) AS c FROM orders"));
    }

    [Fact]
    public void OutboxInstanceIsMemoized()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        void Deliver(JsonElement _) { }

        var a = db.Outbox("w", (Action<JsonElement>)Deliver);
        var b = db.Outbox("w", (Action<JsonElement>)Deliver);

        Assert.Same(a, b);
    }

    [Fact]
    public async Task OutboxEventuallyDiesAfterMaxAttempts()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var calls = 0;

        var outbox = db.Outbox(
            "dead",
            async _ =>
            {
                calls += 1;
                await Task.CompletedTask;
                throw new InvalidOperationException("permanent");
            },
            new OutboxOptions(MaxAttempts: 3, BaseBackoffSeconds: 0)
        );
        outbox.Enqueue(new { n = 1 });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var worker = Task.Run(() => outbox.RunWorker("w1", cts.Token), cts.Token);
        try
        {
            await WaitUntilAsync(() =>
            {
                var rows = db.Query("SELECT state FROM _honker_jobs WHERE queue=@p0", "_outbox:dead");
                return rows.Count == 1 && Convert.ToString(rows[0]["state"]) == "dead";
            }, TimeSpan.FromSeconds(3), cts.Token, async () =>
            {
                using var tx = db.BeginTransaction();
                tx.Execute("UPDATE _honker_live SET run_at=unixepoch() - 1 WHERE queue=@p0", "_outbox:dead");
                tx.Commit();
                await Task.CompletedTask;
            });
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(worker.ContinueWith(_ => Task.CompletedTask));
        }

        var row = db.Query("SELECT state, attempts, last_error FROM _honker_jobs WHERE queue=@p0", "_outbox:dead")[0];
        Assert.Equal("dead", row["state"]);
        Assert.Equal(3L, row["attempts"]);
        Assert.Contains("permanent", Convert.ToString(row["last_error"]));
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task OutboxSyncDeliveryFunctionWorks()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var delivered = new List<int>();

        var outbox = db.Outbox("sync", payload =>
        {
            delivered.Add(payload.GetProperty("a").GetInt32());
        });
        outbox.Enqueue(new { a = 1 });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var worker = Task.Run(() => outbox.RunWorker("w1", cts.Token), cts.Token);
        try
        {
            await WaitUntilAsync(() => delivered.Count == 1, TimeSpan.FromSeconds(2), cts.Token);
        }
        finally
        {
            cts.Cancel();
            await Task.WhenAll(worker.ContinueWith(_ => Task.CompletedTask));
        }

        Assert.Equal([1], delivered);
    }

    [Fact]
    public async Task OutboxWorkerStopsCleanlyOnCancel()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        var outbox = db.Outbox("cancel", async _ => await Task.CompletedTask);
        using var cts = new CancellationTokenSource();
        var worker = Task.Run(() => outbox.RunWorker("w1", cts.Token), cts.Token);
        await Task.Delay(50);

        var watch = System.Diagnostics.Stopwatch.StartNew();
        cts.Cancel();
        var stopped = await Task.WhenAny(worker, Task.Delay(TimeSpan.FromSeconds(2))) == worker;
        Assert.True(stopped, $"worker cancel was too slow: {watch.Elapsed}");
        if (!worker.IsCompletedSuccessfully)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await worker);
        }
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(2), $"worker cancel was too slow: {watch.Elapsed}");
    }

    private static long Scalar(Database db, string sql, params object?[] args)
    {
        var rows = db.Query(sql, args);
        return Convert.ToInt64(rows[0].Values.First() ?? 0L);
    }

    private static async Task WaitUntilAsync(
        Func<bool> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        Func<Task>? tick = null)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (predicate())
            {
                return;
            }

            if (tick is not null)
            {
                await tick();
            }

            await Task.Delay(20, cancellationToken);
        }

        throw new TimeoutException("condition not reached before timeout");
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
            var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-outbox-{Guid.NewGuid():N}");
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

        public void Dispose()
        {
            try
            {
                Directory.Delete(_dir, recursive: true);
            }
            catch
            {
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
    }
}
