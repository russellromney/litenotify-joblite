using System.Runtime.InteropServices;
using System.Text.Json;

namespace Honker.Tests;

public sealed class NotifyStreamTests
{
    [Fact]
    public async Task ListenEmitsOnlyMatchingChannel()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        var got = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var listener = db.Listen("orders");
        var task = Task.Run(async () =>
        {
            await foreach (var notification in listener.WithCancellation(cts.Token))
            {
                got.Add(AsString(notification.Payload));
                if (got.Count == 2)
                {
                    return;
                }
            }
        }, cts.Token);

        db.Notify("unrelated", "skip");
        db.Notify("orders", "one");
        db.Notify("other", "skip");
        db.Notify("orders", "two");

        await task;
        Assert.Equal(["one", "two"], got);
    }

    [Fact]
    public async Task MultipleListenersSameChannelAllReceive()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        static async Task<List<string>> Collect(Database db, CancellationToken cancellationToken)
        {
            var seen = new List<string>();
            await foreach (var notification in db.Listen("ch").WithCancellation(cancellationToken))
            {
                seen.Add(AsString(notification.Payload));
                if (seen.Count == 3)
                {
                    return seen;
                }
            }

            return seen;
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var t1 = Collect(db, cts.Token);
        var t2 = Collect(db, cts.Token);
        await Task.Delay(50, cts.Token);

        for (var i = 0; i < 3; i += 1)
        {
            db.Notify("ch", $"m{i}");
        }

        var r1 = await t1;
        var r2 = await t2;
        Assert.Equal(["m0", "m1", "m2"], r1);
        Assert.Equal(["m0", "m1", "m2"], r2);
    }

    [Fact]
    public async Task ListenObservesPublishRacingConstruction()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var listener = db.Listen("orders");

        db.Notify("orders", new { id = 42 });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await using var enumerator = listener.GetAsyncEnumerator(cts.Token);
        Assert.True(await enumerator.MoveNextAsync());

        var payload = (JsonElement)enumerator.Current.Payload!;
        Assert.Equal("orders", enumerator.Current.Channel);
        Assert.Equal(42, payload.GetProperty("id").GetInt32());
    }

    [Fact]
    public async Task RollbackDropsNotification()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        var got = new List<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var task = Task.Run(async () =>
        {
            await foreach (var notification in db.Listen("ch").WithCancellation(cts.Token))
            {
                got.Add(AsString(notification.Payload));
                if (got.Count == 1)
                {
                    return;
                }
            }
        }, cts.Token);

        await Task.Delay(50, cts.Token);

        try
        {
            using var tx = db.BeginTransaction();
            tx.Execute("CREATE TABLE x (id INTEGER)");
            tx.Notify("ch", "dropped");
            throw new InvalidOperationException("boom");
        }
        catch (InvalidOperationException)
        {
        }

        db.Notify("ch", "delivered");
        await task;
        Assert.Equal(["delivered"], got);
    }

    [Fact]
    public async Task NotifyPayloadRoundTripsCommonJsonShapes()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        var cases = new object?[]
        {
            new { id = 42, name = "alice" },
            new[] { 1, 2, 3 },
            "hello",
            42,
            3.14,
            null,
            true,
        };
        var got = new List<string>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var task = Task.Run(async () =>
        {
            await foreach (var notification in db.Listen("rt").WithCancellation(cts.Token))
            {
                got.Add(Normalize(notification.Payload));
                if (got.Count == cases.Length)
                {
                    return;
                }
            }
        }, cts.Token);

        await Task.Delay(50, cts.Token);
        using (var tx = db.BeginTransaction())
        {
            foreach (var payload in cases)
            {
                tx.Notify("rt", payload);
            }
            tx.Commit();
        }

        await task;
        Assert.Equal(cases.Select(Normalize).ToArray(), got.ToArray());
    }

    [Fact]
    public void PruneNotificationsByMaxKeep()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        for (var i = 0; i < 100; i += 1)
        {
            db.Notify("ch", $"n{i}");
        }

        Assert.Equal(100, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_notifications"));
        Assert.Equal(90, db.PruneNotifications(maxKeep: 10));
        Assert.Equal(10, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_notifications"));
    }

    [Fact]
    public void PruneNotificationsNoArgsIsNoOp()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        db.Notify("ch", "a");

        Assert.Equal(0, db.PruneNotifications());
        Assert.Equal(1, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_notifications"));
    }

    [Fact]
    public void PruneNotificationsByAge()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("INSERT INTO _honker_notifications (channel, payload, created_at) VALUES ('ch', 'old1', 0)");
            tx.Execute("INSERT INTO _honker_notifications (channel, payload, created_at) VALUES ('ch', 'old2', 0)");
            tx.Commit();
        }
        db.Notify("ch", "fresh");

        Assert.Equal(2, db.PruneNotifications(olderThanSeconds: 1));
        var rows = db.Query("SELECT payload FROM _honker_notifications ORDER BY id");
        Assert.Single(rows);
        Assert.Equal("\"fresh\"", rows[0]["payload"]);
    }

    [Fact]
    public void PublishAndReadBack()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");

        stream.Publish(new { a = 1 });
        stream.Publish(new { a = 2 }, key: "k");
        var rows = db.Query("SELECT offset, topic, key, payload FROM _honker_stream ORDER BY offset");

        Assert.Equal(2, rows.Count);
        Assert.Null(rows[0]["key"]);
        Assert.Equal("k", rows[1]["key"]);
    }

    [Fact]
    public void PublishInTransactionAtomicWithBusinessWrite()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");

        using (var tx = db.BeginTransaction())
        {
            tx.Execute("CREATE TABLE users (id INTEGER PRIMARY KEY)");
            tx.Execute("INSERT INTO users (id) VALUES (@p0)", 1);
            stream.Publish(new { u = 1 }, transaction: tx);
            tx.Commit();
        }

        Assert.Equal(1, Scalar(db, "SELECT COUNT(*) AS c FROM users"));
        Assert.Equal(1, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_stream"));
    }

    [Fact]
    public void RollbackDropsPublishedEvent()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");

        try
        {
            using var tx = db.BeginTransaction();
            tx.Execute("CREATE TABLE x (id INTEGER)");
            stream.Publish(new { lost = true }, transaction: tx);
            throw new InvalidOperationException("boom");
        }
        catch (InvalidOperationException)
        {
        }

        Assert.Equal(0, Scalar(db, "SELECT COUNT(*) AS c FROM _honker_stream"));
    }

    [Fact]
    public void OffsetSaveIsMonotonic()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");

        stream.SaveOffset("c", 5);
        stream.SaveOffset("c", 3);
        stream.SaveOffset("c", 10);
        Assert.Equal(10, stream.GetOffset("c"));
    }

    [Fact]
    public async Task SubscribeReplaysThenGoesLive()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");
        for (var i = 0; i < 3; i += 1)
        {
            stream.Publish(new { i });
        }

        var got = new List<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var task = Task.Run(async () =>
        {
            await foreach (var evt in stream.Subscribe(fromOffset: 0, cancellationToken: cts.Token).WithCancellation(cts.Token))
            {
                got.Add(evt.Payload.GetProperty("i").GetInt32());
                if (got.Count == 5)
                {
                    return;
                }
            }
        }, cts.Token);

        await Task.Delay(100, cts.Token);
        stream.Publish(new { i = 3 });
        stream.Publish(new { i = 4 });

        await task;
        Assert.Equal([0, 1, 2, 3, 4], got);
    }

    [Fact]
    public async Task SubscribeFromOffsetSkipsEarlier()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");
        for (var i = 0; i < 3; i += 1)
        {
            stream.Publish(new { i });
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await foreach (var evt in stream.Subscribe(fromOffset: 2, cancellationToken: cts.Token).WithCancellation(cts.Token))
        {
            Assert.Equal(2, evt.Payload.GetProperty("i").GetInt32());
            return;
        }

        throw new InvalidOperationException("expected an event");
    }

    [Fact]
    public async Task SubscribeWithNamedConsumerResumes()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");
        for (var i = 0; i < 5; i += 1)
        {
            stream.Publish(new { i });
        }
        stream.SaveOffset("dashboard", 3);

        var got = new List<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        await foreach (var evt in stream.Subscribe(consumer: "dashboard", cancellationToken: cts.Token).WithCancellation(cts.Token))
        {
            got.Add(evt.Payload.GetProperty("i").GetInt32());
            if (got.Count == 2)
            {
                break;
            }
        }

        Assert.Equal([3, 4], got);
    }

    [Fact]
    public async Task NamedConsumerAutoSavesOffsetEveryNEvents()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("autosave");
        for (var i = 0; i < 25; i += 1)
        {
            stream.Publish(new { i });
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var got = 0;
        await foreach (var _ in stream.Subscribe(consumer: "c1", saveEveryN: 10, saveEverySeconds: 9999, cancellationToken: cts.Token).WithCancellation(cts.Token))
        {
            got += 1;
            if (got == 25)
            {
                break;
            }
        }

        Assert.Equal(20, stream.GetOffset("c1"));
    }

    [Fact]
    public async Task NamedConsumerAutoSavesOffsetEverySeconds()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("autosave-time");
        for (var i = 0; i < 3; i += 1)
        {
            stream.Publish(new { i });
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var got = 0;
        await foreach (var _ in stream.Subscribe(consumer: "c2", saveEveryN: 9999, saveEverySeconds: 0.1, cancellationToken: cts.Token).WithCancellation(cts.Token))
        {
            got += 1;
            await Task.Delay(120, cts.Token);
            if (got == 3)
            {
                break;
            }
        }

        Assert.True(stream.GetOffset("c2") >= 1);
    }

    [Fact]
    public async Task NamedConsumerAutoSaveDisabled()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("no-autosave");
        for (var i = 0; i < 50; i += 1)
        {
            stream.Publish(new { i });
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var got = 0;
        await foreach (var _ in stream.Subscribe(consumer: "c3", saveEveryN: 0, saveEverySeconds: 0.0, cancellationToken: cts.Token).WithCancellation(cts.Token))
        {
            got += 1;
            if (got == 50)
            {
                break;
            }
        }

        Assert.Equal(0, stream.GetOffset("c3"));
    }

    [Fact]
    public async Task NamedConsumerCrashReplaysFromLastSavedOffset()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("crash");
        for (var i = 0; i < 15; i += 1)
        {
            stream.Publish(new { i });
        }

        var first = new List<int>();
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
        {
            await foreach (var evt in stream.Subscribe(consumer: "crashy", saveEveryN: 5, saveEverySeconds: 9999, cancellationToken: cts.Token).WithCancellation(cts.Token))
            {
                first.Add(evt.Payload.GetProperty("i").GetInt32());
                if (first.Count == 12)
                {
                    break;
                }
            }
        }

        Assert.Equal(Enumerable.Range(0, 12).ToArray(), first);
        Assert.Equal(10, stream.GetOffset("crashy"));

        var second = new List<int>();
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
        {
            await foreach (var evt in stream.Subscribe(consumer: "crashy", saveEveryN: 5, saveEverySeconds: 9999, cancellationToken: cts.Token).WithCancellation(cts.Token))
            {
                second.Add(evt.Payload.GetProperty("i").GetInt32());
                if (second.Count == 5)
                {
                    break;
                }
            }
        }

        Assert.Equal([10, 11, 12, 13, 14], second);
    }

    [Fact]
    public void StreamInstanceIsMemoized()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        Assert.Same(db.Stream("a"), db.Stream("a"));
        Assert.NotSame(db.Stream("a"), db.Stream("b"));
    }

    [Fact]
    public async Task SubscribeNoRaceBetweenFirstReadAndListen()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");
        var iterator = stream.Subscribe(fromOffset: 0);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var task = Task.Run(async () =>
        {
            await using var enumerator = iterator.GetAsyncEnumerator(cts.Token);
            Assert.True(await enumerator.MoveNextAsync());
            return enumerator.Current.Payload.GetProperty("i").GetInt32();
        }, cts.Token);

        await Task.Delay(20, cts.Token);
        stream.Publish(new { i = 99 });

        var value = await task;
        Assert.Equal(99, value);
    }

    [Fact]
    public async Task ManyConcurrentSubscribersSameStream()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();
        var stream = db.Stream("events");

        static Task<List<int>> Collect(Stream stream, CancellationToken cancellationToken)
        {
            return Task.Run(async () =>
            {
                var got = new List<int>();
                await foreach (var evt in stream.Subscribe(fromOffset: 0, cancellationToken: cancellationToken).WithCancellation(cancellationToken))
                {
                    got.Add(evt.Payload.GetProperty("i").GetInt32());
                    if (got.Count == 5)
                    {
                        return got;
                    }
                }

                return got;
            }, cancellationToken);
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var tasks = Enumerable.Range(0, 5).Select(_ => Collect(stream, cts.Token)).ToArray();
        await Task.Delay(50, cts.Token);
        for (var i = 0; i < 5; i += 1)
        {
            stream.Publish(new { i });
        }

        var results = await Task.WhenAll(tasks);
        Assert.All(results, got => Assert.Equal([0, 1, 2, 3, 4], got));
    }

    private static long Scalar(Database db, string sql, params object?[] args)
    {
        var rows = db.Query(sql, args);
        return Convert.ToInt64(rows[0].Values.First() ?? 0L);
    }

    private static string AsString(object? payload)
    {
        return payload switch
        {
            JsonElement el when el.ValueKind == JsonValueKind.String => el.GetString() ?? "",
            JsonElement el => el.GetRawText(),
            string s => s,
            null => "null",
            _ => JsonSerializer.Serialize(payload),
        };
    }

    private static string Normalize(object? value)
    {
        return value switch
        {
            JsonElement el => el.GetRawText(),
            null => "null",
            _ => JsonSerializer.Serialize(value),
        };
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
            var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-stream-{Guid.NewGuid():N}");
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
