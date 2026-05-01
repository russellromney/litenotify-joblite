using System.Runtime.InteropServices;

namespace Honker.Tests;

public sealed class RateLimitTests
{
    [Fact]
    public void RateLimitAllowsUnderLimit()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        for (var i = 0; i < 5; i += 1)
        {
            Assert.True(db.TryRateLimit("api", 5, 60));
        }
    }

    [Fact]
    public void RateLimitRejectsOverLimit()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        for (var i = 0; i < 3; i += 1)
        {
            Assert.True(db.TryRateLimit("api", 3, 60));
        }

        Assert.False(db.TryRateLimit("api", 3, 60));
        Assert.False(db.TryRateLimit("api", 3, 60));
    }

    [Fact]
    public void RateLimitRejectedCallIsNotCounted()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        for (var i = 0; i < 3; i += 1)
        {
            db.TryRateLimit("api", 3, 60);
        }

        for (var i = 0; i < 100; i += 1)
        {
            Assert.False(db.TryRateLimit("api", 3, 60));
        }

        var rows = db.Query("SELECT count FROM _honker_rate_limits WHERE name='api'");
        Assert.Single(rows);
        Assert.Equal(3L, rows[0]["count"]);
    }

    [Fact]
    public void RateLimitInvalidArgsRaise()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        Assert.Throws<ArgumentOutOfRangeException>(() => db.TryRateLimit("x", 0, 60));
        Assert.Throws<ArgumentOutOfRangeException>(() => db.TryRateLimit("x", 5, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => db.SweepRateLimits(0));
    }

    [Fact]
    public void SweepRateLimitsRemovesOldWindows()
    {
        using var harness = TestHarness.Create();
        using var db = harness.Open();

        using (var tx = db.BeginTransaction())
        {
            tx.Execute(
                "INSERT INTO _honker_rate_limits (name, window_start, count) VALUES ('old', unixepoch() - 100000, 5)"
            );
            tx.Commit();
        }

        Assert.True(db.TryRateLimit("fresh", 10, 60));
        Assert.Equal(1, db.SweepRateLimits(3600));

        var remaining = db.Query("SELECT name FROM _honker_rate_limits ORDER BY name");
        Assert.Equal(["fresh"], remaining.Select(row => Convert.ToString(row["name"])!).ToArray());
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
            var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-rate-{Guid.NewGuid():N}");
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
