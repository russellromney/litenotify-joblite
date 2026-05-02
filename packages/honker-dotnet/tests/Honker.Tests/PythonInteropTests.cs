using System.Diagnostics;
using System.Text.Json;

namespace Honker.Tests;

public sealed class PythonInteropTests
{
    [Fact]
    public void DotnetAndPythonShareQueueStreamAndNotifyRows()
    {
        var python = FindPython();
        var extension = FindExtension();
        if (python is null || extension is null)
        {
            if (Environment.GetEnvironmentVariable("HONKER_REQUIRE_INTEROP") == "1")
            {
                throw new InvalidOperationException("python honker binding or honker extension missing");
            }

            return;
        }

        var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-python-{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        try
        {
            var dbPath = Path.Combine(dir, "dotnet-python.db");
            using var db = Database.Open(dbPath, new OpenOptions { ExtensionPath = extension });

            var dotnetQueue = db.Queue("dotnet-to-python");
            for (var i = 0; i < 25; i += 1)
            {
                dotnetQueue.Enqueue(new { source = "dotnet", seq = i, key = $"dotnet-{i:00}" });
            }

            db.Notify("from-dotnet", new { source = "dotnet", count = 25 });
            db.Stream("interop").Publish(new { source = "dotnet", kind = "stream" });

            var observed = RunPython(python, dbPath, """
import json
import os
import honker

db = honker.open(os.environ["DB_PATH"])

jobs = db.queue("dotnet-to-python").claim_batch("python-worker", 50)
payloads = [job.payload for job in jobs]
acked = db.queue("dotnet-to-python").ack_batch([job.id for job in jobs], "python-worker")
note = db.query(
    "SELECT payload FROM _honker_notifications "
    "WHERE channel='from-dotnet' ORDER BY id DESC LIMIT 1"
)
events = db.stream("interop")._read_since(0, 10)

py_q = db.queue("python-to-dotnet")
for i in range(25):
    py_q.enqueue({"source": "python", "seq": i, "key": f"py-{i:02d}"})
with db.transaction() as tx:
    tx.notify("from-python", {"source": "python", "count": len(jobs)})
db.stream("interop").publish({"source": "python", "kind": "stream"})

print(json.dumps({
    "acked": acked,
    "payloads": payloads,
    "note": json.loads(note[0]["payload"]),
    "event_count": len(events),
}))
""");

            using var doc = JsonDocument.Parse(observed);
            var root = doc.RootElement;
            Assert.Equal(25, root.GetProperty("acked").GetInt32());
            Assert.Equal(25, root.GetProperty("payloads").GetArrayLength());
            Assert.Equal("dotnet", root.GetProperty("note").GetProperty("source").GetString());
            Assert.Equal(25, root.GetProperty("note").GetProperty("count").GetInt32());
            Assert.Equal(1, root.GetProperty("event_count").GetInt32());

            var pyQueue = db.Queue("python-to-dotnet");
            var pyJobs = pyQueue.ClaimBatch("dotnet-worker", 50);
            Assert.Equal(25, pyJobs.Count);
            Assert.Equal(
                Enumerable.Range(0, 25).ToArray(),
                pyJobs.Select(job =>
                {
                    Assert.Equal("python", job.Payload.GetProperty("source").GetString());
                    return job.Payload.GetProperty("seq").GetInt32();
                }).Order().ToArray()
            );
            Assert.Equal(25, pyQueue.AckBatch(pyJobs.Select(job => job.Id), "dotnet-worker"));

            var note = Convert.ToString(db.Query(
                "SELECT payload FROM _honker_notifications WHERE channel='from-python' ORDER BY id DESC LIMIT 1"
            )[0]["payload"]);
            using var noteDoc = JsonDocument.Parse(note!);
            Assert.Equal("python", noteDoc.RootElement.GetProperty("source").GetString());
            Assert.Equal(25, noteDoc.RootElement.GetProperty("count").GetInt32());
            Assert.Equal(2, db.Stream("interop").ReadSince(0, 10).Count);
        }
        finally
        {
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch
            {
                // best effort cleanup
            }
        }
    }

    private static string? FindPython()
    {
        var root = FindRepoRoot();
        var candidates = new[]
        {
            Environment.GetEnvironmentVariable("HONKER_INTEROP_PYTHON"),
            Path.Combine(root, ".venv", OperatingSystem.IsWindows() ? "Scripts/python.exe" : "bin/python"),
            "python3",
            "python",
        }.Where(p => !string.IsNullOrWhiteSpace(p));

        foreach (var python in candidates)
        {
            var result = RunProcess(python!, ["-c", PythonProbeScript], new Dictionary<string, string?>
            {
                ["PYTHONPATH"] = PythonPath(root),
            });
            if (result.ExitCode == 0)
            {
                return python;
            }
        }

        return null;
    }

    private const string PythonProbeScript = """
import os
import tempfile
import honker

p = tempfile.mktemp(prefix="honker-probe-", suffix=".db")
db = honker.open(p)
db.query("SELECT 1")
db = None
try:
    os.remove(p)
except OSError:
    pass
""";

    private static string RunPython(string python, string dbPath, string script)
    {
        var root = FindRepoRoot();
        var result = RunProcess(python, ["-c", script], new Dictionary<string, string?>
        {
            ["PYTHONPATH"] = PythonPath(root),
            ["DB_PATH"] = dbPath,
        });
        Assert.Equal(0, result.ExitCode);
        return result.Stdout;
    }

    private static (int ExitCode, string Stdout, string Stderr) RunProcess(
        string fileName,
        IReadOnlyList<string> arguments,
        IReadOnlyDictionary<string, string?> env)
    {
        using var proc = new Process();
        proc.StartInfo.FileName = fileName;
        foreach (var argument in arguments)
        {
            proc.StartInfo.ArgumentList.Add(argument);
        }
        proc.StartInfo.RedirectStandardOutput = true;
        proc.StartInfo.RedirectStandardError = true;
        foreach (var (key, value) in env)
        {
            proc.StartInfo.Environment[key] = value;
        }

        try
        {
            proc.Start();
        }
        catch
        {
            return (127, "", "");
        }

        var stdout = proc.StandardOutput.ReadToEnd();
        var stderr = proc.StandardError.ReadToEnd();
        proc.WaitForExit();
        return (proc.ExitCode, stdout, stderr);
    }

    private static string PythonPath(string root)
    {
        return string.Join(
            Path.PathSeparator,
            Path.Combine(root, "packages", "honker", "python"),
            Path.Combine(root, "packages")
        );
    }

    private static string? FindExtension()
    {
        var root = FindRepoRoot();
        return new[]
        {
            Environment.GetEnvironmentVariable("HONKER_EXTENSION_PATH"),
            Environment.GetEnvironmentVariable("HONKER_EXT_PATH"),
            Path.Combine(root, "target", "release", OperatingSystem.IsWindows() ? "honker_ext.dll" : "libhonker_ext.dylib"),
            Path.Combine(root, "target", "release", "libhonker_ext.so"),
        }.FirstOrDefault(path => !string.IsNullOrWhiteSpace(path) && File.Exists(path));
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

            current = Directory.GetParent(current)?.FullName ?? "";
        }

        throw new InvalidOperationException("could not find repo root");
    }
}
