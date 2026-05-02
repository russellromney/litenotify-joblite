defmodule HonkerPythonInteropTest do
  use ExUnit.Case, async: false

  alias Honker.{Job, Queue, Stream}

  @repo_root Path.expand("../../..", __DIR__)

  defp python_path do
    Enum.join(
      [
        Path.join([@repo_root, "packages", "honker", "python"]),
        Path.join([@repo_root, "packages"])
      ],
      if(:os.type() |> elem(0) == :win32, do: ";", else: ":")
    )
  end

  defp find_python do
    probe = """
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
    """

    candidates =
      [
        System.get_env("HONKER_INTEROP_PYTHON"),
        Path.join([@repo_root, ".venv", "bin", "python"]),
        Path.join([@repo_root, ".venv", "Scripts", "python.exe"]),
        "python3",
        "python"
      ]
      |> Enum.reject(&is_nil/1)

    Enum.find(candidates, fn python ->
      {_, status} =
        System.cmd(python, ["-c", probe],
          env: [{"PYTHONPATH", python_path()}],
          stderr_to_stdout: true
        )

      status == 0
    end)
  end

  defp find_extension do
    [
      System.get_env("HONKER_EXTENSION_PATH"),
      System.get_env("HONKER_EXT_PATH"),
      Path.join([@repo_root, "target", "release", "libhonker_ext.dylib"]),
      Path.join([@repo_root, "target", "release", "libhonker_ext.so"])
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.find(&File.exists?/1)
  end

  setup do
    python = find_python()
    ext = find_extension()

    if (python == nil or ext == nil) and System.get_env("CI") do
      flunk("python honker binding or honker extension missing in CI")
    end

    if python == nil or ext == nil do
      {:ok, skip: true}
    else
      dir = Path.join(System.tmp_dir!(), "honker-ex-python-#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      db_path = Path.join(dir, "elixir-python.db")
      on_exit(fn -> File.rm_rf!(dir) end)
      {:ok, db} = Honker.open(db_path, extension_path: ext)
      {:ok, db: db, db_path: db_path, python: python}
    end
  end

  test "elixir and python share queue stream and notify rows", ctx do
    if Map.get(ctx, :skip) do
      :ok
    else
      for i <- 0..24 do
        {:ok, _} =
          Queue.enqueue(ctx.db, "elixir-to-python", %{
            "source" => "elixir",
            "seq" => i,
            "key" => "elixir-#{String.pad_leading(Integer.to_string(i), 2, "0")}"
          })
      end

      {:ok, _} = Honker.notify(ctx.db, "from-elixir", %{"source" => "elixir", "count" => 25})
      {:ok, _} = Stream.publish(ctx.db, "interop", %{"source" => "elixir", "kind" => "stream"})

      script = """
      import json
      import os
      import honker

      db = honker.open(os.environ["DB_PATH"])

      jobs = db.queue("elixir-to-python").claim_batch("python-worker", 50)
      payloads = [job.payload for job in jobs]
      acked = db.queue("elixir-to-python").ack_batch([job.id for job in jobs], "python-worker")
      note = db.query(
          "SELECT payload FROM _honker_notifications "
          "WHERE channel='from-elixir' ORDER BY id DESC LIMIT 1"
      )
      events = db.stream("interop")._read_since(0, 10)

      py_q = db.queue("python-to-elixir")
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
      """

      {out, status} =
        System.cmd(ctx.python, ["-c", script],
          env: [{"PYTHONPATH", python_path()}, {"DB_PATH", ctx.db_path}],
          stderr_to_stdout: true
        )

      assert status == 0, out
      observed = Jason.decode!(out)
      assert observed["acked"] == 25
      assert length(observed["payloads"]) == 25
      assert observed["note"] == %{"source" => "elixir", "count" => 25}
      assert observed["event_count"] == 1

      {:ok, py_jobs} = Queue.claim_batch(ctx.db, "python-to-elixir", "elixir-worker", 50)
      assert length(py_jobs) == 25
      assert Enum.map(py_jobs, & &1.payload["seq"]) |> Enum.sort() == Enum.to_list(0..24)
      assert Enum.all?(py_jobs, &(&1.payload["source"] == "python"))
      assert Enum.all?(py_jobs, fn job -> Job.ack(ctx.db, job) == {:ok, true} end)

      {:ok, [note]} =
        Honker.query_first(
          ctx.db.conn,
          "SELECT payload FROM _honker_notifications WHERE channel='from-python' ORDER BY id DESC LIMIT 1",
          []
        )

      assert Jason.decode!(note) == %{"source" => "python", "count" => 25}
      {:ok, events} = Stream.read_since(ctx.db, "interop", 0, 10)
      assert length(events) == 2
    end
  end
end
