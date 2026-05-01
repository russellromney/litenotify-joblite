defmodule HonkerSmokeTest do
  @moduledoc """
  End-to-end smoke tests. Covers the typed queue API plus raw-SQL
  access to streams, scheduler, locks, rate limits, and results —
  those don't have typed Elixir wrappers yet and this test ensures
  the SQL surface is reachable from a honker-ex caller today.
  """
  use ExUnit.Case, async: false

  alias Honker.{Job, Queue}
  alias Exqlite.Sqlite3

  @candidates [
    "target/release/libhonker_ext.dylib",
    "target/release/libhonker_ext.so"
  ]

  @repo_root Path.expand("../../..", __DIR__)

  defp find_extension do
    Enum.find_value(@candidates, fn rel ->
      p = Path.join(@repo_root, rel)
      if File.exists?(p), do: p, else: nil
    end)
  end

  setup do
    case find_extension() do
      nil ->
        {:skip, "honker extension not built"}

      ext ->
        dir = Path.join(System.tmp_dir!(), "honker-smoke-#{System.unique_integer([:positive])}")
        File.mkdir_p!(dir)
        db_path = Path.join(dir, "t.db")
        on_exit(fn -> File.rm_rf!(dir) end)

        {:ok, db} = Honker.open(db_path, extension_path: ext)
        {:ok, %{db: db}}
    end
  end

  # Helper: run an arbitrary SQL with params and return the first row
  # as a list. Throws on error to keep tests concise.
  defp sql!(db, sql, params \\ []) do
    case Honker.query_first(db.conn, sql, params) do
      {:ok, row} -> row
      {:error, reason} -> flunk("SQL failed: #{inspect(reason)} for #{sql}")
    end
  end

  test "claim_batch returns multiple jobs and ack clears the queue", %{db: db} do
    for i <- 0..4 do
      {:ok, _} = Queue.enqueue(db, "bulk", %{"i" => i})
    end

    {:ok, jobs} = Queue.claim_batch(db, "bulk", "w", 10)
    assert length(jobs) == 5
    sorted = jobs |> Enum.map(& &1.payload["i"]) |> Enum.sort()
    assert sorted == [0, 1, 2, 3, 4]

    Enum.each(jobs, fn j -> {:ok, _} = Job.ack(db, j) end)
    assert {:ok, nil} = Queue.claim_one(db, "bulk", "w")
  end

  test "heartbeat extends visibility on a fresh claim", %{db: db} do
    db = Honker.configure_queue(db, "hb", visibility_timeout_s: 1)
    {:ok, _} = Queue.enqueue(db, "hb", %{})
    {:ok, job} = Queue.claim_one(db, "hb", "w")
    assert {:ok, true} = Job.heartbeat(db, job, 60)
    assert {:ok, true} = Job.ack(db, job)
  end

  test "fail moves job to dead immediately", %{db: db} do
    {:ok, _} = Queue.enqueue(db, "failing", %{"x" => 1})
    {:ok, job} = Queue.claim_one(db, "failing", "w")
    assert {:ok, true} = Job.fail(db, job, "nope")

    [n] = sql!(db, "SELECT COUNT(*) FROM _honker_dead WHERE queue='failing'")
    assert n == 1
  end

  test "atomic business-write + enqueue commits/rolls back together", %{db: db} do
    :ok = Sqlite3.execute(db.conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")

    # Commit path.
    {:ok, _} = Honker.query_first(db.conn, "SELECT 1", [])  # warm prepare cache
    :ok = Sqlite3.execute(db.conn, "BEGIN IMMEDIATE")
    {:ok, _} =
      Honker.query_first(db.conn, "INSERT INTO orders (id, total) VALUES (?1, ?2)", [1, 100])

    {:ok, _} =
      Honker.query_first(
        db.conn,
        "SELECT honker_enqueue(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        ["emails", Jason.encode!(%{"order_id" => 1}), nil, nil, 0, 3, nil]
      )

    :ok = Sqlite3.execute(db.conn, "COMMIT")

    [orders] = sql!(db, "SELECT COUNT(*) FROM orders")
    [live] = sql!(db, "SELECT COUNT(*) FROM _honker_live")
    assert orders == 1
    assert live == 1

    # Rollback path — neither row lands.
    :ok = Sqlite3.execute(db.conn, "BEGIN IMMEDIATE")
    {:ok, _} =
      Honker.query_first(db.conn, "INSERT INTO orders (id, total) VALUES (?1, ?2)", [2, 200])

    {:ok, _} =
      Honker.query_first(
        db.conn,
        "SELECT honker_enqueue(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        ["emails", Jason.encode!(%{"order_id" => 2}), nil, nil, 0, 3, nil]
      )

    :ok = Sqlite3.execute(db.conn, "ROLLBACK")

    [orders_after] = sql!(db, "SELECT COUNT(*) FROM orders")
    [live_after] = sql!(db, "SELECT COUNT(*) FROM _honker_live")
    assert orders_after == 1
    assert live_after == 1
  end

  test "stream publish and read via raw SQL", %{db: db} do
    [off1] = sql!(db, "SELECT honker_stream_publish(?1, ?2, ?3)",
             ["orders", nil, Jason.encode!(%{"id" => 1})])
    [off2] = sql!(db, "SELECT honker_stream_publish(?1, ?2, ?3)",
             ["orders", nil, Jason.encode!(%{"id" => 2})])
    assert off2 > off1

    [rows_json] = sql!(db, "SELECT honker_stream_read_since(?1, ?2, ?3)",
                  ["orders", 0, 100])
    events = Jason.decode!(rows_json)
    assert length(events) == 2
  end

  test "scheduler register + soonest", %{db: db} do
    :ok = Sqlite3.execute(
      db.conn,
      "SELECT honker_scheduler_register('hourly', 'health', '0 * * * *', '{}', 0, NULL)"
    )
    [soonest] = sql!(db, "SELECT honker_scheduler_soonest()")
    assert soonest > 0
  end

  test "advisory lock mutual exclusion", %{db: db} do
    [got] = sql!(db, "SELECT honker_lock_acquire(?1, ?2, ?3)", ["key", "a", 60])
    assert got == 1

    [blocked] = sql!(db, "SELECT honker_lock_acquire(?1, ?2, ?3)", ["key", "b", 60])
    assert blocked == 0

    sql!(db, "SELECT honker_lock_release(?1, ?2)", ["key", "a"])
    [regot] = sql!(db, "SELECT honker_lock_acquire(?1, ?2, ?3)", ["key", "b", 60])
    assert regot == 1
  end

  test "rate limit allows N then blocks", %{db: db} do
    for _ <- 1..3 do
      [ok] = sql!(db, "SELECT honker_rate_limit_try(?1, ?2, ?3)", ["api", 3, 60])
      assert ok == 1
    end

    [blocked] = sql!(db, "SELECT honker_rate_limit_try(?1, ?2, ?3)", ["api", 3, 60])
    assert blocked == 0
  end

  test "results save and get", %{db: db} do
    :ok = Sqlite3.execute(
      db.conn,
      "SELECT honker_result_save(42, '{\"ok\":true}', 3600)"
    )
    [v] = sql!(db, "SELECT honker_result_get(?1)", [42])
    assert Jason.decode!(v) == %{"ok" => true}

    [missing] = sql!(db, "SELECT honker_result_get(?1)", [999])
    assert missing == nil
  end
end
