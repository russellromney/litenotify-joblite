defmodule HonkerParityTest do
  @moduledoc """
  Parity tests for the typed wrappers added to honker-ex:
  `Honker.Transaction`, `Honker.Stream`, `Honker.Scheduler`,
  `Honker.Lock`, results, rate limits, `notify_tx`, `prune_notifications`,
  and `Honker.Queue.enqueue_tx` / `sweep_expired`.

  Existing `honker_test.exs` and `smoke_test.exs` remain the "does
  SQL reach the extension" baseline; these tests target the Elixir
  surface we just added.
  """
  use ExUnit.Case, async: false

  alias Honker.{Job, Lock, Queue, Scheduler, Stream, StreamEvent, Transaction}

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
        dir = Path.join(System.tmp_dir!(), "honker-parity-#{System.unique_integer([:positive])}")
        File.mkdir_p!(dir)
        db_path = Path.join(dir, "t.db")
        on_exit(fn -> File.rm_rf!(dir) end)

        {:ok, db} = Honker.open(db_path, extension_path: ext)
        {:ok, %{db: db}}
    end
  end

  # ---------------------------------------------------------------
  # Transactions
  # ---------------------------------------------------------------

  test "Transaction.transaction commits on success", %{db: db} do
    :ok = Exqlite.Sqlite3.execute(db.conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")

    result =
      Transaction.transaction(db, fn tx ->
        :ok = Transaction.execute(tx, "INSERT INTO orders (id, total) VALUES (?1, ?2)", [1, 100])
        {:ok, _} = Queue.enqueue_tx(tx, "emails", %{"order_id" => 1}, [], [])
        :committed
      end)

    assert result == :committed

    {:ok, [n_orders]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM orders", [])
    {:ok, [n_live]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM _honker_live", [])
    assert n_orders == 1
    assert n_live == 1
  end

  test "Transaction.transaction rolls back on raised exception", %{db: db} do
    :ok = Exqlite.Sqlite3.execute(db.conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")

    assert_raise RuntimeError, "boom", fn ->
      Transaction.transaction(db, fn tx ->
        :ok = Transaction.execute(tx, "INSERT INTO orders (id, total) VALUES (?1, ?2)", [9, 900])
        {:ok, _} = Queue.enqueue_tx(tx, "emails", %{"order_id" => 9}, [], [])
        raise "boom"
      end)
    end

    {:ok, [n_orders]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM orders", [])
    {:ok, [n_live]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM _honker_live", [])
    assert n_orders == 0
    assert n_live == 0
  end

  test "Transaction.begin + explicit rollback drops both sides", %{db: db} do
    :ok = Exqlite.Sqlite3.execute(db.conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")

    {:ok, tx} = Transaction.begin(db)
    :ok = Transaction.execute(tx, "INSERT INTO orders (id, total) VALUES (?1, ?2)", [3, 30])
    {:ok, _} = Queue.enqueue_tx(tx, "emails", %{"order_id" => 3}, [], [])
    :ok = Transaction.rollback(tx)

    {:ok, [n_orders]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM orders", [])
    {:ok, [n_live]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM _honker_live", [])
    assert n_orders == 0
    assert n_live == 0
  end

  test "Transaction.transaction propagates {:error, _} by rolling back", %{db: db} do
    :ok = Exqlite.Sqlite3.execute(db.conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")

    result =
      Transaction.transaction(db, fn tx ->
        :ok = Transaction.execute(tx, "INSERT INTO orders (id, total) VALUES (?1, ?2)", [7, 77])
        {:error, :nope}
      end)

    assert result == {:error, :nope}
    {:ok, [n]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM orders", [])
    assert n == 0
  end

  # ---------------------------------------------------------------
  # Streams
  # ---------------------------------------------------------------

  test "Stream publish / read_since / consumer offset round trip", %{db: db} do
    {:ok, off1} = Stream.publish(db, "orders", %{"id" => 1})
    {:ok, off2} = Stream.publish(db, "orders", %{"id" => 2}, key: "cust-7")
    assert off2 > off1

    {:ok, events} = Stream.read_since(db, "orders", 0, 100)
    assert length(events) == 2
    assert Enum.all?(events, &match?(%StreamEvent{}, &1))

    first = Enum.find(events, &(&1.offset == off1))
    second = Enum.find(events, &(&1.offset == off2))
    assert first.payload == %{"id" => 1}
    assert first.key == nil
    assert second.payload == %{"id" => 2}
    assert second.key == "cust-7"

    # No offset saved yet -> reads from 0.
    {:ok, 0} = Stream.get_offset(db, "billing", "orders")
    {:ok, unread} = Stream.read_from_consumer(db, "orders", "billing", 100)
    assert length(unread) == 2

    {:ok, true} = Stream.save_offset(db, "billing", "orders", off2)
    {:ok, ^off2} = Stream.get_offset(db, "billing", "orders")
    {:ok, []} = Stream.read_from_consumer(db, "orders", "billing", 100)
  end

  test "Stream save_offset is monotonic — lower offset is ignored", %{db: db} do
    {:ok, off1} = Stream.publish(db, "t", %{"n" => 1})
    {:ok, off2} = Stream.publish(db, "t", %{"n" => 2})

    {:ok, true} = Stream.save_offset(db, "c", "t", off2)
    # Lower offset -> false, stored offset stays at off2.
    {:ok, false} = Stream.save_offset(db, "c", "t", off1)
    {:ok, ^off2} = Stream.get_offset(db, "c", "t")
  end

  test "Stream.save_offset_tx rollback does not advance offset", %{db: db} do
    {:ok, _} = Stream.publish(db, "t", %{"n" => 1})
    {:ok, off} = Stream.publish(db, "t", %{"n" => 2})

    # Before any save, consumer offset is 0.
    {:ok, 0} = Stream.get_offset(db, "c", "t")

    # Save inside a transaction, then roll back — offset must stay 0.
    {:ok, tx} = Transaction.begin(db)
    {:ok, true} = Stream.save_offset_tx(tx, "c", "t", off)
    :ok = Transaction.rollback(tx)

    {:ok, after_rb} = Stream.get_offset(db, "c", "t")
    assert after_rb == 0

    # Commit path advances.
    {:ok, tx2} = Transaction.begin(db)
    {:ok, true} = Stream.save_offset_tx(tx2, "c", "t", off)
    :ok = Transaction.commit(tx2)
    {:ok, ^off} = Stream.get_offset(db, "c", "t")
  end

  test "Stream.publish_tx rollback does not persist event", %{db: db} do
    {:ok, tx} = Transaction.begin(db)
    {:ok, _off} = Stream.publish_tx(tx, "t-rb", %{"n" => 1})
    :ok = Transaction.rollback(tx)

    {:ok, events} = Stream.read_since(db, "t-rb", 0, 100)
    assert events == []
  end

  # ---------------------------------------------------------------
  # Scheduler
  # ---------------------------------------------------------------

  test "Scheduler.add / remove / soonest / tick", %{db: db} do
    :ok =
      Scheduler.add(db,
        name: "hourly",
        queue: "health",
        schedule: "0 * * * *",
        payload: %{}
      )

    {:ok, soonest} = Scheduler.soonest(db)
    assert soonest > 0

    # Tick with current time should not fire (cron is "0 * * * *").
    {:ok, fires} = Scheduler.tick(db)
    assert is_list(fires)

    {:ok, 1} = Scheduler.remove(db, "hourly")
    {:ok, 0} = Scheduler.soonest(db)
  end

  test "Scheduler.run stops when stop_fun returns true", %{db: db} do
    :ok =
      Scheduler.add(db,
        name: "every-minute",
        queue: "h",
        schedule: "* * * * *",
        payload: %{}
      )

    ref = :atomics.new(1, signed: false)
    :atomics.put(ref, 1, 0)

    parent = self()

    task =
      Task.async(fn ->
        result =
          Scheduler.run(db, "parity-worker", fn ->
            :atomics.get(ref, 1) == 1
          end)

        send(parent, {:scheduler_done, result})
        result
      end)

    # Let the loop acquire the lock and run at least one tick.
    Process.sleep(300)

    # Verify we're the leader.
    {:ok, [owner]} =
      Honker.query_first(
        db.conn,
        "SELECT owner FROM _honker_locks WHERE name = ?1",
        ["honker-scheduler"]
      )

    assert owner == "parity-worker"

    # Ask the loop to stop.
    :atomics.put(ref, 1, 1)

    assert :ok = Task.await(task, 5_000)
    assert_received {:scheduler_done, :ok}

    # Lock should be released.
    {:ok, row} =
      Honker.query_first(
        db.conn,
        "SELECT COUNT(*) FROM _honker_locks WHERE name = ?1",
        ["honker-scheduler"]
      )

    [n] = row
    assert n == 0
  end

  test "Scheduler accepts legacy cron alias", %{db: db} do
    :ok =
      Scheduler.add(db,
        name: "legacy",
        queue: "health",
        cron: "@every 1s",
        payload: %{}
      )

    {:ok, soonest} = Scheduler.soonest(db)
    assert soonest > 0
    {:ok, 1} = Scheduler.remove(db, "legacy")
  end

  test "Scheduler accepts every-second schedule", %{db: db} do
    :ok =
      Scheduler.add(db,
        name: "fast",
        queue: "health",
        schedule: "@every 1s",
        payload: %{}
      )

    {:ok, soonest} = Scheduler.soonest(db)
    assert soonest > 0
    {:ok, 1} = Scheduler.remove(db, "fast")
  end

  test "Scheduler.run fires every-second schedule", %{db: db} do
    :ok =
      Scheduler.add(db,
        name: "fast-run",
        queue: "fast-run",
        schedule: "@every 1s",
        payload: %{ok: true}
      )

    stop = :atomics.new(1, signed: false)
    :atomics.put(stop, 1, 0)

    task =
      Task.async(fn ->
        Scheduler.run(db, "parity-fast", fn ->
          :atomics.get(stop, 1) == 1
        end)
      end)

    job =
      wait_for_job(fn ->
        Queue.claim_one(db, "fast-run", "worker-fast")
      end)

    assert %Job{} = job
    assert job.payload == %{"ok" => true}
    assert {:ok, true} = Job.ack(db, job)

    :atomics.put(stop, 1, 1)
    assert :ok = Task.await(task, 5_000)
  end

  test "Scheduler.run wakes when a new schedule is registered", %{db: db} do
    stop = :atomics.new(1, signed: false)
    :atomics.put(stop, 1, 0)

    task =
      Task.async(fn ->
        Scheduler.run(db, "parity-wake", fn ->
          :atomics.get(stop, 1) == 1
        end)
      end)

    Process.sleep(200)

    :ok =
      Scheduler.add(db,
        name: "wake-fast",
        queue: "wake-fast",
        schedule: "@every 1s",
        payload: %{wake: true}
      )

    job =
      wait_for_job(fn ->
        Queue.claim_one(db, "wake-fast", "worker-wake")
      end)

    assert %Job{} = job
    assert job.payload == %{"wake" => true}
    assert {:ok, true} = Job.ack(db, job)

    :atomics.put(stop, 1, 1)
    assert :ok = Task.await(task, 5_000)
  end

  # ---------------------------------------------------------------
  # Locks
  # ---------------------------------------------------------------

  test "Lock mutual exclusion, release, and re-acquire", %{db: db} do
    {:ok, %Lock{} = a} = Lock.try_acquire(db, "gate", "alice", 60)

    # Second try by a different owner is blocked.
    assert {:ok, nil} = Lock.try_acquire(db, "gate", "bob", 60)

    # Alice releases.
    assert {:ok, true} = Lock.release(a, db)

    # Bob can now grab it.
    {:ok, %Lock{} = b} = Lock.try_acquire(db, "gate", "bob", 60)
    assert {:ok, true} = Lock.release(b, db)
  end

  defp wait_for_job(fun, timeout_ms \\ 3_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_job(fun, deadline)
  end

  defp do_wait_for_job(fun, deadline) do
    case fun.() do
      {:ok, %Job{} = job} ->
        job

      {:ok, nil} ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("expected job before timeout")
        else
          Process.sleep(50)
          do_wait_for_job(fun, deadline)
        end

      other ->
        flunk("unexpected result while waiting for job: #{inspect(other)}")
    end
  end

  test "Lock heartbeat extends TTL", %{db: db} do
    {:ok, %Lock{} = lock} = Lock.try_acquire(db, "worker", "me", 1)

    assert {:ok, true} = Lock.heartbeat(lock, db, 60)

    # Read back expires_at to confirm it moved forward of "now".
    {:ok, [exp]} =
      Honker.query_first(
        db.conn,
        "SELECT expires_at FROM _honker_locks WHERE name = ?1",
        ["worker"]
      )

    now = System.system_time(:second)
    assert exp > now + 30

    assert {:ok, true} = Lock.release(lock, db)
  end

  test "Lock.heartbeat returns false when lock was stolen", %{db: db} do
    {:ok, %Lock{} = lock} = Lock.try_acquire(db, "stolen", "me", 1)

    # Force-expire: stomp expires_at to a past value, then another owner
    # acquires. This mirrors the TTL-expiry + steal scenario.
    :ok =
      Exqlite.Sqlite3.execute(
        db.conn,
        "UPDATE _honker_locks SET expires_at = 0 WHERE name = 'stolen'"
      )

    {:ok, %Lock{}} = Lock.try_acquire(db, "stolen", "thief", 60)

    assert {:ok, false} = Lock.heartbeat(lock, db, 60)
  end

  # ---------------------------------------------------------------
  # Rate limits
  # ---------------------------------------------------------------

  test "try_rate_limit allows limit then blocks", %{db: db} do
    for _ <- 1..3 do
      assert {:ok, true} = Honker.try_rate_limit(db, "api", 3, 60)
    end

    assert {:ok, false} = Honker.try_rate_limit(db, "api", 3, 60)
  end

  # ---------------------------------------------------------------
  # Results
  # ---------------------------------------------------------------

  test "save_result / get_result / missing returns nil", %{db: db} do
    value = Jason.encode!(%{"ok" => true, "n" => 7})
    assert :ok = Honker.save_result(db, 42, value, 3600)

    {:ok, got} = Honker.get_result(db, 42)
    assert Jason.decode!(got) == %{"ok" => true, "n" => 7}

    assert {:ok, nil} = Honker.get_result(db, 999)
  end

  test "sweep_results removes expired rows", %{db: db} do
    :ok = Honker.save_result(db, 1, "\"keep\"", 3600)
    :ok = Honker.save_result(db, 2, "\"gone\"", 60)

    # Backdate job 2's expiry into the past. (ttl_s = 0 in the core
    # means "never expires", so we can't rely on it to produce a
    # sweep-eligible row.)
    :ok =
      Exqlite.Sqlite3.execute(
        db.conn,
        "UPDATE _honker_results SET expires_at = 1 WHERE job_id = 2"
      )

    {:ok, deleted} = Honker.sweep_results(db)
    assert deleted >= 1

    assert {:ok, nil} = Honker.get_result(db, 2)
    {:ok, kept} = Honker.get_result(db, 1)
    assert Jason.decode!(kept) == "keep"
  end

  # ---------------------------------------------------------------
  # Notifications + notify_tx
  # ---------------------------------------------------------------

  test "notify_tx is invisible until commit", %{db: db} do
    {:ok, tx} = Transaction.begin(db)
    {:ok, id} = Honker.notify_tx(tx, "orders", %{"id" => 1})
    assert id > 0
    :ok = Transaction.rollback(tx)

    {:ok, [n]} =
      Honker.query_first(
        db.conn,
        "SELECT COUNT(*) FROM _honker_notifications WHERE channel = ?1",
        ["orders"]
      )

    assert n == 0

    {:ok, tx2} = Transaction.begin(db)
    {:ok, _id} = Honker.notify_tx(tx2, "orders", %{"id" => 2})
    :ok = Transaction.commit(tx2)

    {:ok, [after_commit]} =
      Honker.query_first(
        db.conn,
        "SELECT COUNT(*) FROM _honker_notifications WHERE channel = ?1",
        ["orders"]
      )

    assert after_commit == 1
  end

  test "prune_notifications deletes older rows", %{db: db} do
    {:ok, _} = Honker.notify(db, "c", %{"i" => 1})
    {:ok, _} = Honker.notify(db, "c", %{"i" => 2})

    # Force a backdated row.
    :ok =
      Exqlite.Sqlite3.execute(
        db.conn,
        "UPDATE _honker_notifications SET created_at = 0 WHERE rowid = 1"
      )

    {:ok, deleted} = Honker.prune_notifications(db, 60)
    assert deleted >= 1

    {:ok, [n]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM _honker_notifications", [])
    assert n == 1
  end

  # ---------------------------------------------------------------
  # Queue.enqueue_tx / sweep_expired
  # ---------------------------------------------------------------

  test "Queue.enqueue_tx uses provided queue_opts for max_attempts", %{db: db} do
    {:ok, tx} = Transaction.begin(db)
    {:ok, id} = Queue.enqueue_tx(tx, "sized", %{"x" => 1}, [], max_attempts: 7)
    :ok = Transaction.commit(tx)

    {:ok, [max_attempts]} =
      Honker.query_first(
        db.conn,
        "SELECT max_attempts FROM _honker_live WHERE id = ?1",
        [id]
      )

    assert max_attempts == 7
  end

  test "Queue.sweep_expired moves pending rows past TTL to dead", %{db: db} do
    # expires: 0 means TTL is "now", already expired.
    {:ok, id} = Queue.enqueue(db, "swept", %{"x" => 1}, expires: 0)

    # The row enqueued with expires=0 has expires_at=unixepoch()+0, so
    # it's already at the boundary. Nudge it back one second to be safe.
    :ok =
      Exqlite.Sqlite3.execute(
        db.conn,
        "UPDATE _honker_live SET expires_at = 1 WHERE id = #{id}"
      )

    {:ok, count} = Queue.sweep_expired(db, "swept")
    assert count == 1

    # Row moved to _honker_dead with last_error = 'expired'.
    {:ok, [in_live]} =
      Honker.query_first(
        db.conn,
        "SELECT COUNT(*) FROM _honker_live WHERE id = ?1",
        [id]
      )

    {:ok, [in_dead]} =
      Honker.query_first(
        db.conn,
        "SELECT last_error FROM _honker_dead WHERE id = ?1",
        [id]
      )

    assert in_live == 0
    assert in_dead == "expired"

    # And no one can claim it from the live queue anymore.
    assert {:ok, nil} = Queue.claim_one(db, "swept", "w")

    # Suppress unused alias warning.
    _ = %Job{}
  end
end
