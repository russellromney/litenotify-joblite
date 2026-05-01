defmodule HonkerTest do
  use ExUnit.Case, async: false

  @candidates [
    "target/release/libhonker_ext.dylib",
    "target/release/libhonker_ext.so",
    "target/release/libhonker_extension.dylib",
    "target/release/libhonker_extension.so"
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
        {:skip, "honker extension not built — run cargo build -p honker-extension --release"}

      ext ->
        dir = Path.join(System.tmp_dir!(), "honker-ex-#{System.unique_integer([:positive])}")
        File.mkdir_p!(dir)
        db_path = Path.join(dir, "t.db")
        on_exit(fn -> File.rm_rf!(dir) end)
        {:ok, %{ext: ext, db_path: db_path}}
    end
  end

  test "enqueue / claim / ack round-trips", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    {:ok, id} = Honker.Queue.enqueue(db, "emails", %{"to" => "alice@example.com"})
    assert id > 0

    {:ok, job} = Honker.Queue.claim_one(db, "emails", "worker-1")
    assert job.id == id
    assert job.attempts == 1
    assert job.payload["to"] == "alice@example.com"

    assert {:ok, true} = Honker.Job.ack(db, job)
    assert {:ok, nil} = Honker.Queue.claim_one(db, "emails", "worker-1")
  end

  test "retry-to-dead after max_attempts", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    db = Honker.configure_queue(db, "retries", max_attempts: 2)

    {:ok, _} = Honker.Queue.enqueue(db, "retries", %{"i" => 1})

    {:ok, job1} = Honker.Queue.claim_one(db, "retries", "w")
    assert {:ok, true} = Honker.Job.retry(db, job1, 0, "first")

    {:ok, job2} = Honker.Queue.claim_one(db, "retries", "w")
    assert job2.attempts == 2
    assert {:ok, true} = Honker.Job.retry(db, job2, 0, "second")

    assert {:ok, nil} = Honker.Queue.claim_one(db, "retries", "w")

    {:ok, [count]} =
      Honker.query_first(
        db.conn,
        "SELECT COUNT(*) FROM _honker_dead WHERE queue='retries'",
        []
      )

    assert count == 1
  end

  test "notify inserts into _honker_notifications", ctx do
    {:ok, db} = Honker.open(ctx.db_path, extension_path: ctx.ext)
    {:ok, id} = Honker.notify(db, "orders", %{"id" => 42})
    assert id > 0

    {:ok, [channel, payload]} =
      Honker.query_first(
        db.conn,
        "SELECT channel, payload FROM _honker_notifications WHERE id = ?1",
        [id]
      )

    assert channel == "orders"
    assert Jason.decode!(payload) == %{"id" => 42}
  end
end
