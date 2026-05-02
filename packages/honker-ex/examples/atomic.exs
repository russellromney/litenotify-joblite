# Atomic business-write + enqueue in one transaction.
#
#   mix run examples/atomic.exs

ext_path =
  System.get_env("HONKER_EXTENSION_PATH") ||
    Path.expand("../../../target/release/libhonker_ext.dylib", __DIR__)

dir = Path.join(System.tmp_dir!(), "honker-#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)

try do
  {:ok, db} = Honker.open(Path.join(dir, "app.db"), extension_path: ext_path)

  {:ok, _} = Honker.query_first(db.conn,
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)", [])

  count = fn sql ->
    {:ok, [n]} = Honker.query_first(db.conn, sql, [])
    n
  end

  # Success path.
  {:ok, _} = Honker.query_first(db.conn, "BEGIN IMMEDIATE", [])
  {:ok, _} = Honker.query_first(db.conn,
    "INSERT INTO orders (user_id, total) VALUES (?1, ?2)", [42, 9900])
  {:ok, _} = Honker.query_first(db.conn,
    "SELECT honker_enqueue(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    ["emails",
     Jason.encode!(%{"to" => "alice@example.com", "order_id" => 42}),
     nil, nil, 0, 3, nil])
  {:ok, _} = Honker.query_first(db.conn, "COMMIT", [])

  IO.puts(
    "committed: #{count.("SELECT COUNT(*) FROM orders")} order(s), " <>
    "#{count.("SELECT COUNT(*) FROM _honker_live WHERE queue='emails'")} job(s)"
  )

  # Rollback path.
  {:ok, _} = Honker.query_first(db.conn, "BEGIN IMMEDIATE", [])
  {:ok, _} = Honker.query_first(db.conn,
    "INSERT INTO orders (user_id, total) VALUES (?1, ?2)", [43, 5000])
  {:ok, _} = Honker.query_first(db.conn,
    "SELECT honker_enqueue(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    ["emails",
     Jason.encode!(%{"to" => "bob@example.com", "order_id" => 43}),
     nil, nil, 0, 3, nil])
  {:ok, _} = Honker.query_first(db.conn, "ROLLBACK", [])
  IO.puts("rolled back: simulated payment-processing failure")

  IO.puts(
    "after rollback: #{count.("SELECT COUNT(*) FROM orders")} order(s), " <>
    "#{count.("SELECT COUNT(*) FROM _honker_live WHERE queue='emails'")} job(s)"
  )
  IO.puts("atomic enqueue + rollback both work as expected")
after
  File.rm_rf!(dir)
end
