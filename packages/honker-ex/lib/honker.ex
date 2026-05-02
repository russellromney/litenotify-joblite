defmodule Honker do
  @moduledoc """
  Elixir binding for [Honker](https://honker.dev) — a SQLite-native
  task runtime.

  This module is a thin wrapper around the Honker loadable extension.
  Each public function is one SQL call via `Exqlite.Sqlite3`; state
  lives in the `.db` file, not in Elixir processes.

  ## Example

      {:ok, db} = Honker.open("app.db", extension_path: "./libhonker_ext.dylib")

      Honker.Queue.enqueue(db, "emails", %{to: "alice@example.com"})

      case Honker.Queue.claim_one(db, "emails", "worker-1") do
        {:ok, nil} -> :empty
        {:ok, job} ->
          send_email(job.payload)
          Honker.Job.ack(db, job)
      end

  A `Honker.Database` struct is the handle; it wraps an `Exqlite.Sqlite3`
  reference + the queue's default options (visibility timeout, max
  attempts). Use `configure_queue/3` to override per-queue.
  """

  alias Exqlite.Sqlite3

  @default_pragmas """
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA busy_timeout = 5000;
  PRAGMA foreign_keys = ON;
  PRAGMA cache_size = -32000;
  PRAGMA temp_store = MEMORY;
  PRAGMA wal_autocheckpoint = 10000;
  """

  defmodule Database do
    @moduledoc """
    A Honker database handle. Wraps the Exqlite reference + a
    registry of per-queue options (visibility timeout, max attempts).
    """
    defstruct [:conn, queue_opts: %{}]
  end

  @update_table :honker_local_update_seq

  # Honker.Job lives in lib/honker/job.ex — its struct + lifecycle
  # helpers are co-located so callers can `alias Honker.Job; Job.ack`
  # without pulling the Database+Queue modules into scope.

  @doc """
  Open (or create) a SQLite database at `path`. Loads the Honker
  extension from `extension_path`, applies default PRAGMAs, and
  bootstraps the schema.
  """
  def open(path, opts) do
    extension_path = Keyword.fetch!(opts, :extension_path)

    with {:ok, conn} <- Sqlite3.open(path),
         :ok <- Sqlite3.enable_load_extension(conn, true),
         :ok <- run_bare(conn, "SELECT load_extension(?1)", [extension_path]),
         :ok <- Sqlite3.enable_load_extension(conn, false),
         :ok <- Sqlite3.execute(conn, @default_pragmas),
         :ok <- run_bare(conn, "SELECT honker_bootstrap()", []) do
      {:ok, %Database{conn: conn}}
    end
  end

  @doc """
  Set the options for a named queue (visibility timeout, max attempts).
  Stored in the handle; Queue.enqueue/claim reads them.

  Defaults: visibility_timeout_s = 300, max_attempts = 3.
  """
  def configure_queue(%Database{} = db, queue_name, opts) do
    vis = Keyword.get(opts, :visibility_timeout_s, 300)
    max = Keyword.get(opts, :max_attempts, 3)
    %{db | queue_opts: Map.put(db.queue_opts, queue_name, {vis, max})}
  end

  @doc """
  Fire a pg_notify-style pub/sub signal. Returns `{:ok, id}`.
  """
  def notify(%Database{conn: conn}, channel, payload) do
    json = Jason.encode!(payload)

    case query_first(conn, "SELECT notify(?1, ?2)", [channel, json]) do
      {:ok, [id]} -> {:ok, id}
      other -> other
    end
  end

  @doc """
  Fire a notification inside an open transaction. The notification row
  only becomes visible to listeners after the transaction commits.
  """
  def notify_tx(%Honker.Transaction{conn: conn}, channel, payload) do
    json = Jason.encode!(payload)

    case query_first(conn, "SELECT notify(?1, ?2)", [channel, json]) do
      {:ok, [id]} -> {:ok, id}
      other -> other
    end
  end

  @doc """
  Fixed-window rate limit. Returns `{:ok, true}` if the caller fits in
  `limit` per `per` seconds, `{:ok, false}` if blocked.
  """
  def try_rate_limit(%Database{conn: conn}, name, limit, per) do
    case query_first(conn, "SELECT honker_rate_limit_try(?1, ?2, ?3)", [name, limit, per]) do
      {:ok, [1]} -> {:ok, true}
      {:ok, [0]} -> {:ok, false}
      {:ok, nil} -> {:ok, false}
      other -> other
    end
  end

  @doc """
  Persist a job result for later retrieval via `get_result/2`. `value`
  must be a string — encode JSON yourself if you want structure. `ttl_s`
  is seconds before `sweep_results/1` will delete it.
  """
  def save_result(%Database{conn: conn}, job_id, value, ttl_s) when is_binary(value) do
    case query_first(conn, "SELECT honker_result_save(?1, ?2, ?3)", [job_id, value, ttl_s]) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @doc """
  Fetch a stored result. Returns `{:ok, value}` or `{:ok, nil}` when
  absent or expired.
  """
  def get_result(%Database{conn: conn}, job_id) do
    case query_first(conn, "SELECT honker_result_get(?1)", [job_id]) do
      {:ok, [value]} -> {:ok, value}
      {:ok, nil} -> {:ok, nil}
      other -> other
    end
  end

  @doc "Delete expired results. Returns `{:ok, count_deleted}`."
  def sweep_results(%Database{conn: conn}) do
    case query_first(conn, "SELECT honker_result_sweep()", []) do
      {:ok, [count]} -> {:ok, count || 0}
      other -> other
    end
  end

  @doc """
  Delete notifications older than `older_than_s` seconds. Returns
  `{:ok, count_deleted}`.
  """
  def prune_notifications(%Database{conn: conn}, older_than_s) do
    sql = """
    DELETE FROM _honker_notifications
    WHERE created_at < unixepoch() - ?1
    """

    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(stmt, [older_than_s]),
         step <- Sqlite3.step(conn, stmt),
         :ok <- Sqlite3.release(conn, stmt) do
      case step do
        :done -> {:ok, changes_count(conn)}
        {:row, _} -> {:ok, changes_count(conn)}
        err -> err
      end
    end
  end

  defp changes_count(conn) do
    case Sqlite3.changes(conn) do
      {:ok, n} -> n
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  @doc false
  def queue_opts(%Database{queue_opts: opts}, queue_name) do
    Map.get(opts, queue_name, {300, 3})
  end

  @doc false
  def mark_updated(conn) do
    ensure_update_table()

    try do
      :ets.update_counter(@update_table, conn, {2, 1}, {conn, 0})
      :ok
    rescue
      ArgumentError ->
        :ok
    end
  end

  @doc false
  def update_snapshot(conn) do
    ensure_update_table()

    case :ets.lookup(@update_table, conn) do
      [{^conn, n}] -> n
      _ -> 0
    end
  end

  @doc false
  def query_first(conn, sql, params) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(stmt, params),
         {:row, row} <- Sqlite3.step(conn, stmt),
         :ok <- Sqlite3.release(conn, stmt) do
      {:ok, row}
    else
      :done ->
        {:ok, nil}

      err ->
        err
    end
  end

  @doc false
  def run_bare(conn, sql, params) do
    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(stmt, params),
         step <- Sqlite3.step(conn, stmt),
         :ok <- Sqlite3.release(conn, stmt) do
      case step do
        {:row, _} -> :ok
        :done -> :ok
        err -> err
      end
    end
  end

  defp ensure_update_table do
    case :ets.whereis(@update_table) do
      :undefined ->
        try do
          :ets.new(@update_table, [:named_table, :public, :set, read_concurrency: true])
        rescue
          ArgumentError -> @update_table
        end

      _ ->
        @update_table
    end
  end
end
