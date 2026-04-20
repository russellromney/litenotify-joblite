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

  @doc false
  def queue_opts(%Database{queue_opts: opts}, queue_name) do
    Map.get(opts, queue_name, {300, 3})
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
end
