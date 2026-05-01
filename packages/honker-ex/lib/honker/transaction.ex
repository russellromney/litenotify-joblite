defmodule Honker.Transaction do
  @moduledoc """
  A SQLite transaction handle. Wraps the underlying Exqlite connection
  while a transaction is open so `*_tx` helpers (e.g.
  `Honker.Queue.enqueue_tx/5`, `Honker.Stream.publish_tx/4`) can take
  it as an explicit parameter.

      {:ok, tx} = Honker.Transaction.begin(db)
      :ok = Honker.Transaction.execute(tx, "INSERT INTO orders ...", [])
      {:ok, _} = Honker.Queue.enqueue_tx(tx, "emails", %{order_id: 1}, [], [])
      :ok = Honker.Transaction.commit(tx)

  Higher-level `transaction/2` runs a function inside `BEGIN IMMEDIATE`,
  commits on success, and rolls back on raised exception — matching the
  Rust RAII shape with Elixir ergonomics.
  """

  alias Exqlite.Sqlite3
  alias Honker.Database

  @enforce_keys [:conn]
  defstruct [:conn]

  @doc """
  Begin a transaction with `BEGIN IMMEDIATE`. Returns `{:ok, tx}` where
  `tx` is a `%Honker.Transaction{}` carrying the raw connection.

  The Exqlite connection is single-threaded; the caller is responsible
  for serialization. Pair with an explicit `commit/1` or `rollback/1`.
  """
  def begin(%Database{conn: conn}) do
    case Sqlite3.execute(conn, "BEGIN IMMEDIATE") do
      :ok -> {:ok, %__MODULE__{conn: conn}}
      err -> err
    end
  end

  @doc """
  Run a statement for side effects. Returns `:ok` on a `:row` or `:done`
  step, or an error tuple. Accepts positional parameters via `?1`-style
  placeholders.
  """
  def execute(%__MODULE__{conn: conn}, sql, params \\ []) do
    Honker.run_bare(conn, sql, params)
  end

  @doc """
  Run a statement and return the first row as a list `[col0, col1, ...]`,
  or `{:ok, nil}` when the query returns no rows. Mirrors
  `Honker.query_first/3`.
  """
  def query_row(%__MODULE__{conn: conn}, sql, params \\ []) do
    Honker.query_first(conn, sql, params)
  end

  @doc "Commit the transaction."
  def commit(%__MODULE__{conn: conn}) do
    Sqlite3.execute(conn, "COMMIT")
  end

  @doc "Roll back the transaction."
  def rollback(%__MODULE__{conn: conn}) do
    Sqlite3.execute(conn, "ROLLBACK")
  end

  @doc """
  Run `fun.(tx)` inside `BEGIN IMMEDIATE`. Commits on `:ok` /
  `{:ok, _}` / any non-error term; rolls back and re-raises on
  exception. Returns whatever `fun` returned.

  `{:error, _}` returned by `fun` rolls back and the error is
  propagated. Use this when you want the common "atomic business write
  + enqueue" shape without hand-rolling commit/rollback.
  """
  def transaction(%Database{} = db, fun) when is_function(fun, 1) do
    {:ok, tx} = begin(db)

    try do
      result = fun.(tx)

      case result do
        {:error, _} = err ->
          :ok = rollback(tx)
          err

        other ->
          :ok = commit(tx)
          other
      end
    rescue
      e ->
        _ = rollback(tx)
        reraise e, __STACKTRACE__
    end
  end
end
