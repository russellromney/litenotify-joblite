defmodule Honker.Lock do
  @moduledoc """
  Advisory locks on top of `honker_lock_*` SQL functions. Elixir has no
  RAII, so the caller manages the lock lifetime explicitly:

      case Honker.Lock.try_acquire(db, "nightly-report", "worker-1", 60) do
        {:ok, nil} -> :already_held
        {:ok, %Honker.Lock{} = lock} ->
          try do
            do_work()
          after
            Honker.Lock.release(lock, db)
          end
      end

  `heartbeat/3` refreshes the TTL. If it returns `{:ok, false}`, another
  owner has stolen the lock — stop doing the work the lock guards.
  """

  alias Exqlite.Sqlite3
  alias Honker.Database

  @enforce_keys [:name, :owner]
  defstruct [:name, :owner]

  @doc """
  Attempt to acquire the lock. Returns `{:ok, %Honker.Lock{}}` if
  acquired, `{:ok, nil}` if another owner already holds it.
  """
  def try_acquire(%Database{conn: conn}, name, owner, ttl_s) do
    case Honker.query_first(
           conn,
           "SELECT honker_lock_acquire(?1, ?2, ?3)",
           [name, owner, ttl_s]
         ) do
      {:ok, [1]} -> {:ok, %__MODULE__{name: name, owner: owner}}
      {:ok, [0]} -> {:ok, nil}
      {:ok, nil} -> {:ok, nil}
      other -> other
    end
  end

  @doc """
  Release the lock. Returns `{:ok, true}` if we still held it and it
  was released, `{:ok, false}` if the TTL had already expired and the
  lock wasn't ours anymore.
  """
  def release(%__MODULE__{name: name, owner: owner}, %Database{conn: conn}) do
    case Honker.query_first(
           conn,
           "SELECT honker_lock_release(?1, ?2)",
           [name, owner]
         ) do
      {:ok, [1]} -> {:ok, true}
      {:ok, [0]} -> {:ok, false}
      {:ok, nil} -> {:ok, false}
      other -> other
    end
  end

  @doc """
  Refresh (extend) the lock's TTL.

  `honker_lock_acquire` uses `INSERT OR IGNORE` internally, so it
  can't extend the TTL on an already-held row — we have to UPDATE
  `expires_at` directly (matching what the Python scheduler does).

  Returns `{:ok, true}` if we still own it, `{:ok, false}` if the TTL
  already expired and someone else took it (row now has a different
  owner) or the row is gone entirely. Check the return value —
  holding the `%Honker.Lock{}` struct is not a guarantee.
  """
  def heartbeat(%__MODULE__{name: name, owner: owner}, %Database{conn: conn}, ttl_s) do
    # UPDATE ... RETURNING would be cleanest, but Exqlite's `step` gives
    # us the affected-row state, and `Sqlite3.changes/1` reflects the
    # last write. Scope the update to (name, owner) so a stolen lock is
    # a zero-change UPDATE.
    sql = """
    UPDATE _honker_locks
    SET expires_at = unixepoch() + ?3
    WHERE name = ?1 AND owner = ?2
    """

    with {:ok, stmt} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(stmt, [name, owner, ttl_s]),
         step <- Sqlite3.step(conn, stmt),
         :ok <- Sqlite3.release(conn, stmt) do
      case step do
        :done -> {:ok, changed?(conn)}
        {:row, _} -> {:ok, changed?(conn)}
        err -> err
      end
    end
  end

  defp changed?(conn) do
    case Sqlite3.changes(conn) do
      {:ok, n} when n > 0 -> true
      n when is_integer(n) and n > 0 -> true
      _ -> false
    end
  end
end
