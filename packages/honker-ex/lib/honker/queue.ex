defmodule Honker.Queue do
  @moduledoc """
  Named queue operations. All methods take a `Honker.Database` and
  the queue name as the first two args.

      {:ok, id} = Honker.Queue.enqueue(db, "emails", %{to: "alice@example.com"})
      {:ok, job} = Honker.Queue.claim_one(db, "emails", "worker-1")
  """

  alias Honker.{Database, Job, Transaction}

  @doc """
  Enqueue a job. `payload` is any term — it's JSON-encoded on the way
  in. Options:

    * `:delay`    — seconds from now before claimable
    * `:run_at`   — absolute unix epoch (ignored if `:delay` is set)
    * `:priority` — higher = picked first within queue (default 0)
    * `:expires`  — seconds from now; drops out of claim pool after
  """
  def enqueue(%Database{conn: conn} = db, queue_name, payload, opts \\ []) do
    {_vis, max_attempts} = Honker.queue_opts(db, queue_name)
    do_enqueue(conn, queue_name, payload, opts, max_attempts)
  end

  @doc """
  Enqueue inside an open transaction. The job is only visible to
  claimers after `Honker.Transaction.commit/1`.

  `queue_opts` lets the caller pass per-queue options
  (`[visibility_timeout_s: _, max_attempts: _]`) when the transaction
  handle was obtained without going through a `Honker.Database`
  carrying a registered queue config. Pass `[]` to use the defaults
  (300s / 3 attempts).
  """
  def enqueue_tx(%Transaction{conn: conn}, queue_name, payload, opts, queue_opts) do
    max_attempts = Keyword.get(queue_opts, :max_attempts, 3)
    do_enqueue(conn, queue_name, payload, opts, max_attempts)
  end

  defp do_enqueue(conn, queue_name, payload, opts, max_attempts) do
    json = Jason.encode!(payload)

    params = [
      queue_name,
      json,
      Keyword.get(opts, :run_at),
      Keyword.get(opts, :delay),
      Keyword.get(opts, :priority, 0),
      max_attempts,
      Keyword.get(opts, :expires)
    ]

    case Honker.query_first(
           conn,
           "SELECT honker_enqueue(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
           params
         ) do
      {:ok, [id]} -> {:ok, id}
      other -> other
    end
  end

  @doc """
  Sweep expired processing rows back to pending. Returns
  `{:ok, rows_touched}`.
  """
  def sweep_expired(%Database{conn: conn}, queue_name) do
    case Honker.query_first(conn, "SELECT honker_sweep_expired(?1)", [queue_name]) do
      {:ok, [count]} -> {:ok, count || 0}
      other -> other
    end
  end

  @doc """
  Atomically claim up to `n` jobs. Returns `{:ok, [%Job{}, ...]}`,
  possibly empty if the queue had no eligible rows.
  """
  def claim_batch(%Database{conn: conn} = db, queue_name, worker_id, n) do
    {vis, _max} = Honker.queue_opts(db, queue_name)

    case Honker.query_first(
           conn,
           "SELECT honker_claim_batch(?1, ?2, ?3, ?4)",
           [queue_name, worker_id, n, vis]
         ) do
      {:ok, [rows_json]} ->
        jobs =
          rows_json
          |> Jason.decode!()
          |> Enum.map(&row_to_job/1)

        {:ok, jobs}

      other ->
        other
    end
  end

  @doc "Claim a single job or `{:ok, nil}` if the queue is empty."
  def claim_one(db, queue_name, worker_id) do
    case claim_batch(db, queue_name, worker_id, 1) do
      {:ok, [job | _]} -> {:ok, job}
      {:ok, []} -> {:ok, nil}
      other -> other
    end
  end

  defp row_to_job(row) do
    %Job{
      id: row["id"],
      queue: row["queue"],
      payload: Jason.decode!(row["payload"]),
      worker_id: row["worker_id"],
      attempts: row["attempts"]
    }
  end
end
