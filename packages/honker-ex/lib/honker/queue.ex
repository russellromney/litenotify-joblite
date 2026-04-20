defmodule Honker.Queue do
  @moduledoc """
  Named queue operations. All methods take a `Honker.Database` and
  the queue name as the first two args.

      {:ok, id} = Honker.Queue.enqueue(db, "emails", %{to: "alice@example.com"})
      {:ok, job} = Honker.Queue.claim_one(db, "emails", "worker-1")
  """

  alias Honker.{Database, Job}

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
