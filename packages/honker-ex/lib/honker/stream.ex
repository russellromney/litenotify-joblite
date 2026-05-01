defmodule Honker.Stream do
  @moduledoc """
  Append-only topic log with named-consumer offsets. Thin wrapper over
  `honker_stream_*` SQL functions.

      {:ok, off1} = Honker.Stream.publish(db, "orders", %{id: 1})
      {:ok, events} = Honker.Stream.read_since(db, "orders", 0, 100)
      {:ok, true}  = Honker.Stream.save_offset(db, "billing", "orders", off1)
  """

  alias Honker.{Database, StreamEvent, Transaction}

  @doc """
  Publish an event on `topic`. Returns `{:ok, offset}` — a monotonic
  per-topic offset assigned by the extension.

  Options:

    * `:key` — optional partition key (string) for per-key ordering
      downstream. Defaults to `nil`.
  """
  def publish(%Database{conn: conn}, topic, payload, opts \\ []) do
    do_publish(conn, topic, payload, Keyword.get(opts, :key))
  end

  @doc "Publish inside an open transaction."
  def publish_tx(%Transaction{conn: conn}, topic, payload, opts \\ []) do
    do_publish(conn, topic, payload, Keyword.get(opts, :key))
  end

  defp do_publish(conn, topic, payload, key) do
    json = Jason.encode!(payload)

    case Honker.query_first(
           conn,
           "SELECT honker_stream_publish(?1, ?2, ?3)",
           [topic, key, json]
         ) do
      {:ok, [offset]} -> {:ok, offset}
      other -> other
    end
  end

  @doc """
  Read up to `limit` events from `topic` with offset strictly greater
  than `offset`. Returns `{:ok, [%Honker.StreamEvent{}, ...]}`.
  """
  def read_since(%Database{conn: conn}, topic, offset, limit) do
    case Honker.query_first(
           conn,
           "SELECT honker_stream_read_since(?1, ?2, ?3)",
           [topic, offset, limit]
         ) do
      {:ok, [rows_json]} ->
        events =
          rows_json
          |> Jason.decode!()
          |> Enum.map(&to_event/1)

        {:ok, events}

      other ->
        other
    end
  end

  @doc """
  Read from the saved offset of `consumer` on `topic`. Does **not**
  advance the offset — call `save_offset/4` after you've processed.
  """
  def read_from_consumer(%Database{} = db, topic, consumer, limit) do
    with {:ok, offset} <- get_offset(db, consumer, topic) do
      read_since(db, topic, offset, limit)
    end
  end

  @doc """
  Persist a consumer's offset. Monotonic: saving a lower offset than
  already stored is a no-op (returns `{:ok, false}`).
  """
  def save_offset(%Database{conn: conn}, consumer, topic, offset) do
    do_save_offset(conn, consumer, topic, offset)
  end

  @doc "Save offset inside an open transaction."
  def save_offset_tx(%Transaction{conn: conn}, consumer, topic, offset) do
    do_save_offset(conn, consumer, topic, offset)
  end

  defp do_save_offset(conn, consumer, topic, offset) do
    case Honker.query_first(
           conn,
           "SELECT honker_stream_save_offset(?1, ?2, ?3)",
           [consumer, topic, offset]
         ) do
      {:ok, [1]} -> {:ok, true}
      {:ok, [0]} -> {:ok, false}
      {:ok, nil} -> {:ok, false}
      other -> other
    end
  end

  @doc "Current saved offset for `consumer` on `topic`, or 0 if unset."
  def get_offset(%Database{conn: conn}, consumer, topic) do
    case Honker.query_first(
           conn,
           "SELECT honker_stream_get_offset(?1, ?2)",
           [consumer, topic]
         ) do
      {:ok, [offset]} -> {:ok, offset || 0}
      {:ok, nil} -> {:ok, 0}
      other -> other
    end
  end

  defp to_event(row) do
    %StreamEvent{
      offset: row["offset"],
      topic: row["topic"],
      key: row["key"],
      payload: decode_payload(row["payload"]),
      created_at: row["created_at"]
    }
  end

  defp decode_payload(nil), do: nil
  defp decode_payload(s) when is_binary(s), do: Jason.decode!(s)
end
