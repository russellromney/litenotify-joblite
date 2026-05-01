defmodule Honker.StreamEvent do
  @moduledoc """
  An event read from a Honker stream. `payload` is decoded from JSON;
  `key` is `nil` when the event was published without a partition key.
  """
  defstruct [:offset, :topic, :key, :payload, :created_at]
end
