# frozen_string_literal: true

require "json"

module Honker
  # An append-only ordered log. Publish writes an offset-stamped event;
  # consumers resume from a saved offset. Mirrors the typed API in the
  # Rust binding.
  #
  #   s = db.stream("orders")
  #   off = s.publish({ id: 1 })
  #   events = s.read_since(0, 100)
  #   s.save_offset("billing", events.last.offset)
  class Stream
    attr_reader :topic

    def initialize(db, topic)
      @db = db
      @topic = topic
    end

    # Publish an event. Returns the assigned offset.
    def publish(payload)
      publish_with_key_opt(nil, payload)
    end

    # Publish with a partition key — used by downstream consumers that
    # want per-key ordering.
    def publish_with_key(key, payload)
      publish_with_key_opt(key, payload)
    end

    # Publish inside an open transaction. The event is visible to
    # readers only after the transaction commits.
    def publish_tx(tx, payload)
      row = tx.query_row(
        "SELECT honker_stream_publish(?, NULL, ?)",
        [@topic, JSON.dump(payload)],
      )
      row[0]
    end

    # Read events with offset > `offset`, up to `limit`. Returns an
    # array of StreamEvent.
    def read_since(offset, limit)
      rows_json = @db.db.get_first_row(
        "SELECT honker_stream_read_since(?, ?, ?)",
        [@topic, offset, limit],
      )[0]
      JSON.parse(rows_json).map { |r| StreamEvent.from_row(r) }
    end

    # Read events newer than this consumer's saved offset. Does NOT
    # advance the offset — call `save_offset` after processing.
    def read_from_consumer(consumer, limit)
      read_since(get_offset(consumer), limit)
    end

    # Save a consumer's offset. Monotonic: saving a lower offset is
    # ignored by the extension and returns false.
    def save_offset(consumer, offset)
      @db.db.get_first_row(
        "SELECT honker_stream_save_offset(?, ?, ?)",
        [consumer, @topic, offset],
      )[0] == 1
    end

    # Save offset inside an open transaction. Gives you exactly-once
    # semantics relative to whatever else ran on the same tx.
    def save_offset_tx(tx, consumer, offset)
      row = tx.query_row(
        "SELECT honker_stream_save_offset(?, ?, ?)",
        [consumer, @topic, offset],
      )
      row[0] == 1
    end

    # Current saved offset for `consumer`, or 0 if never saved.
    def get_offset(consumer)
      @db.db.get_first_row(
        "SELECT honker_stream_get_offset(?, ?)",
        [consumer, @topic],
      )[0]
    end

    private

    def publish_with_key_opt(key, payload)
      @db.db.get_first_row(
        "SELECT honker_stream_publish(?, ?, ?)",
        [@topic, key, JSON.dump(payload)],
      )[0]
    end
  end

  # One event in a stream. `payload` is already JSON-decoded.
  StreamEvent = Struct.new(:offset, :topic, :key, :payload, :created_at) do
    def self.from_row(row)
      new(
        row["offset"],
        row["topic"],
        row["key"],
        row["payload"].nil? ? nil : JSON.parse(row["payload"]),
        row["created_at"],
      )
    end
  end
end
