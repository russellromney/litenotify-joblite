# frozen_string_literal: true

module Honker
  # Wraps a SQLite transaction with helpers that thread SQL through a
  # stable connection handle. Matches the shape of the Rust binding's
  # `Transaction` — `execute` and `query_row` route to the underlying
  # connection, which `*_tx` methods (`Queue#enqueue_tx`,
  # `Stream#publish_tx`, `Stream#save_offset_tx`, `Database#notify_tx`)
  # use to stay inside the open transaction.
  #
  # Obtain one via `Database#transaction do |tx| ... end`. The block
  # auto-commits on normal return and auto-rolls-back on exception; the
  # wrapping `sqlite3` gem handles the BEGIN/COMMIT/ROLLBACK for us.
  # Callers may also invoke `tx.rollback!` mid-block to abort without
  # raising.
  class Transaction
    # The raw `SQLite3::Database` underneath. Exposed as `conn` to
    # mirror the Rust shape for advanced users who need `prepare` or
    # other APIs not wrapped here.
    attr_reader :conn

    def initialize(conn)
      @conn = conn
      @rolled_back = false
    end

    # Execute a SQL statement inside the transaction.
    def execute(sql, params = [])
      @conn.execute(sql, params)
    end

    # Run a query and return the first row (or nil).
    def query_row(sql, params = [])
      @conn.get_first_row(sql, params)
    end

    # Abort the transaction early. Raises `Transaction::Rollback`
    # internally so the outer sqlite3-gem block sees an exception and
    # rolls back; we then swallow it at the `Database#transaction`
    # boundary.
    def rollback!
      @rolled_back = true
      raise Rollback
    end

    def rolled_back?
      @rolled_back
    end

    # Sentinel to unwind the sqlite3 gem's transaction block without
    # surfacing as a user-visible exception.
    class Rollback < StandardError; end
  end
end
