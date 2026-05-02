# frozen_string_literal: true

module Honker
  # Advisory lock. Returned by `Database#try_lock`; `nil` means another
  # owner holds it.
  #
  # Prefer explicit `release` — finalizers are best-effort. Holding the
  # `Lock` instance does NOT guarantee you still own the lock; the
  # extension's TTL can expire and a new owner take it. Use
  # `heartbeat(ttl_s:)` periodically and check its return value.
  class Lock
    attr_reader :name, :owner

    def initialize(db, name, owner)
      @db = db
      @name = name
      @owner = owner
      @released = false
      # Best-effort cleanup if the caller forgets to release. We capture
      # closed-over primitives (not `self`) so the finalizer can run
      # without keeping the Lock itself alive.
      ObjectSpace.define_finalizer(self, self.class._finalizer(db, name, owner))
    end

    # Class-level factory so the finalizer doesn't close over `self`.
    def self._finalizer(db, name, owner)
      proc do
        begin
          db.db.get_first_row(
            "SELECT honker_lock_release(?, ?)",
            [name, owner],
          )
        rescue StandardError
          # Finalizers run after the interpreter teardown can begin;
          # swallowing is the only safe option here.
        end
      end
    end

    # Release the lock. Idempotent — calling release twice is a no-op
    # on the second call.
    def release
      return false if @released

      @released = true
      @db.db.get_first_row(
        "SELECT honker_lock_release(?, ?)",
        [@name, @owner],
      )[0] == 1
    end

    # True if the caller has already released this handle.
    def released?
      @released
    end

    # Extend the TTL. Returns true if we still hold the lock; false if
    # it was stolen (the TTL elapsed and another owner acquired it).
    # The underlying SQL is the same as `try_lock`, but keyed on our
    # existing `(name, owner)` pair so it refreshes rather than blocks.
    def heartbeat(ttl_s:)
      @db.db.get_first_row(
        "SELECT honker_lock_acquire(?, ?, ?)",
        [@name, @owner, ttl_s],
      )[0] == 1
    end
  end
end
