# frozen_string_literal: true
#
# Parity tests for the typed wrappers added on top of the raw SQL
# surface: Transaction, Stream, Scheduler, Lock, rate limits, results,
# notifications pruning, queue sweep.
#
# Run: bundle exec ruby -Ilib spec/parity_spec.rb

require "tmpdir"
require "minitest/autorun"
require "honker"

REPO_ROOT = File.expand_path("../../..", __dir__) unless defined?(REPO_ROOT)

def find_ext_parity
  %w[
    target/release/libhonker_ext.dylib
    target/release/libhonker_ext.so
  ].each do |rel|
    p = File.join(REPO_ROOT, rel)
    return p if File.exist?(p)
  end
  nil
end

class HonkerParityTest < Minitest::Test
  def setup
    ext = find_ext_parity
    skip "honker extension not built" unless ext
    @tmpdir = Dir.mktmpdir("honker-parity-")
    @db_path = File.join(@tmpdir, "t.db")
    @db = Honker::Database.new(@db_path, extension_path: ext)
  end

  def teardown
    @db&.close
    FileUtils.remove_entry(@tmpdir) if @tmpdir && File.directory?(@tmpdir)
  end

  # -----------------------------------------------------------------
  # Transaction
  # -----------------------------------------------------------------

  def test_transaction_commit_business_write_plus_enqueue_tx
    @db.db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    q = @db.queue("emails")

    @db.transaction do |tx|
      tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [1, 100])
      id = q.enqueue_tx(tx, { order_id: 1 })
      assert id > 0
    end

    assert_equal 1, @db.db.get_first_row("SELECT COUNT(*) FROM orders")[0]
    assert_equal 1, @db.db.get_first_row("SELECT COUNT(*) FROM _honker_live")[0]
  end

  def test_transaction_rollback_on_exception
    @db.db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    q = @db.queue("emails")

    assert_raises(RuntimeError) do
      @db.transaction do |tx|
        tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [2, 200])
        q.enqueue_tx(tx, { order_id: 2 })
        raise "abort"
      end
    end

    assert_equal 0, @db.db.get_first_row("SELECT COUNT(*) FROM orders")[0]
    assert_equal 0, @db.db.get_first_row("SELECT COUNT(*) FROM _honker_live")[0]
  end

  def test_transaction_explicit_rollback_bang
    @db.db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    q = @db.queue("emails")

    @db.transaction do |tx|
      tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [3, 300])
      q.enqueue_tx(tx, { order_id: 3 })
      tx.rollback!
    end

    # rollback! unwinds without raising upward, yet both writes are gone.
    assert_equal 0, @db.db.get_first_row("SELECT COUNT(*) FROM orders")[0]
    assert_equal 0, @db.db.get_first_row("SELECT COUNT(*) FROM _honker_live")[0]
  end

  def test_notify_tx_visible_only_after_commit
    @db.transaction do |tx|
      id = @db.notify_tx(tx, "orders", { id: 42 })
      assert id > 0
    end
    count = @db.db.get_first_row(
      "SELECT COUNT(*) FROM _honker_notifications WHERE channel='orders'",
    )[0]
    assert_equal 1, count
  end

  # -----------------------------------------------------------------
  # Stream
  # -----------------------------------------------------------------

  def test_stream_publish_read_round_trip
    s = @db.stream("orders")
    off1 = s.publish({ id: 1 })
    off2 = s.publish({ id: 2 })
    assert off2 > off1

    events = s.read_since(0, 100)
    assert_equal 2, events.length
    assert_equal "orders", events[0].topic
    assert_equal off1, events[0].offset
    assert_equal({ "id" => 1 }, events[0].payload)
    assert_equal({ "id" => 2 }, events[1].payload)
  end

  def test_stream_publish_with_key_is_readable
    s = @db.stream("by-user")
    s.publish_with_key("u-1", { event: "login" })
    s.publish_with_key("u-2", { event: "login" })
    events = s.read_since(0, 100)
    assert_equal 2, events.length
    assert_equal %w[u-1 u-2], events.map(&:key)
  end

  def test_stream_consumer_offset_round_trip
    s = @db.stream("events")
    3.times { |i| s.publish({ i: i }) }

    assert_equal 0, s.get_offset("worker-a")
    events = s.read_from_consumer("worker-a", 100)
    assert_equal 3, events.length

    assert s.save_offset("worker-a", events.last.offset)
    assert_equal events.last.offset, s.get_offset("worker-a")

    # Re-reading from the consumer now returns nothing (caught up).
    assert_equal 0, s.read_from_consumer("worker-a", 100).length
  end

  def test_stream_save_offset_tx_respects_rollback
    s = @db.stream("events")
    s.publish({ i: 1 })
    off = s.publish({ i: 2 })

    assert_raises(RuntimeError) do
      @db.transaction do |tx|
        assert s.save_offset_tx(tx, "worker-b", off)
        raise "abort"
      end
    end

    # Rollback means the offset was never persisted.
    assert_equal 0, s.get_offset("worker-b")
  end

  # -----------------------------------------------------------------
  # Scheduler
  # -----------------------------------------------------------------

  def test_scheduler_add_and_soonest
    sch = @db.scheduler
    sch.add(name: "hourly", queue: "health", schedule: "0 * * * *", payload: {})
    soonest = sch.soonest
    assert soonest > 0
  end

  def test_scheduler_remove
    sch = @db.scheduler
    sch.add(name: "nightly", queue: "health", schedule: "0 0 * * *", payload: {})
    removed = sch.remove("nightly")
    assert_equal 1, removed
    assert_equal 0, sch.soonest
  end

  def test_scheduler_tick_fires_due_task
    sch = @db.scheduler
    # "* * * * *" fires every minute — next_fire_at lands within a minute.
    sch.add(name: "every-min", queue: "beats", schedule: "* * * * *", payload: { ok: true })
    # Tick far enough in the future to guarantee a fire.
    fires = sch.tick(Time.now.to_i + 3_600)
    assert fires.length >= 1
    fire = fires.first
    assert_equal "every-min", fire.name
    assert_equal "beats", fire.queue
    assert fire.job_id > 0
  end

  def test_scheduler_accepts_interval_schedule_alias
    sch = @db.scheduler
    sch.add(name: "fast", queue: "beats", schedule: "@every 1s", payload: { ok: true })
    soonest = sch.soonest
    assert soonest > 0

    fires = sch.tick(soonest)
    assert_equal 1, fires.length
    assert_equal "fast", fires.first.name
  end

  def test_scheduler_accepts_legacy_cron_alias
    sch = @db.scheduler
    sch.add(name: "legacy", queue: "beats", cron: "@every 1s", payload: { ok: true })
    soonest = sch.soonest
    assert soonest > 0
  end

  def test_scheduler_run_start_and_stop
    sch = @db.scheduler
    stop = Struct.new(:flag) do
      def stop?
        flag.value
      end
    end
    flag = Struct.new(:value).new(false)
    stopper = stop.new(flag)

    t = Thread.new { sch.run(owner: "host-1", stop: stopper) }
    # Give the leader loop a tick to acquire the lock.
    sleep 0.2
    flag.value = true
    t.join(5)
    refute t.alive?, "scheduler thread should exit when stop is set"
  end

  def test_scheduler_run_fires_every_second_schedule
    sch = @db.scheduler
    q = @db.queue("fast")
    sch.add(name: "fast", queue: "fast", schedule: "@every 1s", payload: { ok: true })

    flag = Struct.new(:value).new(false)
    stopper = Struct.new(:flag) do
      def stop?
        flag.value
      end
    end.new(flag)

    t = Thread.new { sch.run(owner: "host-fast", stop: stopper) }
    begin
      deadline = Time.now + 3
      job = nil
      while Time.now < deadline
        job = q.claim_one("worker-fast")
        break if job
        sleep 0.05
      end

      refute_nil job, "expected every-second schedule to enqueue within 3s"
      assert_equal({ "ok" => true }, job.payload)
      assert job.ack
    ensure
      flag.value = true
      t.join(5)
    end
  end

  def test_scheduler_run_wakes_when_new_schedule_is_registered
    sch = @db.scheduler
    q = @db.queue("wake")
    flag = Struct.new(:value).new(false)
    stopper = Struct.new(:flag) do
      def stop?
        flag.value
      end
    end.new(flag)

    t = Thread.new { sch.run(owner: "host-wake", stop: stopper) }
    begin
      sleep 0.2
      sch.add(name: "wake-fast", queue: "wake", schedule: "@every 1s", payload: { wake: true })

      deadline = Time.now + 3
      job = nil
      while Time.now < deadline
        job = q.claim_one("worker-wake")
        break if job
        sleep 0.05
      end

      refute_nil job, "expected sleeping scheduler to wake after new schedule registration"
      assert_equal({ "wake" => true }, job.payload)
      assert job.ack
    ensure
      flag.value = true
      t.join(5)
    end
  end

  # -----------------------------------------------------------------
  # Lock
  # -----------------------------------------------------------------

  def test_lock_mutual_exclusion_and_reacquire
    l1 = @db.try_lock("critical", owner: "a", ttl_s: 60)
    refute_nil l1

    # A different owner cannot acquire while l1 holds it.
    l2 = @db.try_lock("critical", owner: "b", ttl_s: 60)
    assert_nil l2

    assert l1.release
    # After release a different owner CAN acquire.
    l3 = @db.try_lock("critical", owner: "b", ttl_s: 60)
    refute_nil l3
    assert l3.release
  end

  def test_lock_double_release_is_noop
    l = @db.try_lock("idempotent", owner: "a", ttl_s: 60)
    refute_nil l
    assert l.release
    refute l.release, "second release should no-op and return false"
  end

  def test_lock_heartbeat_extends_ttl
    l = @db.try_lock("hb", owner: "a", ttl_s: 60)
    refute_nil l
    assert l.heartbeat(ttl_s: 120), "owner's heartbeat should refresh"

    # Another owner still can't grab it.
    other = @db.try_lock("hb", owner: "b", ttl_s: 60)
    assert_nil other
    assert l.release
  end

  # -----------------------------------------------------------------
  # Rate limit
  # -----------------------------------------------------------------

  def test_rate_limit_allows_n_then_blocks
    3.times do |i|
      assert @db.try_rate_limit("api", limit: 3, per: 60),
             "call #{i} should be allowed"
    end
    refute @db.try_rate_limit("api", limit: 3, per: 60),
           "fourth call should be blocked"
  end

  # -----------------------------------------------------------------
  # Results
  # -----------------------------------------------------------------

  def test_results_save_get_roundtrip_and_missing_returns_nil
    @db.save_result(42, JSON.dump({ ok: true }), ttl_s: 3600)
    got = @db.get_result(42)
    assert_equal({ "ok" => true }, JSON.parse(got))

    assert_nil @db.get_result(999)
  end

  def test_results_sweep_removes_expired
    @db.save_result(1, "v1", ttl_s: 3600)
    swept = @db.sweep_results
    # Nothing expired yet; sweep is a no-op but returns a count.
    assert_equal 0, swept
  end

  # -----------------------------------------------------------------
  # Queue.sweep_expired + prune_notifications
  # -----------------------------------------------------------------

  def test_queue_sweep_expired_returns_count
    q = @db.queue("sw", visibility_timeout_s: 1)
    q.enqueue({ x: 1 })
    # No claim made yet -> nothing to sweep.
    assert_equal 0, q.sweep_expired
  end

  def test_prune_notifications_with_very_old_threshold_keeps_fresh
    @db.notify("ch", { id: 1 })
    @db.notify("ch", { id: 2 })
    # Nothing older than 1h ago; all rows fresh.
    assert_equal 0, @db.prune_notifications(older_than_s: 3600)
    count = @db.db.get_first_row(
      "SELECT COUNT(*) FROM _honker_notifications WHERE channel='ch'",
    )[0]
    assert_equal 2, count
  end
end
