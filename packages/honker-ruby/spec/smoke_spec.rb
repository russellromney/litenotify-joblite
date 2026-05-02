# frozen_string_literal: true
#
# End-to-end smoke tests. Exercises every operation a Ruby user can
# reach today — the typed queue API plus raw-SQL access to streams,
# scheduler, locks, rate limits, and results (bindings for those are
# on the roadmap; this test ensures the SQL surface works from Ruby).
#
# Run: bundle exec ruby -Ilib spec/smoke_spec.rb

require "tmpdir"
require "minitest/autorun"
require "honker"

REPO_ROOT = File.expand_path("../../..", __dir__) unless defined?(REPO_ROOT)

def find_ext
  %w[
    target/release/libhonker_ext.dylib
    target/release/libhonker_ext.so
  ].each do |rel|
    p = File.join(REPO_ROOT, rel)
    return p if File.exist?(p)
  end
  nil
end

class HonkerSmokeTest < Minitest::Test
  def setup
    ext = find_ext
    skip "honker extension not built" unless ext
    @ext = ext
    @tmpdir = Dir.mktmpdir("honker-smoke-")
    @db_path = File.join(@tmpdir, "t.db")
    @db = Honker::Database.new(@db_path, extension_path: @ext)
  end

  def teardown
    @db&.close
    FileUtils.remove_entry(@tmpdir) if @tmpdir && File.directory?(@tmpdir)
  end

  def test_claim_batch_returns_multiple_jobs
    q = @db.queue("bulk")
    5.times { |i| q.enqueue({ i: i }) }

    jobs = q.claim_batch("w", 10)
    assert_equal 5, jobs.length
    assert_equal (0..4).to_a, jobs.map { |j| j.payload["i"] }.sort

    jobs.each(&:ack)
    assert_nil q.claim_one("w"), "queue empty after ack-all"
  end

  def test_heartbeat_extends_visibility
    q = @db.queue("hb", visibility_timeout_s: 1)
    q.enqueue({})
    job = q.claim_one("w")
    refute_nil job
    assert job.heartbeat(extend_s: 60), "fresh claim should heartbeat"
    assert job.ack
  end

  def test_fail_moves_to_dead_immediately
    q = @db.queue("failing")
    q.enqueue({ x: 1 })
    job = q.claim_one("w")
    assert job.fail(error: "nope")

    dead_count = @db.db.get_first_row(
      "SELECT COUNT(*) FROM _honker_dead WHERE queue='failing'",
    )[0]
    assert_equal 1, dead_count
  end

  def test_atomic_business_write_plus_enqueue
    @db.db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    q = @db.queue("emails")

    # Commit path — both rows land.
    @db.db.transaction do |tx|
      tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [1, 100])
      tx.execute(
        "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
        ["emails", JSON.dump({ order_id: 1 }), nil, nil, 0, 3, nil],
      )
    end
    assert_equal 1, @db.db.get_first_row("SELECT COUNT(*) FROM orders")[0]
    assert_equal 1, @db.db.get_first_row("SELECT COUNT(*) FROM _honker_live")[0]

    # Rollback path — neither row lands.
    begin
      @db.db.transaction do |tx|
        tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [2, 200])
        tx.execute(
          "SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?)",
          ["emails", JSON.dump({ order_id: 2 }), nil, nil, 0, 3, nil],
        )
        raise "abort"
      end
    rescue RuntimeError => e
      assert_equal "abort", e.message
    end
    assert_equal 1, @db.db.get_first_row("SELECT COUNT(*) FROM orders")[0]
    assert_equal 1, @db.db.get_first_row("SELECT COUNT(*) FROM _honker_live")[0]
  end

  def test_stream_publish_read_via_sql
    # Streams don't have a typed wrapper yet; verify the SQL surface
    # is reachable from Ruby so users can use it today.
    off1 = @db.db.get_first_row(
      "SELECT honker_stream_publish(?, ?, ?)",
      ["orders", nil, JSON.dump({ id: 1 })],
    )[0]
    off2 = @db.db.get_first_row(
      "SELECT honker_stream_publish(?, ?, ?)",
      ["orders", nil, JSON.dump({ id: 2 })],
    )[0]
    assert off2 > off1

    rows_json = @db.db.get_first_row(
      "SELECT honker_stream_read_since(?, ?, ?)",
      ["orders", 0, 100],
    )[0]
    events = JSON.parse(rows_json)
    assert_equal 2, events.length
    assert_equal 1, JSON.parse(events[0]["payload"])["id"]
  end

  def test_scheduler_register_and_soonest
    @db.db.execute(
      "SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?)",
      ["hourly", "health", "0 * * * *", "{}", 0, nil],
    )
    soonest = @db.db.get_first_row("SELECT honker_scheduler_soonest()")[0]
    assert soonest > 0, "soonest should be a future unix ts"
  end

  def test_advisory_lock_mutual_exclusion
    got = @db.db.get_first_row(
      "SELECT honker_lock_acquire(?, ?, ?)",
      ["critical", "owner-a", 60],
    )[0]
    assert_equal 1, got

    other = @db.db.get_first_row(
      "SELECT honker_lock_acquire(?, ?, ?)",
      ["critical", "owner-b", 60],
    )[0]
    assert_equal 0, other, "second owner must not acquire"

    @db.db.execute(
      "SELECT honker_lock_release(?, ?)",
      ["critical", "owner-a"],
    )
    regot = @db.db.get_first_row(
      "SELECT honker_lock_acquire(?, ?, ?)",
      ["critical", "owner-b", 60],
    )[0]
    assert_equal 1, regot, "should be re-acquirable after release"
  end

  def test_rate_limit
    3.times do
      ok = @db.db.get_first_row(
        "SELECT honker_rate_limit_try(?, ?, ?)",
        ["api", 3, 60],
      )[0]
      assert_equal 1, ok
    end
    blocked = @db.db.get_first_row(
      "SELECT honker_rate_limit_try(?, ?, ?)",
      ["api", 3, 60],
    )[0]
    assert_equal 0, blocked, "fourth call must be rate-limited"
  end

  def test_results_save_and_get
    @db.db.execute(
      "SELECT honker_result_save(?, ?, ?)",
      [42, JSON.dump({ ok: true }), 3600],
    )
    value = @db.db.get_first_row("SELECT honker_result_get(?)", [42])[0]
    assert_equal({ "ok" => true }, JSON.parse(value))

    missing = @db.db.get_first_row("SELECT honker_result_get(?)", [999])[0]
    assert_nil missing
  end
end
