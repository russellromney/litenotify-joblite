# frozen_string_literal: true
#
# Run: bundle exec rspec spec/

require "tmpdir"
require "minitest/autorun"
require "honker"

REPO_ROOT = File.expand_path("../../..", __dir__)

def find_extension
  candidates = %w[
    target/release/libhonker_ext.dylib
    target/release/libhonker_ext.so
    target/release/libhonker_extension.dylib
    target/release/libhonker_extension.so
  ]
  candidates.each do |rel|
    p = File.join(REPO_ROOT, rel)
    return p if File.exist?(p)
  end
  nil
end

class HonkerTest < Minitest::Test
  def setup
    ext = find_extension
    skip "honker extension not built — run `cargo build -p honker-extension --release`" unless ext
    @ext = ext
    @tmpdir = Dir.mktmpdir("honker-ruby-")
    @db_path = File.join(@tmpdir, "t.db")
  end

  def teardown
    FileUtils.remove_entry(@tmpdir) if @tmpdir && File.directory?(@tmpdir)
  end

  def test_enqueue_claim_ack
    db = Honker::Database.new(@db_path, extension_path: @ext)
    q = db.queue("emails")

    id = q.enqueue({ to: "alice@example.com" })
    assert id > 0, "expected positive id"

    job = q.claim_one("worker-1")
    refute_nil job
    assert_equal id, job.id
    assert_equal "alice@example.com", job.payload["to"]
    assert job.ack, "expected ack=true for fresh claim"

    assert_nil q.claim_one("worker-1"), "queue should be empty after ack"
    db.close
  end

  def test_retry_to_dead
    db = Honker::Database.new(@db_path, extension_path: @ext)
    q = db.queue("retries", max_attempts: 2)
    q.enqueue({ i: 1 })

    job = q.claim_one("w")
    refute_nil job
    assert job.retry(delay_s: 0, error: "first")

    job2 = q.claim_one("w")
    refute_nil job2
    assert_equal 2, job2.attempts

    assert job2.retry(delay_s: 0, error: "second")
    assert_nil q.claim_one("w"), "second retry should send to dead"

    row = db.db.get_first_row("SELECT COUNT(*) FROM _honker_dead WHERE queue='retries'")
    assert_equal 1, row[0]
    db.close
  end

  def test_notify
    db = Honker::Database.new(@db_path, extension_path: @ext)
    id = db.notify("orders", { id: 42 })
    assert id > 0

    row = db.db.get_first_row(
      "SELECT channel, payload FROM _honker_notifications WHERE id = ?",
      [id],
    )
    assert_equal "orders", row[0]
    assert_equal({ "id" => 42 }, JSON.parse(row[1]))
    db.close
  end
end
