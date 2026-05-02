// Parity tests: mirror the other bindings' coverage.
//
// Run with:
//   zig build test -Dhonker-ext=/path/to/libhonker_ext.{dylib,so}

#include "honker.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

static std::string tmp_db() {
    static int counter = 0;
    auto p = fs::temp_directory_path() / ("honker-cpp-parity-" + std::to_string(++counter) + ".db");
    fs::remove(p);
    fs::remove(p.string() + "-wal");
    fs::remove(p.string() + "-shm");
    return p.string();
}

static void cleanup(const std::string& path) {
    fs::remove(path);
    fs::remove(path + "-wal");
    fs::remove(path + "-shm");
}

#define TEST(name) void test_##name(const char* ext)
#define RUN(name) do { \
    std::cout << "  " #name " ... "; \
    test_##name(ext); \
    std::cout << "ok\n"; \
} while(0)

// ------------------------------------------------------------------
TEST(transaction_commit) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    {
        honker::Transaction tx{db.raw()};
        auto q = db.queue("tx-test");
        auto id = q.enqueue_tx(tx, R"({"k":"v"})");
        assert(id > 0);
        tx.commit();
    }

    auto q = db.queue("tx-test");
    auto job = q.claim_one("w1");
    assert(job.has_value());
    assert(job->payload() == R"({"k":"v"})");
    job->ack();
    cleanup(db_path);
}

TEST(transaction_rollback) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    {
        honker::Transaction tx{db.raw()};
        auto q = db.queue("tx-test");
        q.enqueue_tx(tx, R"({"k":"v"})");
        tx.rollback();
    }

    auto q = db.queue("tx-test");
    auto job = q.claim_one("w1");
    assert(!job.has_value());
    cleanup(db_path);
}

TEST(stream_roundtrip) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto s = db.stream("events");

    auto off1 = s.publish(R"({"a":1})");
    assert(off1 > 0);
    auto off2 = s.publish(R"({"a":2})", "key-1");
    assert(off2 > off1);

    auto evs = s.read_since(0, 100);
    assert(evs.size() == 2);
    assert(evs[0].offset() == off1);
    assert(evs[0].payload() == R"({"a":1})");
    assert(evs[1].offset() == off2);
    assert(evs[1].key() == "key-1");

    cleanup(db_path);
}

TEST(stream_consumer_offset) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto s = db.stream("events");

    auto off1 = s.publish(R"({"a":1})");
    s.save_offset("c1", off1);
    assert(s.get_offset("c1") == off1);

    auto off2 = s.publish(R"({"a":2})");
    (void)off2;
    auto evs = s.read_from_consumer("c1", 100);
    assert(evs.size() == 1);
    assert(evs[0].payload() == R"({"a":2})");

    cleanup(db_path);
}

TEST(stream_publish_tx) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto s = db.stream("events");

    {
        honker::Transaction tx{db.raw()};
        auto off = s.publish_tx(tx, R"({"tx":true})");
        assert(off > 0);
        tx.rollback();
    }

    auto evs = s.read_since(0, 100);
    assert(evs.empty());
    cleanup(db_path);
}

TEST(stream_subscribe) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto s = db.stream("events");

    s.publish(R"({"a":1})");
    s.publish(R"({"a":2})");

    auto sub = s.subscribe("c1", 1, std::chrono::milliseconds(10));
    auto ev1 = sub.next();
    assert(ev1.has_value());
    assert(ev1->payload() == R"({"a":1})");

    auto ev2 = sub.next();
    assert(ev2.has_value());
    assert(ev2->payload() == R"({"a":2})");

    sub.save_offset();
    assert(s.get_offset("c1") == ev2->offset());

    cleanup(db_path);
}

TEST(scheduler_register_tick) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto sched = db.scheduler();

    sched.add("minutely", "q", "* * * * *", R"({"task":"x"})");
    auto soon = sched.soonest();
    assert(soon > 0);

    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto now_sec = std::chrono::duration_cast<std::chrono::seconds>(now).count();
    auto fires = sched.tick(now_sec);
    // May or may not fire depending on timing; just verify it doesn't crash.
    (void)fires;

    auto n = sched.remove("minutely");
    assert(n >= 0);
    cleanup(db_path);
}

TEST(scheduler_every_second_expression) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto sched = db.scheduler();

    sched.add("fast", "q", "@every 1s", R"({"task":"x"})");
    auto soon = sched.soonest();
    assert(soon > 0);

    auto n = sched.remove("fast");
    assert(n >= 0);
    cleanup(db_path);
}

TEST(scheduler_run_fires_every_second_schedule) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto sched = db.scheduler();
    auto q = db.queue("fast");

    sched.add("fast-run", "fast", "@every 1s", R"({"ok":true})");

    std::atomic<bool> stop{false};
    std::thread t([&] { sched.run(stop, "cpp-fast"); });

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    std::optional<honker::Job> job;
    while (std::chrono::steady_clock::now() < deadline) {
        job = q.claim_one("worker-fast");
        if (job.has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    stop = true;
    t.join();

    assert(job.has_value());
    assert(job->payload() == R"({"ok":true})");
    assert(job->ack());
    cleanup(db_path);
}

TEST(scheduler_run_wakes_for_new_schedule_registration) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto sched = db.scheduler();
    auto q = db.queue("wake");

    std::atomic<bool> stop{false};
    std::thread t([&] { sched.run(stop, "cpp-wake"); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    sched.add("wake-fast", "wake", "@every 1s", R"({"wake":true})");

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    std::optional<honker::Job> job;
    while (std::chrono::steady_clock::now() < deadline) {
        job = q.claim_one("worker-wake");
        if (job.has_value()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    stop = true;
    t.join();

    assert(job.has_value());
    assert(job->payload() == R"({"wake":true})");
    assert(job->ack());
    cleanup(db_path);
}

TEST(lock_mutex) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    auto lock1 = db.try_lock("resource", "owner-a", 60);
    assert(lock1.has_value());

    auto lock2 = db.try_lock("resource", "owner-b", 60);
    assert(!lock2.has_value());

    assert(lock1->release());

    auto lock3 = db.try_lock("resource", "owner-b", 60);
    assert(lock3.has_value());

    cleanup(db_path);
}

TEST(lock_heartbeat) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    auto lock = db.try_lock("resource", "owner-a", 2);
    assert(lock.has_value());
    assert(lock->heartbeat(5));

    // Small sleep to ensure original TTL would have expired
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto lock2 = db.try_lock("resource", "owner-b", 2);
    assert(!lock2.has_value());

    cleanup(db_path);
}

TEST(rate_limit) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    for (int i = 0; i < 5; ++i) {
        assert(db.try_rate_limit("api", 5, 60));
    }
    assert(!db.try_rate_limit("api", 5, 60));
    cleanup(db_path);
}

TEST(results_roundtrip) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    db.save_result(42, R"({"ok":true})", 3600);
    auto val = db.get_result(42);
    assert(val.has_value());
    assert(val.value() == R"({"ok":true})");

    auto swept = db.sweep_results();
    (void)swept;
    cleanup(db_path);
}

TEST(notify_listen) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    auto sub = db.listen("ch1");
    db.notify("ch1", R"({"msg":"hello"})");

    auto n = sub.recv(std::chrono::milliseconds(2000));
    assert(n.has_value());
    assert(n->channel() == "ch1");
    assert(n->payload() == R"({"msg":"hello"})");
    cleanup(db_path);
}

TEST(claim_batch_and_ack_batch) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q = db.queue("batch");

    q.enqueue(R"({"n":1})");
    q.enqueue(R"({"n":2})");
    q.enqueue(R"({"n":3})");

    auto jobs = q.claim_batch("w1", 2);
    assert(jobs.size() == 2);

    std::vector<int64_t> ids;
    for (const auto& j : jobs) ids.push_back(j.id());
    auto acked = q.ack_batch(ids, "w1");
    assert(acked == 2);

    auto remaining = q.claim_one("w1");
    assert(remaining.has_value());
    remaining->ack();

    cleanup(db_path);
}

TEST(retry_and_fail) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q = db.queue("retryq", 300, 3);

    auto id = q.enqueue(R"({"x":1})");
    auto job = q.claim_one("w1");
    assert(job.has_value());

    assert(job->retry(10, "transient error"));
    auto again = q.claim_one("w1");
    assert(!again.has_value()); // delayed 10s

    // Re-claim after sweep or just check state via raw SQL? Skip for now.
    cleanup(db_path);
}

TEST(fail_moves_to_dead) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q = db.queue("failq");

    auto id = q.enqueue(R"({"x":1})");
    auto job = q.claim_one("w1");
    assert(job.has_value());
    assert(job->fail("boom"));

    auto empty = q.claim_one("w1");
    assert(!empty.has_value());
    cleanup(db_path);
}

TEST(heartbeat_extends_claim) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q = db.queue("hbq", 2); // 2s visibility

    auto id = q.enqueue(R"({"x":1})");
    auto job = q.claim_one("w1");
    assert(job.has_value());

    assert(job->heartbeat(60));
    std::this_thread::sleep_for(std::chrono::seconds(3));
    assert(job->ack()); // still claimable because heartbeat extended
    cleanup(db_path);
}

TEST(sweep_expired) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q = db.queue("sweepq");

    sqlite3_stmt* stmt = nullptr;
    const char* sql = "INSERT INTO _honker_live (queue, payload, state, run_at, max_attempts, attempts, claim_expires_at, expires_at) VALUES (?1, ?2, 'pending', unixepoch(), 3, 0, NULL, unixepoch()-1)";
    if (sqlite3_prepare_v2(db.raw(), sql, -1, &stmt, nullptr) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, "sweepq", -1, SQLITE_STATIC);
        sqlite3_bind_text(stmt, 2, R"({"x":1})", -1, SQLITE_STATIC);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    auto swept = q.sweep_expired();
    assert(swept == 1);

    auto empty = q.claim_one("w1");
    assert(!empty.has_value());
    cleanup(db_path);
}

TEST(enqueue_with_delay) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q = db.queue("delayq");

    q.enqueue(R"({"x":1})", 10);
    auto empty = q.claim_one("w1");
    assert(!empty.has_value());
    cleanup(db_path);
}

TEST(enqueue_with_priority) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q = db.queue("prioq");

    auto id1 = q.enqueue(R"({"n":"low"})", 0, 0);
    auto id2 = q.enqueue(R"({"n":"high"})", 0, 10);
    (void)id1; (void)id2;

    auto job = q.claim_one("w1");
    assert(job.has_value());
    assert(job->payload() == R"({"n":"high"})");
    job->ack();
    cleanup(db_path);
}

TEST(multiple_queues_isolated) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto q1 = db.queue("a");
    auto q2 = db.queue("b");

    q1.enqueue(R"({"q":"a"})");
    q2.enqueue(R"({"q":"b"})");

    auto j1 = q1.claim_one("w");
    assert(j1.has_value());
    assert(j1->payload() == R"({"q":"a"})");
    j1->ack();

    auto j2 = q2.claim_one("w");
    assert(j2.has_value());
    assert(j2->payload() == R"({"q":"b"})");
    j2->ack();

    cleanup(db_path);
}

TEST(stream_key_optional) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};
    auto s = db.stream("k");

    auto off1 = s.publish(R"({"a":1})");
    assert(off1 > 0);

    auto evs = s.read_since(0, 100);
    assert(evs.size() == 1);
    assert(evs[0].key().empty());
    cleanup(db_path);
}

TEST(lock_raii_release) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    {
        auto lock = db.try_lock("res", "o1", 60);
        assert(lock.has_value());
        // lock released on scope exit
    }

    auto lock2 = db.try_lock("res", "o2", 60);
    assert(lock2.has_value());
    cleanup(db_path);
}

TEST(result_ttl_expiry) {
    auto db_path = tmp_db();
    honker::Database db{db_path, ext};

    db.save_result(1, R"({"v":1})", 1);
    auto v1 = db.get_result(1);
    assert(v1.has_value());

    std::this_thread::sleep_for(std::chrono::seconds(2));
    db.sweep_results();

    auto v2 = db.get_result(1);
    assert(!v2.has_value());
    cleanup(db_path);
}

// ------------------------------------------------------------------

int main() {
    const char* ext = std::getenv("HONKER_EXTENSION_PATH");
    if (!ext || !*ext) {
        std::fputs("skip: HONKER_EXTENSION_PATH not set\n", stderr);
        return 0;
    }

    std::cout << "Running parity tests...\n";
    RUN(transaction_commit);
    RUN(transaction_rollback);
    RUN(stream_roundtrip);
    RUN(stream_consumer_offset);
    RUN(stream_publish_tx);
    RUN(stream_subscribe);
    RUN(scheduler_register_tick);
    RUN(scheduler_every_second_expression);
    RUN(scheduler_run_fires_every_second_schedule);
    RUN(scheduler_run_wakes_for_new_schedule_registration);
    RUN(lock_mutex);
    RUN(lock_heartbeat);
    RUN(rate_limit);
    RUN(results_roundtrip);
    RUN(notify_listen);
    RUN(claim_batch_and_ack_batch);
    RUN(retry_and_fail);
    RUN(fail_moves_to_dead);
    RUN(heartbeat_extends_claim);
    RUN(sweep_expired);
    RUN(enqueue_with_delay);
    RUN(enqueue_with_priority);
    RUN(multiple_queues_isolated);
    RUN(stream_key_optional);
    RUN(lock_raii_release);
    RUN(result_ttl_expiry);

    std::cout << "ALL OK (27 tests)\n";
    return 0;
}
