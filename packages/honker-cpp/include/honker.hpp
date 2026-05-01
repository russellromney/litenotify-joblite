// honker.hpp — C++17 RAII wrappers around the Zig-implemented C ABI.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

extern "C" {
    #include <sqlite3.h>

    int32_t honker_cpp_open(const char* path, const char* ext_path, sqlite3** out_db);
    void    honker_cpp_close(sqlite3* db);

    int32_t honker_cpp_begin_tx(sqlite3* db);
    int32_t honker_cpp_commit_tx(sqlite3* db);
    int32_t honker_cpp_rollback_tx(sqlite3* db);

    int64_t honker_cpp_enqueue(
        sqlite3* db,
        const char* queue,
        const char* payload_json,
        int64_t delay_sec,
        int64_t priority,
        int64_t max_attempts);

    char*   honker_cpp_claim_one(
        sqlite3* db,
        const char* queue,
        const char* worker_id,
        int64_t visibility_timeout_s);
    char*   honker_cpp_claim_batch(
        sqlite3* db,
        const char* queue,
        const char* worker_id,
        int64_t n,
        int64_t visibility_timeout_s);

    int64_t honker_cpp_ack(sqlite3* db, int64_t job_id, const char* worker_id);
    int64_t honker_cpp_ack_batch(sqlite3* db, const char* ids_json, const char* worker_id);
    int64_t honker_cpp_retry(sqlite3* db, int64_t job_id, const char* worker_id,
                             int64_t delay_sec, const char* error);
    int64_t honker_cpp_fail(sqlite3* db, int64_t job_id, const char* worker_id,
                            const char* error);
    int64_t honker_cpp_heartbeat(sqlite3* db, int64_t job_id, const char* worker_id,
                                  int64_t extend_sec);
    int64_t honker_cpp_sweep_expired(sqlite3* db, const char* queue);

    int64_t honker_cpp_stream_publish(
        sqlite3* db, const char* topic, const char* key, const char* payload_json);
    char*   honker_cpp_stream_read_since(
        sqlite3* db, const char* topic, int64_t offset, int64_t limit);
    int64_t honker_cpp_stream_save_offset(
        sqlite3* db, const char* consumer, const char* topic, int64_t offset);
    int64_t honker_cpp_stream_get_offset(
        sqlite3* db, const char* consumer, const char* topic);

    int64_t honker_cpp_scheduler_register(
        sqlite3* db, const char* name, const char* queue, const char* cron,
        const char* payload_json, int64_t priority, int64_t expires_sec);
    int64_t honker_cpp_scheduler_unregister(sqlite3* db, const char* name);
    char*   honker_cpp_scheduler_tick(sqlite3* db, int64_t now_unix);
    int64_t honker_cpp_scheduler_soonest(sqlite3* db);

    int64_t honker_cpp_lock_acquire(
        sqlite3* db, const char* name, const char* owner, int64_t ttl_sec);
    int64_t honker_cpp_lock_release(
        sqlite3* db, const char* name, const char* owner);
    int64_t honker_cpp_lock_heartbeat(
        sqlite3* db, const char* name, const char* owner, int64_t ttl_sec);

    int64_t honker_cpp_rate_limit_try(
        sqlite3* db, const char* name, int64_t limit, int64_t per_sec);

    int64_t honker_cpp_result_save(
        sqlite3* db, int64_t job_id, const char* value_json, int64_t ttl_sec);
    char*   honker_cpp_result_get(sqlite3* db, int64_t job_id);
    int64_t honker_cpp_result_sweep(sqlite3* db);

    int64_t honker_cpp_notify(
        sqlite3* db, const char* channel, const char* payload_json);

    void    honker_cpp_free(char* ptr);
}

namespace honker {

// =====================================================================
// Error
// =====================================================================

class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// =====================================================================
// Forward declarations
// =====================================================================

class Database;
class Transaction;
class Job;
class Queue;
class StreamEvent;
class Stream;
class StreamSubscription;
class ScheduledFire;
class Scheduler;
class Lock;
class Notification;
class Subscription;

// =====================================================================
// Database
// =====================================================================

class Database {
public:
    Database(std::string_view path, std::string_view ext_path) {
        const std::string p{path};
        const std::string e{ext_path};
        if (auto rc = honker_cpp_open(p.c_str(), e.c_str(), &db_); rc != 0) {
            throw Error{"honker_cpp_open failed: code " + std::to_string(rc)};
        }
        path_ = p;
    }

    ~Database() { close(); }

    Database(Database&& other) noexcept
        : db_(other.db_), path_(std::move(other.path_)),
          update_watcher_(std::move(other.update_watcher_)),
          update_watcher_active_(other.update_watcher_active_.load()) {
        other.db_ = nullptr;
        other.update_watcher_active_ = false;
    }

    Database& operator=(Database&& other) noexcept {
        if (this != &other) {
            close();
            db_ = other.db_;
            path_ = std::move(other.path_);
            update_watcher_ = std::move(other.update_watcher_);
            update_watcher_active_ = other.update_watcher_active_.load();
            other.db_ = nullptr;
            other.update_watcher_active_ = false;
        }
        return *this;
    }

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    Queue queue(std::string_view name,
                int64_t visibility_timeout_s = 300,
                int64_t max_attempts = 3);

    Stream stream(std::string_view name);

    Scheduler scheduler();

    std::optional<Lock> try_lock(std::string_view name,
                                  std::string_view owner,
                                  int64_t ttl_sec = 60);

    bool try_rate_limit(std::string_view name, int64_t limit, int64_t per_sec);

    int64_t save_result(int64_t job_id, std::string_view value_json, int64_t ttl_sec = 0);
    std::optional<std::string> get_result(int64_t job_id);
    int64_t sweep_results();

    int64_t notify(std::string_view channel, std::string_view payload_json);

    Subscription listen(std::string_view channel);

    sqlite3* raw() const noexcept { return db_; }
    const std::string& path() const noexcept { return path_; }

    // Internal: start/stop update watcher thread for stream subscriptions.
    void start_update_watcher();
    void stop_update_watcher();
    void wait_update(std::chrono::milliseconds timeout);
    void mark_updated();

private:
    void close() {
        if (db_) {
            stop_update_watcher();
            honker_cpp_close(db_);
            db_ = nullptr;
        }
    }

    sqlite3* db_ = nullptr;
    std::string path_;
    std::thread update_watcher_;
    std::atomic<bool> update_watcher_active_{false};
    std::mutex update_mtx_;
    std::condition_variable update_cv_;
    bool update_changed_ = false;
};

// =====================================================================
// Transaction
// =====================================================================

class Transaction {
public:
    explicit     Transaction(sqlite3* db) : db_(db) {
        if (auto rc = honker_cpp_begin_tx(db_); rc != 0) {
            throw Error{"begin_tx failed: code " + std::to_string(rc)};
        }
    }

    ~Transaction() {
        if (!done_) {
            honker_cpp_rollback_tx(db_);
        }
    }

    void commit() {
        if (done_) return;
        if (auto rc = honker_cpp_commit_tx(db_); rc != 0) {
            throw Error{"commit failed: code " + std::to_string(rc)};
        }
        done_ = true;
    }

    void rollback() {
        if (done_) return;
        if (auto rc = honker_cpp_rollback_tx(db_); rc != 0) {
            throw Error{"rollback failed: code " + std::to_string(rc)};
        }
        done_ = true;
    }

    sqlite3* raw() const noexcept { return db_; }

    Transaction(const Transaction&) = delete;
    Transaction& operator=(const Transaction&) = delete;
    Transaction(Transaction&&) = delete;
    Transaction& operator=(Transaction&&) = delete;

private:
    sqlite3* db_;
    bool done_ = false;
};

// =====================================================================
// Job
// =====================================================================

class Job {
public:
    int64_t     id()        const noexcept { return id_; }
    std::string payload()   const noexcept { return payload_; }
    std::string worker_id() const noexcept { return worker_id_; }
    int64_t     attempts()  const noexcept { return attempts_; }

    bool ack() {
        const auto n = honker_cpp_ack(db_, id_, worker_id_.c_str());
        if (n < 0) throw Error{"ack failed: SQL error"};
        return n > 0;
    }

    bool retry(int64_t delay_sec, std::string_view error) {
        const std::string e{error};
        const auto n = honker_cpp_retry(db_, id_, worker_id_.c_str(), delay_sec, e.c_str());
        if (n < 0) throw Error{"retry failed: SQL error"};
        return n > 0;
    }

    bool fail(std::string_view error) {
        const std::string e{error};
        const auto n = honker_cpp_fail(db_, id_, worker_id_.c_str(), e.c_str());
        if (n < 0) throw Error{"fail failed: SQL error"};
        return n > 0;
    }

    bool heartbeat(int64_t extend_sec) {
        const auto n = honker_cpp_heartbeat(db_, id_, worker_id_.c_str(), extend_sec);
        if (n < 0) throw Error{"heartbeat failed: SQL error"};
        return n > 0;
    }

    Job(sqlite3* db, int64_t id, std::string payload,
        std::string worker_id, int64_t attempts)
        : db_(db), id_(id), payload_(std::move(payload)),
          worker_id_(std::move(worker_id)), attempts_(attempts) {}

private:
    sqlite3*    db_;
    int64_t     id_;
    std::string payload_;
    std::string worker_id_;
    int64_t     attempts_;
};

// =====================================================================
// Queue
// =====================================================================

class Queue {
public:
    int64_t enqueue(std::string_view payload_json,
                    int64_t delay_sec = 0,
                    int64_t priority = 0) {
        const std::string p{payload_json};
        const auto id = honker_cpp_enqueue(
            db_, name_.c_str(), p.c_str(),
            delay_sec, priority, max_attempts_);
        if (id < 0) throw Error{"enqueue failed: code " + std::to_string(id)};
        return id;
    }

    int64_t enqueue_tx(Transaction& tx, std::string_view payload_json,
                       int64_t delay_sec = 0,
                       int64_t priority = 0) {
        const std::string p{payload_json};
        const auto id = honker_cpp_enqueue(
            tx.raw(), name_.c_str(), p.c_str(),
            delay_sec, priority, max_attempts_);
        if (id < 0) throw Error{"enqueue_tx failed: code " + std::to_string(id)};
        return id;
    }

    std::optional<Job> claim_one(std::string_view worker_id) {
        const std::string w{worker_id};
        char* rows = honker_cpp_claim_one(
            db_, name_.c_str(), w.c_str(), visibility_timeout_s_);
        if (!rows) return std::nullopt;
        return parse_jobs(rows, 1);
    }

    std::vector<Job> claim_batch(std::string_view worker_id, int64_t n) {
        const std::string w{worker_id};
        char* rows = honker_cpp_claim_batch(
            db_, name_.c_str(), w.c_str(), n, visibility_timeout_s_);
        if (!rows) return {};
        std::string json{rows};
        honker_cpp_free(rows);
        std::vector<Job> out;
        try {
            auto arr = nlohmann::json::parse(json);
            if (!arr.is_array()) return out;
            for (const auto& j : arr) {
                int64_t id = j.value("id", 0);
                std::string payload = j.value("payload", "");
                std::string wid = j.value("worker_id", w);
                int64_t attempts = j.value("attempts", 1);
                out.emplace_back(db_, id, std::move(payload), std::move(wid), attempts);
            }
        } catch (...) {}
        return out;
    }

    int64_t ack_batch(const std::vector<int64_t>& ids, std::string_view worker_id) {
        nlohmann::json j = ids;
        const std::string ids_json = j.dump();
        const std::string w{worker_id};
        const auto n = honker_cpp_ack_batch(db_, ids_json.c_str(), w.c_str());
        if (n < 0) throw Error{"ack_batch failed: SQL error"};
        return n;
    }

    int64_t sweep_expired() {
        const auto n = honker_cpp_sweep_expired(db_, name_.c_str());
        if (n < 0) throw Error{"sweep_expired failed: SQL error"};
        return n;
    }

    int64_t save_result(int64_t job_id, std::string_view value_json, int64_t ttl_sec = 0) {
        const std::string v{value_json};
        const auto n = honker_cpp_result_save(db_, job_id, v.c_str(), ttl_sec);
        if (n < 0) throw Error{"save_result failed: SQL error"};
        return n;
    }

    Queue(sqlite3* db, std::string name, int64_t vis, int64_t max)
        : db_(db), name_(std::move(name)),
          visibility_timeout_s_(vis), max_attempts_(max) {}

private:
    std::optional<Job> parse_jobs(char* rows, int64_t) {
        std::string json{rows};
        honker_cpp_free(rows);
        try {
            auto arr = nlohmann::json::parse(json);
            if (!arr.is_array() || arr.empty()) return std::nullopt;
            const auto& j = arr[0];
            int64_t id = j.value("id", 0);
            std::string payload = j.value("payload", "");
            std::string wid = j.value("worker_id", "");
            int64_t attempts = j.value("attempts", 1);
            return Job{db_, id, std::move(payload), std::move(wid), attempts};
        } catch (...) {
            return std::nullopt;
        }
    }

    sqlite3*    db_;
    std::string name_;
    int64_t     visibility_timeout_s_;
    int64_t     max_attempts_;
};

// =====================================================================
// StreamEvent
// =====================================================================

class StreamEvent {
public:
    int64_t     offset()    const noexcept { return offset_; }
    std::string topic()     const noexcept { return topic_; }
    std::string key()       const noexcept { return key_; }
    std::string payload()   const noexcept { return payload_; }
    int64_t     created_at() const noexcept { return created_at_; }

    StreamEvent(int64_t offset, std::string topic, std::string key,
                std::string payload, int64_t created_at)
        : offset_(offset), topic_(std::move(topic)), key_(std::move(key)),
          payload_(std::move(payload)), created_at_(created_at) {}

private:
    int64_t     offset_;
    std::string topic_;
    std::string key_;
    std::string payload_;
    int64_t     created_at_;
};

// =====================================================================
// Stream
// =====================================================================

class Stream {
public:
    int64_t publish(std::string_view payload_json, std::optional<std::string_view> key = std::nullopt) {
        const std::string p{payload_json};
        const std::string k = key ? std::string{*key} : "";
        const auto off = honker_cpp_stream_publish(
            db_, name_.c_str(), key ? k.c_str() : nullptr, p.c_str());
        if (off < 0) throw Error{"stream_publish failed: SQL error"};
        return off;
    }

    int64_t publish_tx(Transaction& tx, std::string_view payload_json,
                       std::optional<std::string_view> key = std::nullopt) {
        const std::string p{payload_json};
        const std::string k = key ? std::string{*key} : "";
        const auto off = honker_cpp_stream_publish(
            tx.raw(), name_.c_str(), key ? k.c_str() : nullptr, p.c_str());
        if (off < 0) throw Error{"stream_publish_tx failed: SQL error"};
        return off;
    }

    std::vector<StreamEvent> read_since(int64_t offset, int64_t limit = 1000) {
        char* rows = honker_cpp_stream_read_since(db_, name_.c_str(), offset, limit);
        if (!rows) return {};
        return parse_events(rows);
    }

    std::vector<StreamEvent> read_from_consumer(std::string_view consumer, int64_t limit = 1000) {
        const auto offset = get_offset(consumer);
        return read_since(offset, limit);
    }

    bool save_offset(std::string_view consumer, int64_t offset) {
        const std::string c{consumer};
        const auto n = honker_cpp_stream_save_offset(db_, c.c_str(), name_.c_str(), offset);
        if (n < 0) throw Error{"save_offset failed: SQL error"};
        return n > 0;
    }

    bool save_offset_tx(Transaction& tx, std::string_view consumer, int64_t offset) {
        const std::string c{consumer};
        const auto n = honker_cpp_stream_save_offset(tx.raw(), c.c_str(), name_.c_str(), offset);
        if (n < 0) throw Error{"save_offset_tx failed: SQL error"};
        return n > 0;
    }

    int64_t get_offset(std::string_view consumer) {
        const std::string c{consumer};
        return honker_cpp_stream_get_offset(db_, c.c_str(), name_.c_str());
    }

    StreamSubscription subscribe(std::string_view consumer,
                                  int64_t save_every_n = 1000,
                                  std::chrono::milliseconds poll_interval = std::chrono::milliseconds(100));

    Stream(sqlite3* db, std::string name)
        : db_(db), name_(std::move(name)) {}

private:
    std::vector<StreamEvent> parse_events(char* rows) {
        std::string json{rows};
        honker_cpp_free(rows);
        std::vector<StreamEvent> out;
        try {
            auto arr = nlohmann::json::parse(json);
            if (!arr.is_array()) return out;
            for (const auto& j : arr) {
                std::string key = (j.contains("key") && !j["key"].is_null())
                    ? j["key"].get<std::string>() : "";
                out.emplace_back(
                    j.value("offset", 0),
                    j.value("topic", ""),
                    key,
                    j.value("payload", ""),
                    j.value("created_at", 0)
                );
            }
        } catch (...) {}
        return out;
    }

    sqlite3*    db_;
    std::string name_;
};

// =====================================================================
// StreamSubscription
// =====================================================================

class StreamSubscription {
public:
    StreamSubscription(sqlite3* db, std::string topic, std::string consumer,
                       int64_t save_every_n, std::chrono::milliseconds poll_interval)
        : db_(db), topic_(std::move(topic)), consumer_(std::move(consumer)),
          save_every_n_(save_every_n), poll_interval_(poll_interval) {
        last_offset_ = honker_cpp_stream_get_offset(db_, consumer_.c_str(), topic_.c_str());
    }

    ~StreamSubscription() {
        if (pending_ > 0) {
            try { flush_offset(); } catch (...) {}
        }
    }

    std::optional<StreamEvent> next() {
        while (true) {
            if (idx_ < buffer_.size()) {
                auto ev = std::move(buffer_[idx_]);
                ++idx_;
                ++pending_;
                last_offset_ = ev.offset();
                if (pending_ >= save_every_n_) {
                    flush_offset();
                }
                return ev;
            }
            buffer_ = read_batch();
            idx_ = 0;
            if (buffer_.empty()) {
                std::this_thread::sleep_for(poll_interval_);
            }
        }
    }

    void save_offset() {
        flush_offset();
    }

    int64_t offset() const noexcept { return last_offset_; }

private:
    std::vector<StreamEvent> read_batch() {
        char* rows = honker_cpp_stream_read_since(db_, topic_.c_str(), last_offset_, 100);
        if (!rows) return {};
        std::string json{rows};
        honker_cpp_free(rows);
        std::vector<StreamEvent> out;
        try {
            auto arr = nlohmann::json::parse(json);
            if (!arr.is_array()) return out;
            for (const auto& j : arr) {
                std::string key = (j.contains("key") && !j["key"].is_null())
                    ? j["key"].get<std::string>() : "";
                out.emplace_back(
                    j.value("offset", 0),
                    j.value("topic", ""),
                    key,
                    j.value("payload", ""),
                    j.value("created_at", 0)
                );
            }
        } catch (...) {}
        return out;
    }

    void flush_offset() {
        if (pending_ == 0) return;
        honker_cpp_stream_save_offset(db_, consumer_.c_str(), topic_.c_str(), last_offset_);
        pending_ = 0;
    }

    sqlite3*    db_;
    std::string topic_;
    std::string consumer_;
    int64_t     save_every_n_;
    std::chrono::milliseconds poll_interval_;
    int64_t     last_offset_ = 0;
    int64_t     pending_ = 0;
    std::vector<StreamEvent> buffer_;
    std::size_t idx_ = 0;
};

// =====================================================================
// ScheduledFire
// =====================================================================

class ScheduledFire {
public:
    std::string name()      const noexcept { return name_; }
    std::string queue()     const noexcept { return queue_; }
    int64_t     fire_at()   const noexcept { return fire_at_; }
    int64_t     job_id()    const noexcept { return job_id_; }

    ScheduledFire(std::string name, std::string queue, int64_t fire_at, int64_t job_id)
        : name_(std::move(name)), queue_(std::move(queue)),
          fire_at_(fire_at), job_id_(job_id) {}

private:
    std::string name_;
    std::string queue_;
    int64_t     fire_at_;
    int64_t     job_id_;
};

// =====================================================================
// Scheduler
// =====================================================================

class Scheduler {
public:
    void add(std::string_view name, std::string_view queue, std::string_view schedule_expr,
             std::string_view payload_json, int64_t priority = 0,
             std::optional<int64_t> expires_sec = std::nullopt) {
        const std::string n{name};
        const std::string q{queue};
        const std::string c{schedule_expr};
        const std::string p{payload_json};
        const auto rc = honker_cpp_scheduler_register(
            db_->raw(), n.c_str(), q.c_str(), c.c_str(), p.c_str(), priority,
            expires_sec.value_or(0));
        if (rc < 0) throw Error{"scheduler_register failed: SQL error"};
        db_->mark_updated();
    }

    int64_t remove(std::string_view name) {
        const std::string n{name};
        const auto rc = honker_cpp_scheduler_unregister(db_->raw(), n.c_str());
        if (rc < 0) throw Error{"scheduler_unregister failed: SQL error"};
        db_->mark_updated();
        return rc;
    }

    std::vector<ScheduledFire> tick(int64_t now_unix) {
        char* rows = honker_cpp_scheduler_tick(db_->raw(), now_unix);
        if (!rows) return {};
        return parse_fires(rows);
    }

    int64_t soonest() {
        return honker_cpp_scheduler_soonest(db_->raw());
    }

    void run(std::atomic<bool>& stop_token, std::string_view owner) {
        constexpr int64_t LOCK_TTL = 60;
        constexpr auto HEARTBEAT = std::chrono::seconds(20);
        const std::string o{owner};
        db_->start_update_watcher();

        while (!stop_token.load()) {
            auto acquired = honker_cpp_lock_acquire(
                db_->raw(), "honker-scheduler", o.c_str(), LOCK_TTL);
            if (acquired <= 0) {
                db_->wait_update(std::chrono::seconds(5));
                continue;
            }

            auto last_hb = std::chrono::steady_clock::now();
            while (!stop_token.load()) {
                auto now = std::chrono::system_clock::now().time_since_epoch();
                auto now_sec = std::chrono::duration_cast<std::chrono::seconds>(now).count();
                auto fires = tick(now_sec);
                (void)fires; // caller may process them if they override run()

                auto now_clock = std::chrono::steady_clock::now();
                if (now_clock - last_hb >= HEARTBEAT) {
                    auto hb = honker_cpp_lock_heartbeat(
                        db_->raw(), "honker-scheduler", o.c_str(), LOCK_TTL);
                    if (hb <= 0) {
                        // lost leadership
                        break;
                    }
                    last_hb = now_clock;
                }

                auto wait = HEARTBEAT - (now_clock - last_hb);
                auto next_fire = soonest();
                if (next_fire > 0) {
                    auto fire_tp = std::chrono::system_clock::time_point{
                        std::chrono::seconds(next_fire)
                    };
                    auto until_fire = fire_tp - std::chrono::system_clock::now();
                    if (until_fire < std::chrono::seconds(0)) {
                        wait = std::chrono::seconds(0);
                    } else {
                        auto until_fire_ms = std::chrono::duration_cast<std::chrono::milliseconds>(until_fire);
                        auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(wait);
                        if (until_fire_ms < wait_ms) wait = until_fire_ms;
                    }
                }
                db_->wait_update(std::chrono::duration_cast<std::chrono::milliseconds>(wait));
            }

            honker_cpp_lock_release(db_->raw(), "honker-scheduler", o.c_str());
        }
    }

    Scheduler(Database* db) : db_(db) {}

private:
    std::vector<ScheduledFire> parse_fires(char* rows) {
        std::string json{rows};
        honker_cpp_free(rows);
        std::vector<ScheduledFire> out;
        try {
            auto arr = nlohmann::json::parse(json);
            if (!arr.is_array()) return out;
            for (const auto& j : arr) {
                out.emplace_back(
                    j.value("name", ""),
                    j.value("queue", ""),
                    j.value("fire_at", 0),
                    j.value("job_id", 0)
                );
            }
        } catch (...) {}
        return out;
    }

    Database* db_;
};

// =====================================================================
// Lock
// =====================================================================

class Lock {
public:
    std::string name()  const noexcept { return name_; }
    std::string owner() const noexcept { return owner_; }

    bool release() {
        if (released_) return true;
        const auto n = honker_cpp_lock_release(db_, name_.c_str(), owner_.c_str());
        if (n < 0) throw Error{"lock_release failed: SQL error"};
        released_ = true;
        return n > 0;
    }

    bool heartbeat(int64_t ttl_sec) {
        if (released_) return false;
        const auto n = honker_cpp_lock_heartbeat(db_, name_.c_str(), owner_.c_str(), ttl_sec);
        if (n < 0) throw Error{"lock_heartbeat failed: SQL error"};
        return n > 0;
    }

    ~Lock() {
        if (!released_) {
            try { release(); } catch (...) {}
        }
    }

    Lock(const Lock&) = delete;
    Lock& operator=(const Lock&) = delete;
    Lock(Lock&& other) noexcept
        : db_(other.db_), name_(std::move(other.name_)),
          owner_(std::move(other.owner_)), released_(other.released_) {
        other.released_ = true;
    }
    Lock& operator=(Lock&& other) noexcept {
        if (this != &other) {
            if (!released_) { try { release(); } catch (...) {} }
            db_ = other.db_;
            name_ = std::move(other.name_);
            owner_ = std::move(other.owner_);
            released_ = other.released_;
            other.released_ = true;
        }
        return *this;
    }

private:
    friend class Database;
    Lock(sqlite3* db, std::string name, std::string owner)
        : db_(db), name_(std::move(name)), owner_(std::move(owner)) {}

    sqlite3*    db_;
    std::string name_;
    std::string owner_;
    bool        released_ = false;
};

// =====================================================================
// Notification
// =====================================================================

class Notification {
public:
    int64_t     id()       const noexcept { return id_; }
    std::string channel()  const noexcept { return channel_; }
    std::string payload()  const noexcept { return payload_; }

    Notification(int64_t id, std::string channel, std::string payload)
        : id_(id), channel_(std::move(channel)), payload_(std::move(payload)) {}

private:
    int64_t     id_;
    std::string channel_;
    std::string payload_;
};

// =====================================================================
// Subscription (listen)
// =====================================================================

class Subscription {
public:
    std::optional<Notification> recv(std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            char* rows = nullptr;
            {
                const std::string c{channel_};
                const std::string sql =
                    "SELECT id, channel, payload FROM _honker_notifications "
                    "WHERE channel = ? AND id > ? ORDER BY id LIMIT 1";
                sqlite3_stmt* stmt = nullptr;
                if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
                    sqlite3_bind_text(stmt, 1, c.c_str(), -1, SQLITE_STATIC);
                    sqlite3_bind_int64(stmt, 2, last_id_);
                if (sqlite3_step(stmt) == SQLITE_ROW) {
                    int64_t id = sqlite3_column_int64(stmt, 0);
                    std::string ch = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
                    std::string pl = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
                    last_id_ = id;
                    sqlite3_finalize(stmt);
                    return Notification{id, std::move(ch), std::move(pl)};
                }
                sqlite3_finalize(stmt);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return std::nullopt;
    }

private:
    friend class Database;
    Subscription(sqlite3* db, std::string channel)
        : db_(db), channel_(std::move(channel)) {
        // Attach at current max id so we only see new notifications.
        const std::string c{channel_};
        const std::string sql = "SELECT COALESCE(MAX(id), 0) FROM _honker_notifications WHERE channel = ?";
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, c.c_str(), -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                last_id_ = sqlite3_column_int64(stmt, 0);
            }
            sqlite3_finalize(stmt);
        }
    }

    sqlite3*    db_;
    std::string channel_;
    int64_t     last_id_ = 0;
};

// =====================================================================
// Database inline implementations
// =====================================================================

inline Queue Database::queue(std::string_view name,
                             int64_t visibility_timeout_s,
                             int64_t max_attempts) {
    return Queue{db_, std::string{name}, visibility_timeout_s, max_attempts};
}

inline Stream Database::stream(std::string_view name) {
    return Stream{db_, std::string{name}};
}

inline Scheduler Database::scheduler() {
    return Scheduler{this};
}

inline std::optional<Lock> Database::try_lock(std::string_view name,
                                               std::string_view owner,
                                               int64_t ttl_sec) {
    const std::string n{name};
    const std::string o{owner};
    const auto rc = honker_cpp_lock_acquire(db_, n.c_str(), o.c_str(), ttl_sec);
    if (rc < 0) throw Error{"lock_acquire failed: SQL error"};
    if (rc == 0) return std::nullopt;
    return Lock{db_, n, o};
}

inline bool Database::try_rate_limit(std::string_view name, int64_t limit, int64_t per_sec) {
    const std::string n{name};
    const auto rc = honker_cpp_rate_limit_try(db_, n.c_str(), limit, per_sec);
    if (rc < 0) throw Error{"rate_limit_try failed: SQL error"};
    return rc > 0;
}

inline int64_t Database::save_result(int64_t job_id, std::string_view value_json, int64_t ttl_sec) {
    const std::string v{value_json};
    const auto rc = honker_cpp_result_save(db_, job_id, v.c_str(), ttl_sec);
    if (rc < 0) throw Error{"save_result failed: SQL error"};
    return rc;
}

inline std::optional<std::string> Database::get_result(int64_t job_id) {
    char* ptr = honker_cpp_result_get(db_, job_id);
    if (!ptr) return std::nullopt;
    std::string val{ptr};
    honker_cpp_free(ptr);
    return val;
}

inline int64_t Database::sweep_results() {
    return honker_cpp_result_sweep(db_);
}

inline int64_t Database::notify(std::string_view channel, std::string_view payload_json) {
    const std::string c{channel};
    const std::string p{payload_json};
    const auto rc = honker_cpp_notify(db_, c.c_str(), p.c_str());
    if (rc < 0) throw Error{"notify failed: SQL error"};
    return rc;
}

inline Subscription Database::listen(std::string_view channel) {
    return Subscription{db_, std::string{channel}};
}

// =====================================================================
// Database commit watcher (internal)
// =====================================================================

// Platform-specific file identity for the dead-man's switch.
#if defined(_WIN32)
inline bool file_identity(const std::string& path, uint64_t& dev, uint64_t& ino) {
    // Windows: std::filesystem doesn't expose volume serial or file index
    // cheaply. Best-effort: return zeros so the identity check is a no-op.
    (void)path;
    dev = 0;
    ino = 0;
    return true;
}
#else
#include <sys/stat.h>
inline bool file_identity(const std::string& path, uint64_t& dev, uint64_t& ino) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) return false;
    dev = static_cast<uint64_t>(st.st_dev);
    ino = static_cast<uint64_t>(st.st_ino);
    return true;
}
#endif

inline void Database::start_update_watcher() {
    if (update_watcher_active_.exchange(true)) return;
    update_watcher_ = std::thread([this]() {
        uint64_t init_dev = 0, init_ino = 0;
        file_identity(path_, init_dev, init_ino);

        // Seed initial data_version.
        uint32_t last_version = 0;
        auto query_dv = [this](uint32_t& out) -> bool {
            sqlite3_stmt* stmt = nullptr;
            if (sqlite3_prepare_v2(db_, "PRAGMA data_version", -1, &stmt, nullptr) != SQLITE_OK)
                return false;
            bool ok = false;
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                out = static_cast<uint32_t>(sqlite3_column_int(stmt, 0));
                ok = true;
            }
            sqlite3_finalize(stmt);
            return ok;
        };
        query_dv(last_version);

        uint64_t tick = 0;
        while (update_watcher_active_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));

            // Path 1: PRAGMA data_version (fast path)
            uint32_t version = 0;
            if (query_dv(version)) {
                if (version != last_version) {
                    last_version = version;
                    {
                        std::lock_guard<std::mutex> lk(update_mtx_);
                        update_changed_ = true;
                    }
                    update_cv_.notify_all();
                }
            } else {
                // Transient failure — force conservative wake
                {
                    std::lock_guard<std::mutex> lk(update_mtx_);
                    update_changed_ = true;
                }
                update_cv_.notify_all();
            }

            // Path 2: stat identity check (dead-man's switch)
            if (++tick % 100 == 0) {
                uint64_t dev = 0, ino = 0;
                if (!file_identity(path_, dev, ino)) {
                    // File vanished — force wake, let caller recover
                    {
                        std::lock_guard<std::mutex> lk(update_mtx_);
                        update_changed_ = true;
                    }
                    update_cv_.notify_all();
                } else if (dev != init_dev || ino != init_ino) {
                    throw std::runtime_error(
                        "honker: database file replaced (dev=" +
                        std::to_string(init_dev) + "->" + std::to_string(dev) +
                        ", ino=" + std::to_string(init_ino) + "->" + std::to_string(ino) +
                        ") at " + path_ + ". Restart required."
                    );
                }
            }
        }
    });
}

inline void Database::stop_update_watcher() {
    if (!update_watcher_active_.exchange(false)) return;
    update_cv_.notify_all();
    if (update_watcher_.joinable()) update_watcher_.join();
}

inline void Database::wait_update(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(update_mtx_);
    update_cv_.wait_for(lk, timeout, [this]() { return update_changed_ || !update_watcher_active_.load(); });
    update_changed_ = false;
}

inline void Database::mark_updated() {
    {
        std::lock_guard<std::mutex> lk(update_mtx_);
        update_changed_ = true;
    }
    update_cv_.notify_all();
}

// =====================================================================
// Stream inline implementations
// =====================================================================

inline StreamSubscription Stream::subscribe(std::string_view consumer,
                                             int64_t save_every_n,
                                             std::chrono::milliseconds poll_interval) {
    return StreamSubscription{db_, name_, std::string{consumer}, save_every_n, poll_interval};
}

} // namespace honker
