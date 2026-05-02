// Minimal smoke test: open, enqueue, claim, ack.
// Full test suite (Stream/Scheduler/Lock/RateLimit/etc.) is on the
// roadmap — see the session handoff.

#include "honker.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>

namespace fs = std::filesystem;

int main() {
    const char* ext = std::getenv("HONKER_EXTENSION_PATH");
    if (!ext || !*ext) {
        std::fputs(
            "skip: HONKER_EXTENSION_PATH not set "
            "(export it to ./libhonker_ext.{dylib,so})\n",
            stderr);
        return 0;
    }

    const auto tmp = fs::temp_directory_path() / "honker-cpp-test.db";
    fs::remove(tmp);
    fs::remove(tmp.string() + "-wal");
    fs::remove(tmp.string() + "-shm");

    try {
        honker::Database db{tmp.string(), ext};
        auto q = db.queue("emails");

        const auto id = q.enqueue(R"({"to":"alice@example.com"})");
        assert(id > 0 && "enqueue should return a positive id");
        std::cout << "enqueued id=" << id << '\n';

        auto job = q.claim_one("worker-1");
        assert(job.has_value() && "should claim the job");
        std::cout << "claimed payload=" << job->payload() << '\n';

        const bool acked = job->ack();
        assert(acked && "fresh claim should ack");
        std::cout << "acked\n";

        auto second = q.claim_one("worker-1");
        assert(!second.has_value() && "queue should be empty after ack");
        std::cout << "queue empty after ack\n";

    } catch (const honker::Error& e) {
        std::fprintf(stderr, "honker error: %s\n", e.what());
        return 1;
    }

    std::cout << "ALL OK\n";
    return 0;
}
