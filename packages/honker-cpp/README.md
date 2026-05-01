# honker-cpp

C++17 binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Requirements

- Zig 0.15+
- C++17 compiler
- SQLite development headers / library
- nlohmann-json headers
- Honker SQLite extension

The C++ binding loads the Honker SQLite extension itself. That means SQLite must be built with loadable extension support.

Platform installs:

```bash
# macOS with Homebrew
brew install sqlite nlohmann-json

# macOS with MacPorts
sudo port install sqlite3 nlohmann-json

# Ubuntu / Debian
sudo apt-get install libsqlite3-dev nlohmann-json3-dev

# Fedora
sudo dnf install sqlite-devel json-devel

# Arch
sudo pacman -S sqlite nlohmann-json
```

Apple's system SQLite headers do not expose the load-extension API. On macOS, pass the package-manager prefix:

```bash
zig build test \
  -Dsqlite-prefix="$(brew --prefix sqlite)" \
  -Djson-prefix="$(brew --prefix nlohmann-json)" \
  -Dhonker-ext=/path/to/libhonker_ext.dylib
```

## Quick start

```cpp
#include "honker.hpp"

int main() {
    honker::Database db{"app.db", "./libhonker_ext.dylib"};
    auto q = db.queue("emails");

    q.enqueue(R"({"to":"alice@example.com"})");

    if (auto job = q.claim_one("worker-1")) {
        send_email(job->payload);
        job->ack();
    }
}
```

Delayed jobs use `run_at` options on enqueue. Recurring schedules use schedule expressions:

```cpp
auto sched = db.scheduler();
sched.add("fast", "emails", "@every 1s", R"({"kind":"tick"})");
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

For full API docs, streams, notify/listen, and SQL details, see the main repo and docs site.
