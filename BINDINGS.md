# Binding Support

Honker's core is Rust. The extension owns the SQLite schema and SQL
functions. Bindings are thin language wrappers over that same shape.

This table is meant to be boring and honest. "Yes" means the feature
has a typed binding and runs in root CI. "SQL" means the extension
feature is available through raw SQL, but the language wrapper does
not expose the nice API yet.

| Binding | Package proof | Queue | Streams | Notify/listen | Scheduler | Wake behavior |
|---|---:|---:|---:|---:|---:|---|
| SQLite extension | load smoke | SQL | SQL | notify SQL only | SQL | host language must watch/read |
| Python `honker` | yes | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| Node `@russellthehippo/honker-node` | yes | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| .NET `Honker` | yes | yes | yes | yes | yes | .NET `PRAGMA data_version` poller |
| Rust `honker` | CI | yes | yes | yes | yes | shared Rust `UpdateWatcher` |
| Go | CI | yes | yes | yes | yes | Go `PRAGMA data_version` poller |
| Bun `@russellthehippo/honker-bun` | CI | yes | yes | yes | yes | Bun `PRAGMA data_version` poller |
| C++ | CI | yes | yes | yes | yes | C++ `PRAGMA data_version` poller |
| Ruby `honker` | yes | yes | yes | notify yes, listen no | yes | no async listener API yet |
| Elixir `honker` | CI | yes | yes | notify yes, listen no | yes | local update snapshots + PRAGMA polling |

## What CI Proves

- PR CI runs Rust core/extension on Linux, macOS, and Windows.
- PR CI runs Python on Linux, macOS, and Windows.
- PR CI runs Node on Linux, macOS, and Windows.
- PR CI runs .NET on Linux, macOS, and Windows.
- The aggregate Linux binding smoke runs Rust wrapper, Go, .NET
  Python interop, C++, Bun, Ruby, Elixir, and Ruby <-> Python interop.
- The packaged-install proof workflow builds and installs Python,
  Node, Ruby, and .NET packages into clean throwaway consumers.

## What Is Not Proven Yet

- Published registry installs after release. The proof workflow uses
  locally-built artifacts, which catches packaging shape but not registry
  permissions or CDN weirdness.
- Every possible cross-language pair. CI proves representative pairs
  and shared table behavior. It does not run N x N interop.
- Long soak on every OS. The scary nightly workflow soaks Linux; PR CI
  stays shorter.
- Ruby and Elixir do not yet have the same async listener API as
  Python/Node/.NET/Rust/Go/Bun/C++.
