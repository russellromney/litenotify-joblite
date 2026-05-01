# honker-dotnet

Early .NET / C# binding for Honker.

Install:

```bash
dotnet add package Honker
```

Current shape:

- thin wrapper over the SQLite loadable extension
- `Database.Open(...)` loads `honker-extension` and runs
  `honker_bootstrap()`
- typed `Queue`, `Stream`, `Outbox`, `Scheduler`, `Job`, and lock wrappers
- async claim / listen / subscribe / outbox worker loops with
  `CancellationToken`
- local `PRAGMA data_version` polling for update wakes

Current status:

- queue enqueue / claim / ack / retry / fail / heartbeat / results are wired
- stream publish / read / subscribe / offset persistence are wired
- notify / listen, advisory locks, transactional outbox, and rate limits are wired
- scheduler add / remove / tick / soonest / run are wired
- delayed-claim wake and `@every` schedule support are implemented in
  the binding, but tests only exercise them when the underlying
  extension build exposes the corresponding SQL functions

Native loading:

- `Database.Open(...)` first honors `OpenOptions.ExtensionPath`
- then `HONKER_EXTENSION_PATH`
- then it looks for the bundled native extension from the NuGet package
  in the app output root or `runtimes/<rid>/native/`

Planned first-release RID coverage:

- `linux-x64`
- `osx-x64`
- `osx-arm64`
- `win-x64`

## Local test

Build the extension first:

```bash
cargo build -p honker-extension
```

Then run the .NET tests:

```bash
dotnet test packages/honker-dotnet/tests/Honker.Tests/Honker.Tests.csproj
```

## Release

CI now smoke-tests `dotnet pack` on every supported OS. NuGet publish
is handled by `.github/workflows/release-dotnet.yml`:

- push a tag like `dotnet-v0.1.0`, or
- run the workflow manually with a version input

The workflow builds per-RID native extension artifacts, assembles them
into one NuGet package, then pushes it with `NUGET_API_KEY`.
