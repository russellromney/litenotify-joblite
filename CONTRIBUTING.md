# Contributing

Short notes on repo layout, tests, and releases.

## Layout

- `honker-core/` — shared Rust crate (rlib). Published to crates.io.
- `honker-extension/` — SQLite loadable extension (cdylib). Published to crates.io.
- `packages/honker-{py,node,rs,go,ruby,bun,ex}/` — language bindings. Each lives in its own GitHub repo and is git-submoduled here.
- `site/` — honker.dev (Astro Starlight; git submodule).
- `tests/` — cross-binding integration tests.

## Running tests

```bash
make test           # rust core + python + node, fast
make test-python-slow   # soak + real-time cron (~2 min)
make test-all       # everything
```

Submodule bindings have their own test runners; see each repo's README for language-specific commands (`cargo test`, `bun test`, `mix test`, `bundle exec ruby spec/*.rb`, etc.).

## Releases

Crate releases are tag-triggered. Bump the version in the crate's `Cargo.toml`, commit, then tag:

```bash
# honker-core
# edit honker-core/Cargo.toml → version = "0.1.1"
git commit -am "honker-core v0.1.1: <summary>"
git tag core-v0.1.1
git push origin main core-v0.1.1

# honker-extension
git tag ext-v0.1.1
git push origin ext-v0.1.1
```

GitHub Actions (`.github/workflows/release-crates.yml`) picks up tags matching `core-v*` / `ext-v*` and runs `cargo publish`.

Each language binding lives in its own submodule repo and has its own release process; see each binding's README.

## Making changes

- PRs welcome; CI must be green before merge.
- Keep PRs focused: one feature or fix, not mixed refactors.
- No framework plugins (FastAPI/Django/Flask/etc) — see README for rationale.
