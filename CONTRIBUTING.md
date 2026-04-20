# Contributing

Short notes on repo layout, tests, secrets, and releases.

## Layout

- `honker-core/` — shared Rust crate (rlib). Published to crates.io.
- `honker-extension/` — SQLite loadable extension (cdylib). Published to crates.io.
- `packages/honker/` — Python binding (in-tree for now).
- `packages/honker-{node,rs,go,ruby,bun,ex}/` — language bindings (each its own repo, git-submoduled).
- `site/` — honker.dev (Astro Starlight; git submodule).
- `tests/` — cross-binding integration tests.

## Running tests

```bash
make test           # rust core + python + node, fast
make test-python-slow   # soak + real-time cron (~2 min)
make test-all       # everything
```

Submodule bindings have their own test runners; see each repo's README.

## Secrets

Local dev + CI secrets live in the `honker/honker` Soup project
(project slug `honker`, env slug `honker`). Current keys:

| Key                     | Notes                                             |
|-------------------------|---------------------------------------------------|
| `CARGO_REGISTRY_TOKEN`  | crates.io publish token. **Populated.**           |
| `NPM_TOKEN`             | npm publish for `@honker/node`. Placeholder.      |
| `PYPI_TOKEN`            | PyPI publish for `honker`. Placeholder.           |
| `CLOUDFLARE_API_TOKEN`  | CI deploy of honker.dev. Placeholder.             |
| `CLOUDFLARE_ACCOUNT_ID` | Cloudflare account ID. Placeholder.               |

### Using Soup locally

`soup config --show` should report `honker/honker`. Prefix any command
that needs secrets as env vars with `soup run --`:

```bash
soup run -- cargo publish -p honker-core
soup run -- npm publish
```

### Syncing Soup → GitHub Actions secrets

GitHub Actions has its own secrets store (`Settings → Secrets and
variables → Actions`). One-liner to sync a single key:

```bash
soup secrets export | grep '^CARGO_REGISTRY_TOKEN=' | \
  sed 's/^[^=]*=//' | gh secret set CARGO_REGISTRY_TOKEN --repo russellromney/honker
```

Or bulk:

```bash
soup secrets export > /tmp/.env.honker
while IFS='=' read -r key val; do
  gh secret set "$key" --repo russellromney/honker --body "$val"
done < /tmp/.env.honker
rm /tmp/.env.honker
```

Replace placeholders in Soup first (`soup secrets set NPM_TOKEN
<real-token>`) before syncing — otherwise you'll push
`PLACEHOLDER_*` values into GitHub Actions.

## Releases

Crate releases are tag-triggered. Bump the version in the crate's
`Cargo.toml`, commit, then tag:

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

GitHub Actions (`.github/workflows/release-crates.yml`) picks up
tags matching `core-v*` / `ext-v*` and runs `cargo publish` with
`CARGO_REGISTRY_TOKEN`.

`honker` (the Rust binding) lives in the `honker-rs` submodule; bump
+ tag + publish happens inside that repo.

## Making changes

- PRs welcome; CI must be green before merge.
- Keep PRs focused: one feature / fix, not mixed refactors.
- No framework plugins (FastAPI/Django/Flask/etc) — see README for rationale.
