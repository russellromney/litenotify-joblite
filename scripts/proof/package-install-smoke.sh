#!/usr/bin/env bash
set -euo pipefail

# Build packages, install them into throwaway consumer projects, and run
# tiny user-shaped smoke tests. This is release proof, not normal dev CI.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="${TMPDIR:-/tmp}/honker-package-proof-$$"
PYTHON_BIN="${PYTHON_BIN:-}"
if [[ -z "$PYTHON_BIN" && -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT/.venv/bin/python"
fi
PYTHON_BIN="${PYTHON_BIN:-python3}"
RUBY_BIN="${RUBY_BIN:-}"
if [[ -z "$RUBY_BIN" ]] && command -v brew >/dev/null 2>&1; then
  BREW_RUBY="$(brew --prefix ruby 2>/dev/null || true)"
  if [[ -n "$BREW_RUBY" && -x "$BREW_RUBY/bin/ruby" ]]; then
    RUBY_BIN="$BREW_RUBY/bin/ruby"
  fi
fi
RUBY_BIN="${RUBY_BIN:-ruby}"
mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

cd "$ROOT"

echo "== build extension =="
cargo build --release -p honker-extension
EXT="$ROOT/target/release/libhonker_ext.so"
if [[ "$(uname -s)" == "Darwin" ]]; then
  EXT="$ROOT/target/release/libhonker_ext.dylib"
fi
if [[ ! -f "$EXT" ]]; then
  echo "honker extension not found at $EXT" >&2
  exit 1
fi

echo "== python wheel install =="
"$PYTHON_BIN" -m venv "$TMP/py"
"$TMP/py/bin/python" -m pip install --upgrade pip maturin >/dev/null
"$TMP/py/bin/python" -m maturin build --release \
  -i "$TMP/py/bin/python" \
  --manifest-path "$ROOT/packages/honker/Cargo.toml" \
  --out "$TMP/wheels"
"$TMP/py/bin/python" -m pip install "$TMP"/wheels/*.whl >/dev/null
"$TMP/py/bin/python" - <<'PY'
import tempfile
import honker

with tempfile.TemporaryDirectory() as d:
    db = honker.open(f"{d}/app.db")
    q = db.queue("emails")
    job_id = q.enqueue({"to": "alice@example.com"})
    job = q.claim_one("worker-1")
    assert job and job.id == job_id
    assert job.payload["to"] == "alice@example.com"
    assert job.ack()
print("python package smoke ok")
PY

echo "== node package install =="
mkdir -p "$TMP/npm"
(
  cd "$ROOT/packages/honker-node"
  npm install
  npx napi build --platform --release
  npm pack --pack-destination "$TMP/npm"
)
mkdir -p "$TMP/node-consumer"
(
  cd "$TMP/node-consumer"
  npm init -y >/dev/null
  npm install "$TMP"/npm/*.tgz >/dev/null
  node <<'JS'
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { open } = require('@russellthehippo/honker-node');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-node-proof-'));
try {
  const db = open(path.join(dir, 'app.db'));
  const q = db.queue('emails');
  const id = q.enqueue({ to: 'alice@example.com' });
  const job = q.claimOne('worker-1');
  assert.equal(job.id, id);
  assert.equal(job.payload.to, 'alice@example.com');
  assert.equal(job.ack(), true);
  db.close();
  console.log('node package smoke ok');
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
JS
)

echo "== ruby gem install =="
(
  cd "$ROOT/packages/honker-ruby"
  "$RUBY_BIN" -S gem build honker.gemspec --output "$TMP/honker.gem"
)
GEM_HOME="$TMP/gems" GEM_PATH="$TMP/gems" "$RUBY_BIN" -S gem install "$TMP/honker.gem" >/dev/null
GEM_HOME="$TMP/gems" GEM_PATH="$TMP/gems" HONKER_EXTENSION_PATH="$EXT" "$RUBY_BIN" <<'RB'
require "tmpdir"
require "honker"

Dir.mktmpdir("honker-ruby-proof-") do |dir|
  db = Honker::Database.new(File.join(dir, "app.db"), extension_path: ENV.fetch("HONKER_EXTENSION_PATH"))
  q = db.queue("emails")
  id = q.enqueue({to: "alice@example.com"})
  job = q.claim_one("worker-1")
  raise "claim failed" unless job && job.id == id
  raise "payload mismatch" unless job.payload["to"] == "alice@example.com"
  raise "ack failed" unless job.ack
  db.close
end
puts "ruby package smoke ok"
RB

echo "== dotnet nuget install =="
RID="linux-x64"
NATIVE="libhonker_ext.so"
if [[ "$(uname -s)" == "Darwin" ]]; then
  RID="osx-arm64"
  NATIVE="libhonker_ext.dylib"
fi
mkdir -p "$ROOT/packages/honker-dotnet/src/Honker/package-assets/runtimes/$RID/native"
cp "$EXT" "$ROOT/packages/honker-dotnet/src/Honker/package-assets/runtimes/$RID/native/$NATIVE"
dotnet pack "$ROOT/packages/honker-dotnet/src/Honker/Honker.csproj" \
  -c Release \
  -p:PackageVersion=0.0.0-proof \
  -o "$TMP/nuget" >/dev/null
mkdir -p "$TMP/dotnet-consumer"
(
  cd "$TMP/dotnet-consumer"
  dotnet new console >/dev/null
  dotnet add package Honker --version 0.0.0-proof --source "$TMP/nuget" >/dev/null
  cat > Program.cs <<'CS'
using Honker;
using System.Text.Json;

var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-proof-{Guid.NewGuid():N}");
Directory.CreateDirectory(dir);
try
{
    using var db = Database.Open(Path.Combine(dir, "app.db"));
    var q = db.Queue("emails");
    var id = q.Enqueue(new { to = "alice@example.com" });
    var job = q.ClaimOne("worker-1") ?? throw new Exception("claim failed");
    if (job.Id != id) throw new Exception("id mismatch");
    if (job.Payload.GetProperty("to").GetString() != "alice@example.com")
        throw new Exception("payload mismatch");
    if (!job.Ack()) throw new Exception("ack failed");
    Console.WriteLine("dotnet package smoke ok");
}
finally
{
    Directory.Delete(dir, recursive: true);
}
CS
  dotnet run --no-restore
)

echo "package install proof ok"
