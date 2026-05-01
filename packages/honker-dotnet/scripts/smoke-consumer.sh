#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <nupkg-dir> [package-id]" >&2
  exit 1
fi

PACKAGE_DIR=$(cd "$1" && pwd)
PACKAGE_ID=${2:-Honker}
TARGET_FRAMEWORK=${HONKER_DOTNET_SMOKE_TFM:-net10.0}

PACKAGE_FILE=$(find "$PACKAGE_DIR" -maxdepth 1 -name "${PACKAGE_ID}*.nupkg" | head -n 1)
if [[ -z "${PACKAGE_FILE:-}" ]]; then
  echo "could not find ${PACKAGE_ID}*.nupkg under $PACKAGE_DIR" >&2
  exit 1
fi

PACKAGE_NAME=$(basename "$PACKAGE_FILE")
PACKAGE_VERSION=${PACKAGE_NAME#${PACKAGE_ID}.}
PACKAGE_VERSION=${PACKAGE_VERSION%.nupkg}

WORKDIR=$(mktemp -d)
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

dotnet new console --framework "$TARGET_FRAMEWORK" --output "$WORKDIR" >/dev/null
dotnet add "$WORKDIR" package "$PACKAGE_ID" --version "$PACKAGE_VERSION" --source "$PACKAGE_DIR" >/dev/null

cat > "$WORKDIR/Program.cs" <<'EOF'
using Honker;

var dbPath = Path.Combine(AppContext.BaseDirectory, "smoke.db");
using var db = Database.Open(dbPath);
var queue = db.Queue("smoke");
queue.Enqueue(new { ok = true });
var job = queue.ClaimOne("smoke-worker");
if (job is null)
{
    throw new Exception("failed to claim smoke job");
}

if (!job.Ack())
{
    throw new Exception("failed to ack smoke job");
}

Console.WriteLine("honker-dotnet smoke ok");
EOF

dotnet run --project "$WORKDIR" -c Release >/dev/null
