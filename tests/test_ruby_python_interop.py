"""Cross-binding interop between Ruby and Python.

Ruby is currently an extension-backed binding rather than a core
watcher binding. This test still matters: both packages must agree on
the on-disk schema, queue semantics, and notification payload format.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

import honker


REPO_ROOT = Path(__file__).resolve().parents[1]
RUBY_DIR = REPO_ROOT / "packages" / "honker-ruby"


def _extension_path():
    candidates = [
        REPO_ROOT / "target" / "release" / "libhonker_ext.dylib",
        REPO_ROOT / "target" / "release" / "libhonker_ext.so",
    ]
    return next((p for p in candidates if p.exists()), None)


def _ruby_command():
    if shutil.which("bundle"):
        return ["bundle", "exec", "ruby"]
    ruby = shutil.which("ruby")
    if ruby:
        return [ruby]
    return None


def _ruby_ready(cmd):
    if cmd is None:
        return False
    probe = subprocess.run(
        [*cmd, "-Ilib", "-e", 'require "honker"'],
        cwd=RUBY_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return probe.returncode == 0


EXT_PATH = _extension_path()
RUBY_CMD = _ruby_command()


pytestmark = pytest.mark.skipif(
    EXT_PATH is None or not _ruby_ready(RUBY_CMD),
    reason=(
        "Ruby binding unavailable, or honker-extension missing; run "
        "`cargo build -p honker-extension --release` and "
        "`bundle install` in packages/honker-ruby"
    ),
)


def _run_ruby(script, db_path):
    env = os.environ.copy()
    env["DB_PATH"] = str(db_path)
    env["HONKER_EXTENSION_PATH"] = str(EXT_PATH)
    proc = subprocess.run(
        [*RUBY_CMD, "-Ilib", "-e", script],
        cwd=RUBY_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    return proc.stdout


def test_ruby_and_python_share_queue_and_notification_tables(tmp_path):
    db_path = tmp_path / "ruby-python.db"

    _run_ruby(
        r'''
        require "honker"

        db = Honker::Database.new(
          ENV.fetch("DB_PATH"),
          extension_path: ENV.fetch("HONKER_EXTENSION_PATH"),
        )
        db.queue("emails").enqueue({"to" => "ruby@example.com"})
        db.notify("from-ruby", {"source" => "ruby"})
        db.close
        ''',
        db_path,
    )

    py_db = honker.open(str(db_path))
    ruby_job = py_db.queue("emails").claim_one("python-worker")
    assert ruby_job is not None
    assert ruby_job.payload == {"to": "ruby@example.com"}
    assert ruby_job.ack()

    ruby_notification = py_db.query(
        "SELECT payload FROM _honker_notifications "
        "WHERE channel = 'from-ruby' ORDER BY id DESC LIMIT 1"
    )
    assert json.loads(ruby_notification[0]["payload"]) == {"source": "ruby"}

    py_db.queue("python-jobs").enqueue({"to": "python@example.com"})
    with py_db.transaction() as tx:
        tx.notify("from-python", {"source": "python"})

    out = _run_ruby(
        r'''
        require "json"
        require "honker"

        db = Honker::Database.new(
          ENV.fetch("DB_PATH"),
          extension_path: ENV.fetch("HONKER_EXTENSION_PATH"),
        )
        job = db.queue("python-jobs").claim_one("ruby-worker")
        note = db.db.get_first_row(
          "SELECT payload FROM _honker_notifications " \
          "WHERE channel = 'from-python' ORDER BY id DESC LIMIT 1"
        )
        puts JSON.generate({
          job_payload: job.payload,
          acked: job.ack,
          notification: JSON.parse(note[0]),
        })
        db.close
        ''',
        db_path,
    )
    observed = json.loads(out)
    assert observed == {
        "job_payload": {"to": "python@example.com"},
        "acked": True,
        "notification": {"source": "python"},
    }
