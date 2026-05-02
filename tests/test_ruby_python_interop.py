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


def _require_ruby_interop():
    reason = (
        "Ruby binding unavailable, or honker-extension missing; run "
        "`cargo build -p honker-extension --release` and "
        "`bundle install` in packages/honker-ruby"
    )
    ready = EXT_PATH is not None and _ruby_ready(RUBY_CMD)
    if ready:
        return
    if os.environ.get("HONKER_REQUIRE_INTEROP") == "1":
        pytest.fail(reason)
    pytest.skip(reason)


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


def test_ruby_and_python_share_queue_stream_and_notification_tables(tmp_path):
    _require_ruby_interop()
    db_path = tmp_path / "ruby-python.db"

    _run_ruby(
        r'''
        require "honker"

        db = Honker::Database.new(
          ENV.fetch("DB_PATH"),
          extension_path: ENV.fetch("HONKER_EXTENSION_PATH"),
        )
        q = db.queue("ruby-to-python")
        25.times do |i|
          q.enqueue({"source" => "ruby", "seq" => i, "key" => "ruby-%02d" % i})
        end
        db.notify("from-ruby", {"source" => "ruby", "count" => 25})
        db.stream("interop").publish({"source" => "ruby", "kind" => "stream"})
        db.close
        ''',
        db_path,
    )

    py_db = honker.open(str(db_path))
    ruby_jobs = py_db.queue("ruby-to-python").claim_batch("python-worker", 50)
    assert len(ruby_jobs) == 25
    assert sorted(job.payload["seq"] for job in ruby_jobs) == list(range(25))
    assert all(job.payload["source"] == "ruby" for job in ruby_jobs)
    assert (
        py_db.queue("ruby-to-python").ack_batch(
            [job.id for job in ruby_jobs],
            "python-worker",
        )
        == 25
    )

    ruby_notification = py_db.query(
        "SELECT payload FROM _honker_notifications "
        "WHERE channel = 'from-ruby' ORDER BY id DESC LIMIT 1"
    )
    assert json.loads(ruby_notification[0]["payload"]) == {
        "source": "ruby",
        "count": 25,
    }
    assert len(py_db.stream("interop")._read_since(0, 10)) == 1

    py_q = py_db.queue("python-to-ruby")
    for i in range(25):
        py_q.enqueue({"source": "python", "seq": i, "key": f"py-{i:02d}"})
    with py_db.transaction() as tx:
        tx.notify("from-python", {"source": "python", "count": 25})
    py_db.stream("interop").publish({"source": "python", "kind": "stream"})

    out = _run_ruby(
        r'''
        require "json"
        require "honker"

        db = Honker::Database.new(
          ENV.fetch("DB_PATH"),
          extension_path: ENV.fetch("HONKER_EXTENSION_PATH"),
        )
        jobs = db.queue("python-to-ruby").claim_batch("ruby-worker", 50)
        note = db.db.get_first_row(
          "SELECT payload FROM _honker_notifications " \
          "WHERE channel = 'from-python' ORDER BY id DESC LIMIT 1"
        )
        events = db.stream("interop").read_since(0, 10)
        puts JSON.generate({
          payloads: jobs.map(&:payload),
          acked: db.queue("python-to-ruby").ack_batch(jobs.map(&:id), "ruby-worker"),
          notification: JSON.parse(note[0]),
          event_count: events.length,
        })
        db.close
        ''',
        db_path,
    )
    observed = json.loads(out)
    assert len(observed["payloads"]) == 25
    assert sorted(row["seq"] for row in observed["payloads"]) == list(range(25))
    assert all(row["source"] == "python" for row in observed["payloads"])
    assert observed["acked"] == 25
    assert observed["notification"] == {"source": "python", "count": 25}
    assert observed["event_count"] == 2
