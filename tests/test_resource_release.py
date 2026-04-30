import asyncio
import gc
import shutil
import sys
import time

import honker


def _remove_dir_strict(path) -> None:
    last_err = None
    attempts = 60 if sys.platform == "win32" else 1
    for i in range(attempts):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError:
            return
        except OSError as err:
            last_err = err
        gc.collect()
        time.sleep(0.05 * (i + 1))
    if last_err is not None:
        raise last_err


async def test_explicit_close_releases_db_files(tmp_path):
    root = tmp_path / "release-check"
    root.mkdir()
    db_path = root / "t.db"

    db = honker.open(str(db_path))
    updates = db.update_events()
    listener = db.listen("release")
    worker = db.queue("jobs").claim("worker-1")
    stream_iter = db.stream("events").subscribe("consumer")

    # Let the async resources park on their subscriptions so this test
    # exercises the real release path rather than a never-used handle.
    parked = [
        asyncio.create_task(asyncio.wait_for(listener.__anext__(), 0.01)),
        asyncio.create_task(asyncio.wait_for(worker.__anext__(), 0.01)),
        asyncio.create_task(asyncio.wait_for(stream_iter.__anext__(), 0.01)),
    ]
    await asyncio.gather(*parked, return_exceptions=True)

    await listener.aclose()
    await worker.aclose()
    await stream_iter.aclose()
    updates.close()
    db.close()

    gc.collect()
    if sys.platform == "win32":
        time.sleep(0.5)

    _remove_dir_strict(root)
    assert not root.exists()
