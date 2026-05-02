import importlib.util
import sys


def _load_example():
    spec = importlib.util.spec_from_file_location(
        "honker_real_app_example",
        "packages/honker/examples/real_app.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


async def test_real_app_example_proves_request_worker_stream_notify_scheduler(db_path):
    example = _load_example()
    proof = await example.run(db_path)

    assert proof.order_id == 1
    assert proof.email_sent_to == "alice@example.com"
    assert proof.stream_event == {"order_id": 1, "kind": "created"}
    assert proof.notification == {"order_id": 1, "kind": "created"}
    assert proof.scheduler_payload == {"task": "prune_notifications"}
    assert proof.worker_wake_ms < 5000.0
