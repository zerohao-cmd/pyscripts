from __future__ import annotations

import hashlib
import time
import zipfile
from pathlib import Path

import pytest
import ray

from pyscripts.runtime.actor import UnifiedActor
from pyscripts.runtime.compute import ComputeTask
from pyscripts.runtime.output import ExecutionOutcome


@pytest.fixture(scope="module")
def ray_runtime():
    ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
    yield
    ray.shutdown()


def test_remote_actor_accepts_shared_io_and_rejects_compute(
    ray_runtime: None, tmp_path: Path
) -> None:
    actor = UnifiedActor.remote("integration-test", str(tmp_path), 2, 1)

    first_io = ray.get(actor.try_reserve.remote("io-1", "io", "svc", "rev"))
    second_io = ray.get(actor.try_reserve.remote("io-2", "io", "svc", "rev"))
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(actor.try_reserve.remote("compute", "compute", "svc", "rev"))
    status = ray.get(actor.status.remote())

    assert first_io is not None
    assert second_io is not None
    assert status["state"] == "SATURATED_IO"

    ray.get(actor.cancel_reservation.remote(first_io))
    ray.get(actor.cancel_reservation.remote(second_io))
    third_io = ray.get(actor.try_reserve.remote("io-3", "io", "svc", "rev"))
    assert third_io is not None
    assert ray.get(actor.status.remote())["state"] == "SHARED_IO"


def test_compute_runs_as_remote_task(ray_runtime: None, tmp_path: Path) -> None:
    artifact = tmp_path / "compute.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "service.py",
            "def add(context, left, right):\n"
            "    return {'value': left + right, 'request_id': context['request_id']}\n",
        )
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()

    reference = ComputeTask.options(num_cpus=1).remote(
        transport="http",
        cache_root=str(tmp_path / "runtime"),
        environment_digest="integration-environment",
        service="math-service",
        revision="compute-rev-1",
        endpoint_id="add",
        context={"request_id": "compute-request"},
        payload={"left": 20, "right": 22},
        artifact_uri=artifact.as_uri(),
        artifact_digest=digest,
        endpoint_manifest=[
            {
                "id": "add",
                "task_type": "compute",
                "entrypoint": "service:add",
            }
        ],
    )

    assert ray.get(reference) == {"value": 42, "request_id": "compute-request"}


def test_io_hook_remote_call_dispatches_compute_task(
    ray_runtime: None, tmp_path: Path
) -> None:
    artifact = tmp_path / "hook-pipeline.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "service.py",
            "from sc_hook import Int32, IOType, hook\n"
            "@hook(io_types=[IOType.Inner])\n"
            "def multiply(left: Int32, right: Int32) -> Int32:\n"
            "    print('compute-child')\n"
            "    return left * right\n"
            "@hook(io_types=[IOType.Inner])\n"
            "async def prepare(value: Int32) -> Int32:\n"
            "    print('io-child')\n"
            "    return value + 1\n"
            "@hook(io_types=[IOType.Rest])\n"
            "async def pipeline(left: Int32, right: Int32) -> Int32:\n"
            "    print('io-parent')\n"
            "    prepared = await prepare.remote_call(left)\n"
            "    return await multiply.remote_call(prepared, right)\n",
        )
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    manifest = [
        {
            "id": "prepare",
            "task_type": "io",
            "entrypoint": "service:prepare",
            "io_type": ["inner"],
        },
        {
            "id": "multiply",
            "task_type": "compute",
            "entrypoint": "service:multiply",
            "io_type": ["inner"],
        },
        {
            "id": "pipeline",
            "task_type": "io",
            "entrypoint": "service:pipeline",
            "io_type": ["rest"],
        },
    ]
    actor = UnifiedActor.remote("hook-integration", str(tmp_path / "actor"), 2, 5)
    lease = ray.get(
        actor.try_reserve.remote("request-1", "io", "hook-service", "rev-1")
    )
    assert lease is not None

    outcome = ray.get(
        actor.execute.remote(
            lease,
            "pipeline",
            {"request_id": "request-1"},
            {"left": 6, "right": 7},
            artifact.as_uri(),
            digest,
            manifest,
        )
    )

    assert isinstance(outcome, ExecutionOutcome)
    assert outcome.succeeded is True
    assert outcome.value == 49
    output = "".join(chunk.content for chunk in outcome.logs)
    assert "io-parent" in output
    assert "io-child" in output
    assert "compute-child" in output


def test_cancel_running_io_is_scoped_to_lease(ray_runtime, tmp_path):
    artifact = tmp_path / "cancel.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("service.py", "import asyncio\nasync def wait(value, delay):\n    await asyncio.sleep(delay)\n    return value\n")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    manifest = [{"id": "wait", "task_type": "io", "entrypoint": "service:wait"}]
    actor = UnifiedActor.remote("cancel-test", str(tmp_path / "runtime"), 4, 5)
    cancelled_lease = ray.get(actor.try_reserve.remote("cancel", "io", "svc", "rev"))
    other_lease = ray.get(actor.try_reserve.remote("other", "io", "svc", "rev"))
    cancelled = actor.execute.remote(cancelled_lease, "wait", {}, {"value": 1, "delay": 60}, artifact.as_uri(), digest, manifest)
    other = actor.execute.remote(other_lease, "wait", {}, {"value": 2, "delay": .4}, artifact.as_uri(), digest, manifest)
    deadline = time.monotonic() + 5
    while ray.get(actor.status.remote())["running_leases"] < 2:
        assert time.monotonic() < deadline
        time.sleep(.01)
    start = time.monotonic()
    assert ray.get(actor.cancel_execution.remote(cancelled_lease), timeout=5)
    assert time.monotonic() - start < 3
    with pytest.raises(ray.exceptions.RayError):
        ray.get(cancelled, timeout=5)
    assert ray.get(other, timeout=5).value == 2
    assert ray.get(actor.status.remote())["active_io"] == 0
    # The actor and loaded revision remain usable after a request is cancelled.
    lease = ray.get(actor.try_reserve.remote("next", "io", "svc", "rev"))
    result = ray.get(actor.execute.remote(lease, "wait", {}, {"value": 3, "delay": 0}, artifact.as_uri(), digest, manifest))
    assert result.value == 3
    reserved = ray.get(actor.try_reserve.remote("unused", "io", "svc", "rev"))
    assert ray.get(actor.cancel_execution.remote(reserved))
    assert not ray.get(actor.cancel_execution.remote(reserved))
    ray.kill(actor)


def test_cancel_io_cascades_to_compute(ray_runtime, tmp_path):
    artifact = tmp_path / "cancel-child.zip"
    started = tmp_path / "started"
    finished = tmp_path / "finished"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("service.py", (
            "import time\nfrom pathlib import Path\nfrom sc_hook import hook, IOType, Int32\n"
            "@hook(io_types=[IOType.Inner])\n"
            "def slow(seconds: Int32) -> Int32:\n"
            f"    Path({str(started)!r}).touch()\n"
            "    time.sleep(seconds)\n"
            f"    Path({str(finished)!r}).touch()\n"
            "    return 42\n"
            "@hook(io_types=[IOType.Rest])\n"
            "async def pipeline(seconds: Int32) -> Int32:\n"
            "    return await slow.remote_call(seconds)\n"
        ))
    manifest = [
        {"id": "slow", "task_type": "compute", "entrypoint": "service:slow", "io_type": ["inner"]},
        {"id": "pipeline", "task_type": "io", "entrypoint": "service:pipeline"},
    ]
    actor = UnifiedActor.remote("cancel-child", str(tmp_path / "runtime"), 2, 5)
    lease = ray.get(actor.try_reserve.remote("cancel", "io", "svc", "rev"))
    reference = actor.execute.remote(lease, "pipeline", {}, {"seconds": 3}, artifact.as_uri(), hashlib.sha256(artifact.read_bytes()).hexdigest(), manifest)
    deadline = time.monotonic() + 10
    while not started.exists():
        assert time.monotonic() < deadline
        time.sleep(.01)
    assert ray.get(actor.cancel_execution.remote(lease), timeout=5)
    with pytest.raises(ray.exceptions.RayError):
        ray.get(reference, timeout=5)
    assert ray.get(actor.status.remote())["active_io"] == 0
    time.sleep(3.2)
    assert not finished.exists()
    ray.kill(actor)
