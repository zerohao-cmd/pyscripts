from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

import pytest
import ray

from pyscripts.runtime.actor import UnifiedActor
from pyscripts.runtime.compute import ComputeTask


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
