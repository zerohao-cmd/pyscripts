from __future__ import annotations

import asyncio
import uuid

from pydantic import SecretStr

from pyscripts.config import Settings
from pyscripts.repository import ResolvedEndpoint
from pyscripts.runtime import directory
from pyscripts.runtime.compute import ComputeTaskResult
from pyscripts.runtime.directory import ActorCandidate, ProfilePoolScheduler


def candidate(replica: int, state: str, active_io: int) -> ActorCandidate:
    return ActorCandidate(
        replica=replica,
        handle=object(),
        status={"state": state, "active_io": active_io},
    )


def test_io_packs_into_the_busiest_shared_actor() -> None:
    candidates = [
        candidate(0, "IDLE", 0),
        candidate(1, "SHARED_IO", 3),
        candidate(2, "SHARED_IO", 8),
        candidate(3, "EXCLUSIVE_COMPUTE", 0),
    ]
    ordered = ProfilePoolScheduler._order_candidates(candidates)
    assert [item.replica for item in ordered] == [2, 1, 0]


async def test_compute_is_submitted_as_ray_task_not_actor(monkeypatch) -> None:
    class FakeComputeTask:
        options_value: dict | None = None
        call_value: dict | None = None

        @classmethod
        def options(cls, **options):
            cls.options_value = options
            return cls

        @classmethod
        def remote(cls, **arguments):
            cls.call_value = arguments
            future = asyncio.get_running_loop().create_future()
            future.set_result({"value": 42})
            return future

    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        ray_use_label_selector=True,
    )
    scheduler = ProfilePoolScheduler(settings)

    async def ray_ready() -> None:
        return None

    async def actors_must_not_be_used(*args, **kwargs):
        raise AssertionError("compute execution attempted to use an actor")

    monkeypatch.setattr(directory, "ComputeTask", FakeComputeTask)
    monkeypatch.setattr(scheduler, "_ensure_ray", ray_ready)
    monkeypatch.setattr(scheduler, "_pool_snapshot", actors_must_not_be_used)
    target = ResolvedEndpoint(
        service_id=uuid.uuid4(),
        service_name="math-service",
        revision_id=uuid.uuid4(),
        revision="rev-1",
        artifact_uri="https://example.invalid/math.zip",
        artifact_digest="a" * 64,
        runtime_profile="compute@v1",
        endpoint_manifest=[
            {
                "id": "add",
                "task_type": "compute",
                "entrypoint": "service:add",
            }
        ],
        endpoint_id="add",
        task_type="compute",
        entrypoint="service:add",
        environment_digest="environment-v1",
        runtime_env={"pip": {"packages": ["numpy==2.1.0"]}},
        worker_pool="py312-ray258-default",
        num_cpus=2,
    )

    result = await scheduler.execute(target, uuid.uuid4(), {"x": 20, "y": 22})

    assert result == {"value": 42}
    assert FakeComputeTask.options_value is not None
    assert FakeComputeTask.options_value["num_cpus"] == 2
    assert "py_modules" not in FakeComputeTask.options_value["runtime_env"]
    assert FakeComputeTask.options_value["label_selector"] == {
        "pyscripts.worker-pool": "py312-ray258-default"
    }
    assert FakeComputeTask.call_value is not None
    assert FakeComputeTask.call_value["environment_digest"] == "environment-v1"
    assert FakeComputeTask.call_value["include_metadata"] is True


async def test_compute_reuses_warm_profile_node_with_pool_fallback(monkeypatch) -> None:
    class FakeComputeTask:
        options_values: list[dict] = []

        @classmethod
        def options(cls, **options):
            cls.options_values.append(options)
            return cls

        @classmethod
        def remote(cls, **arguments):
            future = asyncio.get_running_loop().create_future()
            future.set_result(
                ComputeTaskResult(value={"value": 42}, node_id="node-abc")
            )
            return future

    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        ray_use_label_selector=True,
    )
    scheduler = ProfilePoolScheduler(settings)

    async def ray_ready() -> None:
        return None

    monkeypatch.setattr(directory, "ComputeTask", FakeComputeTask)
    monkeypatch.setattr(scheduler, "_ensure_ray", ray_ready)
    target = ResolvedEndpoint(
        service_id=uuid.uuid4(),
        service_name="math-service",
        revision_id=uuid.uuid4(),
        revision="rev-1",
        artifact_uri="https://example.invalid/math.zip",
        artifact_digest="a" * 64,
        runtime_profile="compute@v1",
        endpoint_manifest=[
            {"id": "add", "task_type": "compute", "entrypoint": "service:add"}
        ],
        endpoint_id="add",
        task_type="compute",
        entrypoint="service:add",
        environment_digest="environment-v1",
        runtime_env={},
        worker_pool="py312-ray258-default",
    )

    assert await scheduler.execute(target, uuid.uuid4(), {}) == {"value": 42}
    assert await scheduler.execute(target, uuid.uuid4(), {}) == {"value": 42}

    assert FakeComputeTask.options_values[0]["label_selector"] == {
        "pyscripts.worker-pool": "py312-ray258-default"
    }
    assert FakeComputeTask.options_values[1]["label_selector"] == {
        "pyscripts.worker-pool": "py312-ray258-default",
        "ray.io/node-id": "node-abc",
    }
    assert FakeComputeTask.options_values[1]["fallback_strategy"] == [
        {
            "label_selector": {
                "pyscripts.worker-pool": "py312-ray258-default"
            }
        }
    ]
