from __future__ import annotations

import asyncio
import uuid

from pydantic import SecretStr
from ray.exceptions import RayError

from pyscripts.config import Settings
from pyscripts.repository import ResolvedEndpoint
from pyscripts.runtime import directory
from pyscripts.runtime.compute import ComputeTaskResult
from pyscripts.runtime.directory import (
    ActorCandidate,
    PoolProfile,
    ProfilePoolScheduler,
)


class FakeRemoteMethod:
    def __init__(self, callback):
        self.callback = callback

    def remote(self, *args, **kwargs):
        return self.callback(*args, **kwargs)


class FakeIdleActor:
    def __init__(self, idle_for_seconds: float):
        self.draining = False
        self.idle_for_seconds = idle_for_seconds
        self.status = FakeRemoteMethod(self._status)
        self.drain_actor = FakeRemoteMethod(self._drain)

    async def _status(self):
        return {
            "state": "DRAINING" if self.draining else "IDLE",
            "active_io": 0,
            "reserved_leases": 0,
            "running_leases": 0,
            "idle_for_seconds": self.idle_for_seconds,
        }

    async def _drain(self):
        self.draining = True


def candidate(replica: int, state: str, active_io: int) -> ActorCandidate:
    return ActorCandidate(
        replica=replica,
        handle=object(),
        status={"state": state, "active_io": active_io},
    )


def io_target() -> ResolvedEndpoint:
    return ResolvedEndpoint(
        service_id=uuid.uuid4(),
        service_name="io-service",
        revision_id=uuid.uuid4(),
        revision="rev-1",
        artifact_uri="https://example.invalid/io.zip",
        artifact_digest="a" * 64,
        runtime_profile="io@v1",
        endpoint_manifest=[
            {"id": "sleep", "task_type": "io", "entrypoint": "service:sleep"}
        ],
        endpoint_id="sleep",
        task_type="io",
        entrypoint="service:sleep",
        environment_digest="environment-v1",
        runtime_env={},
        worker_pool="default",
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


def test_io_promotes_warm_actor_at_target_concurrency() -> None:
    candidates = [
        candidate(0, "SHARED_IO", 50),
        candidate(1, "IDLE", 0),
    ]

    ordered = ProfilePoolScheduler._order_candidates(
        candidates,
        target_concurrency=50,
    )

    assert [item.replica for item in ordered] == [1, 0]


def test_actor_name_uses_current_wire_protocol() -> None:
    assert ProfilePoolScheduler._actor_name("test@v2", 3).startswith(
        "pyscripts-runtime-v7-"
    )


async def test_actor_recovery_is_owned_by_scheduler(monkeypatch) -> None:
    class FakeActorFactory:
        options_value: dict | None = None
        call_value: tuple | None = None

        @classmethod
        def options(cls, **options):
            cls.options_value = options
            return cls

        @classmethod
        def remote(cls, *arguments):
            cls.call_value = arguments
            return object()

    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        ray_use_label_selector=True,
    )
    scheduler = ProfilePoolScheduler(settings)
    profile_key = "environment-v1"
    scheduler._profiles[profile_key] = PoolProfile(
        key=profile_key,
        profile_ref="io@v1",
        worker_pool="default",
        runtime_env={"pip": {"packages": ["example==1.0"]}},
    )
    monkeypatch.setattr(directory, "UnifiedActor", FakeActorFactory)

    await scheduler._create_actor(profile_key, "actor-name")

    assert FakeActorFactory.options_value is not None
    assert FakeActorFactory.options_value["lifetime"] == "detached"
    assert FakeActorFactory.options_value["max_restarts"] == 0
    assert FakeActorFactory.options_value["runtime_env"] == {
        "pip": {"packages": ["example==1.0"]}
    }
    assert FakeActorFactory.options_value["label_selector"] == {
        "pyscripts.worker-pool": "default"
    }


async def test_idle_reaper_cools_pool_to_one_warm_actor(monkeypatch) -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_hot_idle_timeout_seconds=30,
        actor_warm_idle_timeout_seconds=300,
    )
    scheduler = ProfilePoolScheduler(settings)
    profile_key = "environment-v1"
    actors = [FakeIdleActor(90) for _ in range(3)]
    scheduler._profiles[profile_key] = object()  # type: ignore[assignment]
    scheduler._actors.update(
        {(profile_key, replica): actor for replica, actor in enumerate(actors)}
    )

    async def no_op(*args, **kwargs):
        return None

    killed = []
    monkeypatch.setattr(scheduler, "_prepare_pool", no_op)
    monkeypatch.setattr(scheduler, "_discover_actors", no_op)
    monkeypatch.setattr(
        directory.ray,
        "kill",
        lambda actor, no_restart: killed.append((actor, no_restart)),
    )

    assert await scheduler.reap_idle_actors() == 2
    assert list(scheduler._actors) == [(profile_key, 0)]
    assert killed == [(actors[2], True), (actors[1], True)]


async def test_idle_reaper_does_not_remove_recent_actor(monkeypatch) -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_hot_idle_timeout_seconds=30,
        actor_warm_idle_timeout_seconds=300,
    )
    scheduler = ProfilePoolScheduler(settings)
    profile_key = "environment-v1"
    actors = [FakeIdleActor(10), FakeIdleActor(10)]
    scheduler._profiles[profile_key] = object()  # type: ignore[assignment]
    scheduler._actors.update(
        {(profile_key, replica): actor for replica, actor in enumerate(actors)}
    )

    async def no_op(*args, **kwargs):
        return None

    monkeypatch.setattr(scheduler, "_prepare_pool", no_op)
    monkeypatch.setattr(scheduler, "_discover_actors", no_op)
    monkeypatch.setattr(
        directory.ray,
        "kill",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("recent actor must not be killed")
        ),
    )

    assert await scheduler.reap_idle_actors() == 0
    assert len(scheduler._actors) == 2


async def test_idle_reaper_scales_final_warm_actor_to_zero(monkeypatch) -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_hot_idle_timeout_seconds=30,
        actor_warm_idle_timeout_seconds=300,
    )
    scheduler = ProfilePoolScheduler(settings)
    profile_key = "environment-v1"
    actor = FakeIdleActor(400)
    scheduler._profiles[profile_key] = object()  # type: ignore[assignment]
    scheduler._actors[(profile_key, 0)] = actor

    async def no_op(*args, **kwargs):
        return None

    killed = []
    monkeypatch.setattr(scheduler, "_prepare_pool", no_op)
    monkeypatch.setattr(scheduler, "_discover_actors", no_op)
    monkeypatch.setattr(
        directory.ray,
        "kill",
        lambda target, no_restart: killed.append((target, no_restart)),
    )

    assert await scheduler.reap_idle_actors() == 1
    assert scheduler._actors == {}
    assert killed == [(actor, True)]


def test_request_frequency_adds_one_warm_spare() -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_hot_request_rate=0.5,
        actor_request_rate_window_seconds=10,
        actor_target_io_concurrency=50,
        actor_warm_spares=1,
    )
    scheduler = ProfilePoolScheduler(settings)
    profile_key = "environment-v1"

    scheduler._record_arrival(profile_key, now=100)
    assert scheduler._desired_actor_count(
        profile_key, [], pending_requests=1, now=100
    ) == 1

    for _ in range(4):
        scheduler._record_arrival(profile_key, now=100)
    assert scheduler._desired_actor_count(
        profile_key, [], pending_requests=1, now=100
    ) == 2


def test_concurrency_scales_hot_actors_before_hard_limit() -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_target_io_concurrency=50,
        actor_warm_spares=1,
    )
    scheduler = ProfilePoolScheduler(settings)
    profile_key = "environment-v1"
    scheduler._record_arrival(profile_key, now=100)

    assert scheduler._desired_actor_count(
        profile_key,
        [candidate(0, "SHARED_IO", 50)],
        pending_requests=1,
        now=100,
    ) == 3


async def test_reserve_uses_existing_capacity_without_waiting_for_scale_up(
    monkeypatch,
) -> None:
    class ReservableActor:
        def __init__(self, lease_id: str | None):
            self.try_reserve = FakeRemoteMethod(
                lambda *_args, **_kwargs: self._reserve(lease_id)
            )

        @staticmethod
        async def _reserve(lease_id: str | None):
            return lease_id

    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_target_io_concurrency=50,
        actor_max_io=100,
    )
    scheduler = ProfilePoolScheduler(settings)
    ready_actor = ReservableActor("lease-ready")
    candidates = [
        ActorCandidate(
            replica=0,
            handle=ReservableActor(None),
            status={"state": "SHARED_IO", "active_io": 50},
        ),
        ActorCandidate(
            replica=1,
            handle=ready_actor,
            status={"state": "IDLE", "active_io": 0},
        ),
    ]
    creation = asyncio.create_task(asyncio.Event().wait())

    async def pool_snapshot(_profile_key: str):
        return candidates

    async def schedule_actor_count(_profile_key: str, desired: int):
        assert desired == 3
        return [creation]

    monkeypatch.setattr(scheduler, "_pool_snapshot", pool_snapshot)
    monkeypatch.setattr(scheduler, "_schedule_actor_count", schedule_actor_count)
    try:
        assignment = await asyncio.wait_for(
            scheduler.reserve(io_target(), uuid.uuid4()),
            timeout=0.1,
        )
    finally:
        creation.cancel()
        await asyncio.gather(creation, return_exceptions=True)

    assert assignment.actor is ready_actor
    assert assignment.lease_id == "lease-ready"


async def test_cold_reserve_does_not_create_an_unrequested_second_actor(
    monkeypatch,
) -> None:
    class ReservableActor:
        def __init__(self):
            self.try_reserve = FakeRemoteMethod(self._reserve)

        @staticmethod
        async def _reserve(*_args, **_kwargs):
            return "cold-lease"

    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_hot_request_rate=0.5,
        actor_request_rate_window_seconds=10,
    )
    scheduler = ProfilePoolScheduler(settings)
    actor = ReservableActor()
    snapshots = iter(
        [
            [],
            [
                ActorCandidate(
                    replica=0,
                    handle=actor,
                    status={"state": "IDLE", "active_io": 0},
                )
            ],
        ]
    )
    scheduled: list[int] = []

    async def pool_snapshot(_profile_key: str):
        return next(snapshots)

    async def schedule_actor_count(_profile_key: str, desired: int):
        scheduled.append(desired)
        if desired == 1 and len(scheduled) == 1:
            return [asyncio.create_task(asyncio.sleep(0))]
        return []

    async def wait_for_creation(tasks):
        await asyncio.gather(*tasks)
        return True

    monkeypatch.setattr(scheduler, "_pool_snapshot", pool_snapshot)
    monkeypatch.setattr(scheduler, "_schedule_actor_count", schedule_actor_count)
    monkeypatch.setattr(scheduler, "_wait_for_actor_creation", wait_for_creation)

    assignment = await scheduler.reserve(io_target(), uuid.uuid4())

    assert assignment.lease_id == "cold-lease"
    assert scheduled == [1, 1]


async def test_actor_pool_status_reports_ready_and_creating_actors(
    monkeypatch,
) -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        actor_target_io_concurrency=50,
        actor_max_io=100,
    )
    scheduler = ProfilePoolScheduler(settings)
    profile_key = "environment-v1"
    scheduler._profiles[profile_key] = PoolProfile(
        key=profile_key,
        profile_ref="io@v1",
        worker_pool="default",
        runtime_env={},
    )
    candidates = [
        ActorCandidate(
            replica=0,
            handle=object(),
            status={
                "state": "SHARED_IO",
                "active_io": 4,
                "max_io": 100,
                "reserved_leases": 1,
                "running_leases": 3,
                "idle_for_seconds": 0.0,
            },
        ),
        ActorCandidate(
            replica=1,
            handle=object(),
            status={
                "state": "IDLE",
                "active_io": 0,
                "max_io": 100,
                "reserved_leases": 0,
                "running_leases": 0,
                "idle_for_seconds": 12.0,
            },
        ),
    ]
    creation = asyncio.create_task(asyncio.Event().wait())
    scheduler._actor_creation_tasks[(profile_key, 2)] = creation

    async def no_op(_profile_key: str):
        return None

    async def actor_statuses(_profile_key: str):
        return candidates

    monkeypatch.setattr(scheduler, "_prepare_pool", no_op)
    monkeypatch.setattr(scheduler, "_actor_statuses", actor_statuses)
    try:
        snapshots = await scheduler.actor_pool_statuses()
    finally:
        creation.cancel()
        await asyncio.gather(creation, return_exceptions=True)

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot["temperature"] == "HOT"
    assert snapshot["total_actors"] == 3
    assert snapshot["ready_actors"] == 2
    assert snapshot["creating_actors"] == 1
    assert snapshot["active_io"] == 4
    assert [actor["state"] for actor in snapshot["actors"]] == [
        "SHARED_IO",
        "IDLE",
        "CREATING",
    ]


async def test_unavailable_actor_is_removed_before_replacement() -> None:
    class DeadActor:
        def __init__(self):
            self.status = FakeRemoteMethod(self._status)

        @staticmethod
        async def _status():
            raise RayError("actor died")

    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
    )
    scheduler = ProfilePoolScheduler(settings)
    key = ("environment-v1", 0)
    scheduler._actors[key] = DeadActor()

    assert await scheduler._actor_statuses(key[0]) == []
    assert key not in scheduler._actors


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

    assert result.succeeded is True
    assert result.value == {"value": 42}
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

    assert (await scheduler.execute(target, uuid.uuid4(), {})).value == {"value": 42}
    assert (await scheduler.execute(target, uuid.uuid4(), {})).value == {"value": 42}

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
