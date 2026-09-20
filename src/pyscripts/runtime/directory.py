from __future__ import annotations

import asyncio
import copy
import hashlib
import time
import uuid
from dataclasses import dataclass
from typing import Any

import ray
from ray.exceptions import RayError

from pyscripts.config import Settings
from pyscripts.ray_client import ensure_ray
from pyscripts.repository import ResolvedEndpoint
from pyscripts.runtime.actor import UnifiedActor
from pyscripts.runtime.compute import ComputeTask, ComputeTaskResult
from pyscripts.storage import ArtifactStore, create_artifact_store


class PoolOverloadedError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ActorCandidate:
    replica: int
    handle: Any
    status: dict[str, Any]


@dataclass(frozen=True, slots=True)
class LeaseAssignment:
    actor: Any
    lease_id: str


@dataclass(frozen=True, slots=True)
class PoolProfile:
    key: str
    profile_ref: str
    worker_pool: str
    runtime_env: dict[str, Any]


class ProfilePoolScheduler:
    """Routes shared IO to actors and stateless compute to Ray tasks."""

    def __init__(
        self,
        settings: Settings,
        artifact_store: ArtifactStore | None = None,
    ):
        if settings.actor_replicas_per_profile > settings.actor_max_per_profile:
            raise ValueError("minimum actor replicas cannot exceed maximum replicas")
        self.settings = settings
        self.artifact_store = artifact_store or create_artifact_store(settings)
        self._actors: dict[tuple[str, int], Any] = {}
        self._profile_locks: dict[str, asyncio.Lock] = {}
        self._profiles: dict[str, PoolProfile] = {}
        self._compute_limits: dict[str, asyncio.BoundedSemaphore] = {}
        self._compute_warm_nodes: dict[str, list[str]] = {}
        self._compute_warm_cursor: dict[str, int] = {}

    async def execute(
        self,
        target: ResolvedEndpoint,
        request_id: uuid.UUID,
        params: dict[str, Any],
    ) -> Any:
        if target.task_type == "compute":
            return await self._execute_compute(
                target,
                request_id,
                params,
                transport="http",
            )
        artifact_uri = self.artifact_store.distribution_uri(target.artifact_uri)
        assignment = await self.reserve(target, request_id)
        reference = assignment.actor.execute.remote(
            assignment.lease_id,
            target.endpoint_id,
            {"request_id": str(request_id)},
            params,
            artifact_uri,
            target.artifact_digest,
            target.endpoint_manifest,
        )
        try:
            return await reference
        except BaseException:
            # If execute has not consumed the lease yet this releases it. If it is
            # already RUNNING, the actor owns release in its finally block.
            assignment.actor.cancel_reservation.remote(assignment.lease_id)
            raise

    async def execute_grpc(
        self,
        target: ResolvedEndpoint,
        request_id: uuid.UUID,
        payload: bytes,
    ) -> bytes:
        if target.task_type == "compute":
            result = await self._execute_compute(
                target,
                request_id,
                payload,
                transport="grpc",
            )
            if not isinstance(result, bytes):
                raise TypeError("gRPC compute task returned a non-bytes result")
            return result
        artifact_uri = self.artifact_store.distribution_uri(target.artifact_uri)
        assignment = await self.reserve(target, request_id)
        reference = assignment.actor.execute_grpc.remote(
            assignment.lease_id,
            target.endpoint_id,
            {
                "request_id": str(request_id),
                "service": target.service_name,
                "revision": target.revision,
                "endpoint": target.endpoint_id,
                "transport": "grpc",
            },
            payload,
            artifact_uri,
            target.artifact_digest,
            target.endpoint_manifest,
        )
        try:
            return await reference
        except BaseException:
            assignment.actor.cancel_reservation.remote(assignment.lease_id)
            raise

    async def reserve(
        self,
        target: ResolvedEndpoint,
        request_id: uuid.UUID,
    ) -> LeaseAssignment:
        if target.task_type != "io":
            raise ValueError("only IO endpoints can reserve an actor lease")
        profile = self._register_profile(target)
        deadline = time.monotonic() + self.settings.actor_queue_timeout_seconds

        while True:
            candidates = await self._pool_snapshot(profile.key)
            ordered = self._order_candidates(candidates)

            shared = [
                item for item in ordered if item.status["state"] == "SHARED_IO"
            ]
            idle_count = sum(item.status["state"] == "IDLE" for item in candidates)
            if (
                not shared
                and idle_count <= self.settings.actor_min_idle_per_profile
            ):
                created = await self._create_next_actor(profile.key)
                if created is not None:
                    candidates = await self._pool_snapshot(profile.key)
                    ordered = self._order_candidates(candidates)

            for candidate in ordered:
                lease_id = await candidate.handle.try_reserve.remote(
                    str(request_id),
                    target.task_type,
                    target.service_name,
                    target.revision,
                )
                if lease_id is not None:
                    return LeaseAssignment(candidate.handle, lease_id)

            if len(candidates) < self.settings.actor_max_per_profile:
                created = await self._create_next_actor(profile.key)
                if created is not None:
                    continue

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PoolOverloadedError(
                    f"runtime profile {target.runtime_profile!r} has no capacity "
                    f"for {target.task_type}"
                )
            await asyncio.sleep(
                min(self.settings.actor_scheduler_poll_seconds, remaining)
            )

    async def _execute_compute(
        self,
        target: ResolvedEndpoint,
        request_id: uuid.UUID,
        payload: dict[str, Any] | bytes,
        *,
        transport: str,
    ) -> Any:
        profile = self._register_profile(target)
        await self._ensure_ray()
        limit = self._compute_limits.setdefault(
            profile.key,
            asyncio.BoundedSemaphore(
                self.settings.compute_max_pending_per_profile
            ),
        )
        try:
            await asyncio.wait_for(
                limit.acquire(),
                timeout=self.settings.actor_queue_timeout_seconds,
            )
        except TimeoutError as error:
            raise PoolOverloadedError(
                f"runtime profile {target.runtime_profile!r} compute queue is full"
            ) from error

        runtime_env = copy.deepcopy(profile.runtime_env)
        options: dict[str, Any] = {
            "runtime_env": runtime_env,
            "num_cpus": target.num_cpus or self.settings.compute_task_num_cpus,
            "num_gpus": (
                target.num_gpus
                if target.num_gpus is not None
                else self.settings.compute_task_num_gpus
            ),
            "name": (
                f"pyscripts:{target.service_name}:{target.endpoint_id}:"
                f"{target.revision}"
            ),
        }
        if self.settings.ray_use_label_selector:
            pool_selector = {
                self.settings.ray_worker_pool_label_key: profile.worker_pool
            }
            warm_node = self._next_compute_warm_node(profile.key)
            if self.settings.compute_profile_affinity and warm_node is not None:
                options["label_selector"] = {
                    **pool_selector,
                    self.settings.ray_node_id_label_key: warm_node,
                }
                # The exact node is a cache-locality preference. The container
                # worker pool remains a hard constraint in every fallback.
                options["fallback_strategy"] = [
                    {"label_selector": pool_selector}
                ]
            else:
                options["label_selector"] = pool_selector
        reference: Any | None = None
        try:
            artifact_uri = self.artifact_store.distribution_uri(target.artifact_uri)
            reference = ComputeTask.options(**options).remote(
                transport=transport,
                cache_root=str(self.settings.actor_cache_root),
                environment_digest=profile.key,
                service=target.service_name,
                revision=target.revision,
                endpoint_id=target.endpoint_id,
                context={
                    "request_id": str(request_id),
                    "service": target.service_name,
                    "revision": target.revision,
                    "endpoint": target.endpoint_id,
                    "transport": transport,
                },
                payload=payload,
                artifact_uri=artifact_uri,
                artifact_digest=target.artifact_digest,
                endpoint_manifest=[
                    item
                    for item in target.endpoint_manifest
                    if item["id"] == target.endpoint_id
                ],
                include_metadata=True,
            )
            result = await reference
            if isinstance(result, ComputeTaskResult):
                self._remember_compute_warm_node(profile.key, result.node_id)
                return result.value
            # Test doubles and older workers may still return the value directly.
            return result
        except asyncio.CancelledError:
            if reference is not None:
                ray.cancel(reference, force=True)
            raise
        finally:
            limit.release()

    def _register_profile(self, target: ResolvedEndpoint) -> PoolProfile:
        key = target.environment_digest or target.runtime_profile
        profile = PoolProfile(
            key=key,
            profile_ref=target.runtime_profile,
            worker_pool=target.worker_pool or target.runtime_profile,
            runtime_env=dict(target.runtime_env or {}),
        )
        existing = self._profiles.setdefault(key, profile)
        if existing != profile:
            raise RuntimeError(f"conflicting runtime profile metadata for {key}")
        return existing

    async def _pool_snapshot(self, runtime_profile: str) -> list[ActorCandidate]:
        await self._ensure_minimum_pool(runtime_profile)
        actors = [
            (replica, handle)
            for (profile, replica), handle in self._actors.items()
            if profile == runtime_profile
        ]

        async def get_status(replica: int, handle: Any) -> ActorCandidate | None:
            try:
                status = await handle.status.remote()
            except RayError:
                return None
            return ActorCandidate(replica, handle, status)

        statuses = await asyncio.gather(
            *(get_status(replica, handle) for replica, handle in actors)
        )
        return [item for item in statuses if item is not None]

    async def _ensure_minimum_pool(self, runtime_profile: str) -> None:
        await self._ensure_ray()
        lock = self._profile_locks.setdefault(runtime_profile, asyncio.Lock())
        async with lock:
            await self._discover_actors(runtime_profile)
            current = self._actor_count(runtime_profile)
            for _ in range(current, self.settings.actor_replicas_per_profile):
                await self._create_next_actor_unlocked(runtime_profile)

    async def _create_next_actor(self, runtime_profile: str) -> Any | None:
        lock = self._profile_locks.setdefault(runtime_profile, asyncio.Lock())
        async with lock:
            await self._discover_actors(runtime_profile)
            return await self._create_next_actor_unlocked(runtime_profile)

    async def _create_next_actor_unlocked(self, runtime_profile: str) -> Any | None:
        for replica in range(self.settings.actor_max_per_profile):
            key = (runtime_profile, replica)
            if key in self._actors:
                continue
            actor_name = self._actor_name(runtime_profile, replica)
            try:
                actor = ray.get_actor(actor_name, namespace=self.settings.ray_namespace)
            except ValueError:
                actor = await self._create_actor(runtime_profile, actor_name)
            self._actors[key] = actor
            return actor
        return None

    async def _create_actor(self, runtime_profile: str, actor_name: str) -> Any:
        profile = self._profiles[runtime_profile]
        runtime_env = copy.deepcopy(profile.runtime_env)
        options: dict[str, Any] = {
            "name": actor_name,
            "namespace": self.settings.ray_namespace,
            "lifetime": "detached",
            "num_cpus": self.settings.actor_num_cpus,
            "max_restarts": -1,
            "runtime_env": runtime_env,
        }
        if self.settings.ray_use_label_selector:
            options["label_selector"] = {
                self.settings.ray_worker_pool_label_key: profile.worker_pool
            }
        try:
            return UnifiedActor.options(**options).remote(
                profile.profile_ref,
                str(self.settings.actor_cache_root / actor_name),
                self.settings.actor_max_io,
                self.settings.actor_lease_ttl_seconds,
            )
        except ValueError:
            # Another gateway/controller may have won the named-actor race.
            return ray.get_actor(actor_name, namespace=self.settings.ray_namespace)

    async def retire_profile(
        self,
        environment_digest: str,
        timeout_seconds: float = 300.0,
    ) -> bool:
        """Stop new admission, then remove the profile's actors once leases finish."""
        await self._ensure_ray()
        await self._discover_actors(environment_digest)
        actors = [
            (key, handle)
            for key, handle in self._actors.items()
            if key[0] == environment_digest
        ]
        if not actors:
            self._profiles.pop(environment_digest, None)
            self._compute_limits.pop(environment_digest, None)
            self._compute_warm_nodes.pop(environment_digest, None)
            self._compute_warm_cursor.pop(environment_digest, None)
            return True
        await asyncio.gather(
            *(handle.drain_actor.remote() for _, handle in actors),
            return_exceptions=True,
        )
        deadline = time.monotonic() + timeout_seconds
        while True:
            statuses = await asyncio.gather(
                *(handle.status.remote() for _, handle in actors),
                return_exceptions=True,
            )
            busy = any(
                not isinstance(item, BaseException)
                and (
                    int(item["running_leases"]) > 0
                    or int(item["reserved_leases"]) > 0
                )
                for item in statuses
            )
            if not busy:
                break
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(self.settings.actor_scheduler_poll_seconds)
        for key, handle in actors:
            ray.kill(handle, no_restart=True)
            self._actors.pop(key, None)
        self._profiles.pop(environment_digest, None)
        self._compute_limits.pop(environment_digest, None)
        self._compute_warm_nodes.pop(environment_digest, None)
        self._compute_warm_cursor.pop(environment_digest, None)
        return True

    def _next_compute_warm_node(self, profile_key: str) -> str | None:
        nodes = self._compute_warm_nodes.get(profile_key)
        if not nodes:
            return None
        cursor = self._compute_warm_cursor.get(profile_key, 0)
        self._compute_warm_cursor[profile_key] = cursor + 1
        return nodes[cursor % len(nodes)]

    def _remember_compute_warm_node(self, profile_key: str, node_id: str) -> None:
        if not node_id:
            return
        nodes = self._compute_warm_nodes.setdefault(profile_key, [])
        if node_id in nodes:
            return
        nodes.append(node_id)
        del nodes[: -self.settings.compute_warm_nodes_per_profile]

    async def _discover_actors(self, runtime_profile: str) -> None:
        for replica in range(self.settings.actor_max_per_profile):
            key = (runtime_profile, replica)
            if key in self._actors:
                continue
            actor_name = self._actor_name(runtime_profile, replica)
            try:
                self._actors[key] = ray.get_actor(
                    actor_name, namespace=self.settings.ray_namespace
                )
            except ValueError:
                continue

    async def _ensure_ray(self) -> None:
        await ensure_ray(self.settings)

    def _actor_count(self, runtime_profile: str) -> int:
        return sum(profile == runtime_profile for profile, _ in self._actors)

    @staticmethod
    def _order_candidates(candidates: list[ActorCandidate]) -> list[ActorCandidate]:
        eligible = [
            item for item in candidates if item.status["state"] in {"SHARED_IO", "IDLE"}
        ]
        return sorted(
            eligible,
            key=lambda item: (
                item.status["state"] != "SHARED_IO",
                -int(item.status["active_io"]),
                item.replica,
            ),
        )

    @staticmethod
    def _actor_name(runtime_profile: str, replica: int) -> str:
        digest = hashlib.sha256(runtime_profile.encode("utf-8")).hexdigest()[:20]
        return f"pyscripts-runtime-{digest}-{replica}"
