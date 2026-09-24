from __future__ import annotations

import asyncio
import copy
import hashlib
import logging
import math
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import ray
from ray.exceptions import RayError

from pyscripts.config import Settings
from pyscripts.ray_client import ensure_ray
from pyscripts.repository import ResolvedEndpoint
from pyscripts.runtime.actor import UnifiedActor
from pyscripts.runtime.compute import ComputeTask, ComputeTaskResult
from pyscripts.runtime.output import ExecutionOutcome
from pyscripts.runtime.profiles import materialize_runtime_env
from pyscripts.storage import ArtifactStore, create_artifact_store

logger = logging.getLogger(__name__)


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


@dataclass(slots=True)
class ProfileTraffic:
    """Recent demand used for system-managed IO actor autoscaling."""

    arrivals: deque[float] = field(default_factory=deque)
    last_request_at: float | None = None
    duration_ewma_seconds: float = 1.0


class ProfilePoolScheduler:
    """Routes shared IO to actors and stateless compute to Ray tasks."""

    def __init__(
        self,
        settings: Settings,
        artifact_store: ArtifactStore | None = None,
    ):
        if settings.actor_target_io_concurrency > settings.actor_max_io:
            raise ValueError("target IO concurrency cannot exceed actor maximum IO")
        if (
            settings.actor_warm_idle_timeout_seconds
            < settings.actor_hot_idle_timeout_seconds
        ):
            raise ValueError("warm actor timeout cannot be shorter than hot timeout")
        self.settings = settings
        self.artifact_store = artifact_store or create_artifact_store(settings)
        self._actors: dict[tuple[str, int], Any] = {}
        self._profile_locks: dict[str, asyncio.Lock] = {}
        self._profiles: dict[str, PoolProfile] = {}
        self._profile_traffic: dict[str, ProfileTraffic] = {}
        self._legacy_checked_profiles: set[str] = set()
        self._actor_creation_tasks: dict[
            tuple[str, int], asyncio.Task[Any | None]
        ] = {}
        self._compute_limits: dict[str, asyncio.BoundedSemaphore] = {}
        self._compute_warm_nodes: dict[str, list[str]] = {}
        self._compute_warm_cursor: dict[str, int] = {}
        self._idle_reaper_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._idle_reaper_task is not None:
            return
        self._idle_reaper_task = asyncio.create_task(
            self._reap_idle_actors_forever(),
            name="pyscripts-io-actor-reaper",
        )

    async def close(self) -> None:
        reaper = self._idle_reaper_task
        self._idle_reaper_task = None
        if reaper is not None:
            reaper.cancel()
            try:
                await reaper
            except asyncio.CancelledError:
                pass
        creation_tasks = list(self._actor_creation_tasks.values())
        for task in creation_tasks:
            task.cancel()
        if creation_tasks:
            await asyncio.gather(*creation_tasks, return_exceptions=True)

    async def _reap_idle_actors_forever(self) -> None:
        while True:
            await asyncio.sleep(self.settings.actor_reaper_interval_seconds)
            try:
                await self.reap_idle_actors()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("IO actor idle reaper failed")

    async def reap_idle_actors(self) -> int:
        """Cool hot pools to one warm actor, then scale long-idle pools to zero."""
        reaped = 0
        for profile_key in tuple(self._profiles):
            await self._prepare_pool(profile_key)
            candidates = await self._actor_statuses(profile_key)
            current = self._actor_count(profile_key)
            desired = self._desired_actor_count(
                profile_key,
                candidates,
                pending_requests=0,
            )
            for candidate in sorted(
                candidates,
                key=lambda item: item.replica,
                reverse=True,
            ):
                status = candidate.status
                state = status.get("state")
                idle_for = float(status.get("idle_for_seconds", 0.0))
                is_final_warm_actor = current == 1
                timeout = (
                    self.settings.actor_warm_idle_timeout_seconds
                    if is_final_warm_actor
                    else self.settings.actor_hot_idle_timeout_seconds
                )
                idle_long_enough = state == "IDLE" and idle_for >= timeout
                drained_and_empty = (
                    state == "DRAINING"
                    and int(status.get("reserved_leases", 0)) == 0
                    and int(status.get("running_leases", 0)) == 0
                )
                if not idle_long_enough and not drained_and_empty:
                    continue
                if not is_final_warm_actor and current <= desired:
                    continue
                if is_final_warm_actor and not self._profile_is_cold(
                    profile_key,
                    timeout,
                ):
                    continue

                if state != "DRAINING":
                    await candidate.handle.drain_actor.remote()
                latest = await candidate.handle.status.remote()
                if (
                    int(latest.get("reserved_leases", 0)) > 0
                    or int(latest.get("running_leases", 0)) > 0
                ):
                    continue
                ray.kill(candidate.handle, no_restart=True)
                self._actors.pop((profile_key, candidate.replica), None)
                current -= 1
                reaped += 1
                logger.info(
                    "reaped IO actor",
                    extra={
                        "runtime_profile": profile_key,
                        "replica": candidate.replica,
                        "tier": "warm" if is_final_warm_actor else "hot",
                    },
                )
        return reaped

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
        execution_started = time.monotonic()
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
            return self._outcome(await reference)
        except BaseException:
            # If execute has not consumed the lease yet this releases it. If it is
            # already RUNNING, the actor owns release in its finally block.
            assignment.actor.cancel_reservation.remote(assignment.lease_id)
            raise
        finally:
            self._record_duration(
                target.environment_digest or target.runtime_profile,
                time.monotonic() - execution_started,
            )

    async def execute_grpc(
        self,
        target: ResolvedEndpoint,
        request_id: uuid.UUID,
        payload: bytes,
    ) -> ExecutionOutcome:
        if target.task_type == "compute":
            result = await self._execute_compute(
                target,
                request_id,
                payload,
                transport="grpc",
            )
            outcome = self._outcome(result)
            if outcome.succeeded and not isinstance(outcome.value, bytes):
                raise TypeError("gRPC compute task returned a non-bytes result")
            return outcome
        artifact_uri = self.artifact_store.distribution_uri(target.artifact_uri)
        assignment = await self.reserve(target, request_id)
        execution_started = time.monotonic()
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
            result = await reference
            outcome = self._outcome(result)
            if outcome.succeeded and not isinstance(outcome.value, bytes):
                raise TypeError("gRPC actor returned a non-bytes result")
            return outcome
        except BaseException:
            assignment.actor.cancel_reservation.remote(assignment.lease_id)
            raise
        finally:
            self._record_duration(
                target.environment_digest or target.runtime_profile,
                time.monotonic() - execution_started,
            )

    async def reserve(
        self,
        target: ResolvedEndpoint,
        request_id: uuid.UUID,
    ) -> LeaseAssignment:
        if target.task_type != "io":
            raise ValueError("only IO endpoints can reserve an actor lease")
        profile = self._register_profile(target)
        self._record_arrival(profile.key)
        deadline = time.monotonic() + self.settings.actor_queue_timeout_seconds

        while True:
            candidates = await self._pool_snapshot(profile.key)
            desired = self._desired_actor_count(
                profile.key,
                candidates,
                pending_requests=1,
            )
            creation_tasks = await self._schedule_actor_count(profile.key, desired)
            ordered = self._order_candidates(
                candidates,
                self.settings.actor_target_io_concurrency,
            )

            for candidate in ordered:
                lease_id = await candidate.handle.try_reserve.remote(
                    str(request_id),
                    target.task_type,
                    target.service_name,
                    target.revision,
                )
                if lease_id is not None:
                    return LeaseAssignment(candidate.handle, lease_id)

            # If autoscaling did not already start an Actor, rejection can be
            # caused by a capacity race between status() and try_reserve().
            # Add one replica in that case. On a Cold start, wait for the one
            # already being created instead of accidentally starting two.
            total = len(candidates) + len(creation_tasks)
            if not creation_tasks and total < self.settings.actor_max_per_profile:
                creation_tasks = await self._schedule_actor_count(
                    profile.key,
                    total + 1,
                )

            # Existing actors were tried first. Waiting for creation is only
            # necessary when none of them can accept this request; replenishing
            # a promoted Warm actor must never delay usable capacity.
            if creation_tasks:
                created = await self._wait_for_actor_creation(creation_tasks)
                if created:
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
                capture=True,
                invocation_log_max_bytes=self.settings.invocation_log_max_bytes,
                invocation_log_chunk_bytes=self.settings.invocation_log_chunk_bytes,
                capture_stderr=self.settings.capture_stderr,
            )
            result = await reference
            if isinstance(result, ComputeTaskResult):
                self._remember_compute_warm_node(profile.key, result.node_id)
                return self._outcome(result.value)
            # Test doubles and older workers may still return the value directly.
            return self._outcome(result)
        except asyncio.CancelledError:
            if reference is not None:
                ray.cancel(reference, force=True)
            raise
        finally:
            limit.release()

    def _register_profile(self, target: ResolvedEndpoint) -> PoolProfile:
        key = target.environment_digest or target.runtime_profile
        return self.register_runtime_profile(
            environment_digest=key,
            profile_ref=target.runtime_profile,
            worker_pool=target.worker_pool or target.runtime_profile,
            runtime_env=dict(target.runtime_env or {}),
            pip_source=target.pip_source,
        )

    def register_runtime_profile(
        self,
        *,
        environment_digest: str,
        profile_ref: str,
        worker_pool: str,
        runtime_env: dict[str, Any],
        pip_source: str = "default",
    ) -> PoolProfile:
        """Register immutable metadata so Cold pools are observable."""
        runtime_env = materialize_runtime_env(
            runtime_env,
            pip_source,
            self.settings,
        )
        profile = PoolProfile(
            key=environment_digest,
            profile_ref=profile_ref,
            worker_pool=worker_pool,
            runtime_env=runtime_env,
        )
        existing = self._profiles.setdefault(environment_digest, profile)
        if existing != profile:
            raise RuntimeError(
                f"conflicting runtime profile metadata for {environment_digest}"
            )
        self._profile_traffic.setdefault(environment_digest, ProfileTraffic())
        return existing

    async def _pool_snapshot(self, runtime_profile: str) -> list[ActorCandidate]:
        await self._prepare_pool(runtime_profile)
        return await self._actor_statuses(runtime_profile)

    async def _actor_statuses(
        self,
        runtime_profile: str,
    ) -> list[ActorCandidate]:
        actors = [
            (replica, handle)
            for (profile, replica), handle in self._actors.items()
            if profile == runtime_profile
        ]
        unavailable: list[tuple[int, Any]] = []

        async def get_status(replica: int, handle: Any) -> ActorCandidate | None:
            try:
                status = await handle.status.remote()
            except RayError:
                unavailable.append((replica, handle))
                return None
            return ActorCandidate(replica, handle, status)

        statuses = await asyncio.gather(
            *(get_status(replica, handle) for replica, handle in actors)
        )
        for replica, handle in unavailable:
            key = (runtime_profile, replica)
            if self._actors.get(key) is handle:
                self._actors.pop(key, None)
        return [item for item in statuses if item is not None]

    async def _prepare_pool(self, runtime_profile: str) -> None:
        await self._ensure_ray()
        lock = self._profile_locks.setdefault(runtime_profile, asyncio.Lock())
        async with lock:
            if runtime_profile not in self._legacy_checked_profiles:
                await self._retire_legacy_actors(runtime_profile)
                self._legacy_checked_profiles.add(runtime_profile)
            await self._discover_actors(runtime_profile)

    async def _schedule_actor_count(
        self,
        runtime_profile: str,
        desired: int,
    ) -> list[asyncio.Task[Any | None]]:
        """Start missing Actors without waiting for their Runtime Env setup."""
        desired = min(desired, self.settings.actor_max_per_profile)
        lock = self._profile_locks.setdefault(runtime_profile, asyncio.Lock())
        async with lock:
            occupied = {
                replica
                for profile, replica in self._actors
                if profile == runtime_profile
            }
            occupied.update(
                replica
                for profile, replica in self._actor_creation_tasks
                if profile == runtime_profile
            )
            while len(occupied) < desired:
                replica = next(
                    (
                        value
                        for value in range(self.settings.actor_max_per_profile)
                        if value not in occupied
                    ),
                    None,
                )
                if replica is None:
                    break
                key = (runtime_profile, replica)
                task = asyncio.create_task(
                    self._create_actor_replica(runtime_profile, replica),
                    name=f"pyscripts-io-actor-create-{replica}",
                )
                self._actor_creation_tasks[key] = task
                occupied.add(replica)
        return [
            task
            for (profile, _), task in self._actor_creation_tasks.items()
            if profile == runtime_profile and not task.done()
        ]

    async def _wait_for_actor_creation(
        self,
        tasks: list[asyncio.Task[Any | None]],
    ) -> bool:
        pending = [task for task in tasks if not task.done()]
        if not pending:
            return False
        done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                if task.result() is not None:
                    return True
            except asyncio.CancelledError:
                continue
        return False

    async def _retire_legacy_actors(self, runtime_profile: str) -> None:
        """Drain and reclaim detached actors using an older wire protocol."""
        digest = hashlib.sha256(runtime_profile.encode("utf-8")).hexdigest()[:20]
        for replica in range(self.settings.actor_max_per_profile):
            names = (
                f"pyscripts-runtime-{digest}-{replica}",
                f"pyscripts-runtime-v2-{digest}-{replica}",
                f"pyscripts-runtime-v3-{digest}-{replica}",
                f"pyscripts-runtime-v4-{digest}-{replica}",
                f"pyscripts-runtime-v5-{digest}-{replica}",
                f"pyscripts-runtime-v6-{digest}-{replica}",
            )
            for actor_name in names:
                try:
                    actor = ray.get_actor(
                        actor_name,
                        namespace=self.settings.ray_namespace,
                    )
                except ValueError:
                    continue
                try:
                    await actor.drain_actor.remote()
                    status = await actor.status.remote()
                except RayError:
                    continue
                if (
                    int(status.get("running_leases", 0)) == 0
                    and int(status.get("reserved_leases", 0)) == 0
                ):
                    ray.kill(actor, no_restart=True)

    async def _create_actor_replica(
        self,
        runtime_profile: str,
        replica: int,
    ) -> Any | None:
        key = (runtime_profile, replica)
        actor_name = self._actor_name(runtime_profile, replica)
        try:
            try:
                actor = ray.get_actor(actor_name, namespace=self.settings.ray_namespace)
            except ValueError:
                actor = await self._create_actor(runtime_profile, actor_name)
            await actor.status.remote()
            self._actors[key] = actor
            logger.info(
                "IO actor became ready: profile=%s replica=%d",
                runtime_profile,
                replica,
            )
            return actor
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "IO actor creation failed: profile=%s replica=%d",
                runtime_profile,
                replica,
            )
            return None
        finally:
            current = asyncio.current_task()
            if self._actor_creation_tasks.get(key) is current:
                self._actor_creation_tasks.pop(key, None)

    async def _create_actor(self, runtime_profile: str, actor_name: str) -> Any:
        profile = self._profiles[runtime_profile]
        runtime_env = copy.deepcopy(profile.runtime_env)
        options: dict[str, Any] = {
            "name": actor_name,
            "namespace": self.settings.ray_namespace,
            "lifetime": "detached",
            "num_cpus": self.settings.actor_num_cpus,
            # The scheduler owns actor recovery. Letting Ray restart a
            # detached actor indefinitely can resurrect an old actor on a new
            # worker after its job-level py_modules package has expired.
            "max_restarts": 0,
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
                self.settings.invocation_log_max_bytes,
                self.settings.invocation_log_chunk_bytes,
                self.settings.capture_stderr,
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
        creation_tasks = [
            task
            for (profile, _), task in self._actor_creation_tasks.items()
            if profile == environment_digest
        ]
        for task in creation_tasks:
            task.cancel()
        if creation_tasks:
            await asyncio.gather(*creation_tasks, return_exceptions=True)
        await self._discover_actors(environment_digest)
        actors = [
            (key, handle)
            for key, handle in self._actors.items()
            if key[0] == environment_digest
        ]
        if not actors:
            self._profiles.pop(environment_digest, None)
            self._profile_traffic.pop(environment_digest, None)
            self._legacy_checked_profiles.discard(environment_digest)
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
        self._profile_traffic.pop(environment_digest, None)
        self._legacy_checked_profiles.discard(environment_digest)
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
            if key in self._actors or key in self._actor_creation_tasks:
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

    async def actor_pool_statuses(self) -> list[dict[str, Any]]:
        """Return a consistent management snapshot for every known profile."""
        snapshots: list[dict[str, Any]] = []
        for profile_key in sorted(self._profiles):
            profile = self._profiles[profile_key]
            await self._prepare_pool(profile_key)
            candidates = await self._actor_statuses(profile_key)
            creating_replicas = sorted(
                replica
                for (key, replica), task in self._actor_creation_tasks.items()
                if key == profile_key and not task.done()
            )
            now = time.monotonic()
            rate = self._request_rate(profile_key, now)
            traffic = self._profile_traffic.setdefault(
                profile_key,
                ProfileTraffic(),
            )
            last_request_ago = (
                max(0.0, now - traffic.last_request_at)
                if traffic.last_request_at is not None
                else None
            )
            desired = self._desired_actor_count(
                profile_key,
                candidates,
                pending_requests=0,
                now=now,
            )
            active_io = sum(
                int(candidate.status.get("active_io", 0))
                for candidate in candidates
            )
            reserved = sum(
                int(candidate.status.get("reserved_leases", 0))
                for candidate in candidates
            )
            running = sum(
                int(candidate.status.get("running_leases", 0))
                for candidate in candidates
            )
            hot_count = sum(
                candidate.status.get("state")
                in {"SHARED_IO", "SATURATED_IO"}
                for candidate in candidates
            )
            warm_count = sum(
                candidate.status.get("state") == "IDLE"
                for candidate in candidates
            )
            draining_count = sum(
                candidate.status.get("state") == "DRAINING"
                for candidate in candidates
            )
            total = len(candidates) + len(creating_replicas)
            is_cold = total == 0 and (
                last_request_ago is None
                or last_request_ago
                >= self.settings.actor_warm_idle_timeout_seconds
            )
            if is_cold:
                temperature = "COLD"
                desired = 0
            elif (
                hot_count > 0
                or rate >= self.settings.actor_hot_request_rate
                or desired > 1
            ):
                temperature = "HOT"
            else:
                temperature = "WARM"

            actors = [
                {
                    "replica": candidate.replica,
                    "name": self._actor_name(profile_key, candidate.replica),
                    "tier": (
                        "WARM"
                        if candidate.status.get("state") == "IDLE"
                        else (
                            "DRAINING"
                            if candidate.status.get("state") == "DRAINING"
                            else "HOT"
                        )
                    ),
                    "state": str(candidate.status.get("state", "UNKNOWN")),
                    "active_io": int(candidate.status.get("active_io", 0)),
                    "max_io": int(
                        candidate.status.get("max_io", self.settings.actor_max_io)
                    ),
                    "reserved_leases": int(
                        candidate.status.get("reserved_leases", 0)
                    ),
                    "running_leases": int(
                        candidate.status.get("running_leases", 0)
                    ),
                    "idle_for_seconds": float(
                        candidate.status.get("idle_for_seconds", 0.0)
                    ),
                }
                for candidate in sorted(candidates, key=lambda item: item.replica)
            ]
            actors.extend(
                {
                    "replica": replica,
                    "name": self._actor_name(profile_key, replica),
                    "tier": "WARM",
                    "state": "CREATING",
                    "active_io": 0,
                    "max_io": self.settings.actor_max_io,
                    "reserved_leases": 0,
                    "running_leases": 0,
                    "idle_for_seconds": 0.0,
                }
                for replica in creating_replicas
            )
            snapshots.append(
                {
                    "environment_digest": profile_key,
                    "runtime_profile": profile.profile_ref,
                    "worker_pool": profile.worker_pool,
                    "temperature": temperature,
                    "desired_actors": desired,
                    "total_actors": total,
                    "ready_actors": len(candidates),
                    "creating_actors": len(creating_replicas),
                    "hot_actors": hot_count,
                    "warm_actors": warm_count,
                    "draining_actors": draining_count,
                    "active_io": active_io,
                    "reserved_leases": reserved,
                    "running_leases": running,
                    "request_rate_per_second": rate,
                    "average_duration_seconds": traffic.duration_ewma_seconds,
                    "last_request_ago_seconds": last_request_ago,
                    "target_io_capacity": (
                        len(candidates)
                        * self.settings.actor_target_io_concurrency
                    ),
                    "max_io_capacity": (
                        len(candidates) * self.settings.actor_max_io
                    ),
                    "actors": actors,
                }
            )
        return snapshots

    def _record_arrival(self, profile_key: str, now: float | None = None) -> None:
        observed_at = time.monotonic() if now is None else now
        traffic = self._profile_traffic.setdefault(profile_key, ProfileTraffic())
        traffic.arrivals.append(observed_at)
        traffic.last_request_at = observed_at
        self._trim_arrivals(traffic, observed_at)

    def _record_duration(self, profile_key: str, duration_seconds: float) -> None:
        traffic = self._profile_traffic.setdefault(profile_key, ProfileTraffic())
        alpha = self.settings.actor_duration_ewma_alpha
        duration = max(0.001, duration_seconds)
        traffic.duration_ewma_seconds = (
            alpha * duration + (1 - alpha) * traffic.duration_ewma_seconds
        )

    def _request_rate(self, profile_key: str, now: float | None = None) -> float:
        observed_at = time.monotonic() if now is None else now
        traffic = self._profile_traffic.setdefault(profile_key, ProfileTraffic())
        self._trim_arrivals(traffic, observed_at)
        return len(traffic.arrivals) / self.settings.actor_request_rate_window_seconds

    def _trim_arrivals(self, traffic: ProfileTraffic, now: float) -> None:
        cutoff = now - self.settings.actor_request_rate_window_seconds
        while traffic.arrivals and traffic.arrivals[0] <= cutoff:
            traffic.arrivals.popleft()

    def _desired_actor_count(
        self,
        profile_key: str,
        candidates: list[ActorCandidate],
        *,
        pending_requests: int,
        now: float | None = None,
    ) -> int:
        active = sum(
            int(item.status.get("active_io", 0))
            for item in candidates
            if item.status.get("state") != "DRAINING"
        )
        rate = self._request_rate(profile_key, now)
        traffic = self._profile_traffic.setdefault(profile_key, ProfileTraffic())
        predicted_concurrency = max(
            active + pending_requests,
            math.ceil(rate * traffic.duration_ewma_seconds),
        )
        hot_actors = max(
            1,
            math.ceil(
                predicted_concurrency / self.settings.actor_target_io_concurrency
            ),
        )
        is_hot = (
            rate >= self.settings.actor_hot_request_rate or hot_actors > 1
        )
        desired = hot_actors + (
            self.settings.actor_warm_spares if is_hot else 0
        )
        return min(desired, self.settings.actor_max_per_profile)

    def _profile_is_cold(self, profile_key: str, timeout: float) -> bool:
        traffic = self._profile_traffic.get(profile_key)
        if traffic is None or traffic.last_request_at is None:
            return True
        return time.monotonic() - traffic.last_request_at >= timeout

    @staticmethod
    def _outcome(value: Any) -> ExecutionOutcome:
        """Normalize old workers and test doubles during rolling upgrades."""
        if isinstance(value, ExecutionOutcome):
            return value
        return ExecutionOutcome(succeeded=True, value=value)

    @staticmethod
    def _order_candidates(
        candidates: list[ActorCandidate],
        target_concurrency: int | None = None,
    ) -> list[ActorCandidate]:
        eligible = [
            item for item in candidates if item.status["state"] in {"SHARED_IO", "IDLE"}
        ]

        def priority(item: ActorCandidate) -> tuple[int, int, int]:
            state = item.status["state"]
            active = int(item.status["active_io"])
            if state == "SHARED_IO" and (
                target_concurrency is None or active < target_concurrency
            ):
                # Pack hot actors until the target concurrency is reached.
                return (0, -active, item.replica)
            if state == "IDLE":
                # Promote a prepared warm actor before overloading a hot actor.
                return (1, 0, item.replica)
            return (2, active, item.replica)

        return sorted(
            eligible,
            key=priority,
        )

    @staticmethod
    def _actor_name(runtime_profile: str, replica: int) -> str:
        digest = hashlib.sha256(runtime_profile.encode("utf-8")).hexdigest()[:20]
        # Version the detached actor protocol. V7 transfers restart ownership
        # from Ray to the scheduler so old actors are replaced during rollout.
        # Older actors may finish in-flight work before they are reclaimed.
        return f"pyscripts-runtime-v7-{digest}-{replica}"
