from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import ray
from sc_hook import RemoteFunc

from pyscripts.runtime.admission import AdmissionController
from pyscripts.runtime.compute import ComputeTask
from pyscripts.runtime.loader import EndpointDefinition, VersionedRuntime
from pyscripts.runtime.output import ExecutionOutcome, replay_captured_output


class InternalRemoteCallError(RuntimeError):
    pass


class ActorRemoteCallBackend:
    """Revision-pinned backend used by Hook ``remote_call`` inside IO actors."""

    def __init__(
        self,
        *,
        runtime: VersionedRuntime,
        slots: asyncio.Semaphore,
        cache_root: str,
        environment_digest: str,
        service: str,
        revision: str,
        context: dict[str, Any],
        artifact_uri: str,
        artifact_digest: str,
        endpoint_manifest: list[dict[str, Any]],
        endpoints: list[EndpointDefinition],
        invocation_log_max_bytes: int,
        invocation_log_chunk_bytes: int,
        capture_stderr: bool,
        worker_pool: str | None,
        use_label_selector: bool,
        worker_pool_label_key: str,
        compute_task_num_cpus: float,
        compute_task_num_gpus: float,
    ):
        self.runtime = runtime
        self.slots = slots
        self.cache_root = cache_root
        self.environment_digest = environment_digest
        self.service = service
        self.revision = revision
        self.context = context
        self.artifact_uri = artifact_uri
        self.artifact_digest = artifact_digest
        self.endpoint_manifest = endpoint_manifest
        self.endpoints = endpoints
        self.invocation_log_max_bytes = invocation_log_max_bytes
        self.invocation_log_chunk_bytes = invocation_log_chunk_bytes
        self.capture_stderr = capture_stderr
        self.worker_pool = worker_pool
        self.use_label_selector = use_label_selector
        self.worker_pool_label_key = worker_pool_label_key
        self.compute_task_num_cpus = compute_task_num_cpus
        self.compute_task_num_gpus = compute_task_num_gpus

    async def remote_call(
        self,
        endpoint: RemoteFunc[Any, Any],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> Any:
        definition = next(
            (item for item in self.endpoints if item.id == endpoint.endpoint_id),
            None,
        )
        if definition is None:
            raise InternalRemoteCallError(
                f"endpoint {endpoint.endpoint_id!r} is not in the current revision"
            )
        if definition.task_type != endpoint.task_type.value:
            raise InternalRemoteCallError("remote endpoint task type mismatch")

        depth = int(self.context.get("internal_call_depth", 0)) + 1
        if depth > 16:
            raise InternalRemoteCallError("internal call depth exceeds 16")
        bound = endpoint.__signature__.bind(*args, **dict(kwargs))
        bound.apply_defaults()
        payload = dict(bound.arguments)
        child_context = {
            **self.context,
            "endpoint": definition.id,
            "transport": "internal",
            "internal_call_depth": depth,
        }

        if definition.task_type == "io":
            child_backend = self._with_context(child_context)
            result = await self.runtime.execute(
                self.service,
                self.revision,
                definition.id,
                child_context,
                payload,
                artifact_uri=self.artifact_uri,
                artifact_digest=self.artifact_digest,
                endpoints=self.endpoints,
                capture=True,
                remote_call_backend=child_backend,
            )
        else:
            async with self.slots:
                manifest = next(
                    item
                    for item in self.endpoint_manifest
                    if item["id"] == definition.id
                )
                options: dict[str, Any] = {
                    "num_cpus": manifest.get("num_cpus")
                    or self.compute_task_num_cpus,
                    "num_gpus": (
                        manifest["num_gpus"]
                        if manifest.get("num_gpus") is not None
                        else self.compute_task_num_gpus
                    ),
                }
                if self.use_label_selector and self.worker_pool is not None:
                    options["label_selector"] = {
                        self.worker_pool_label_key: self.worker_pool
                    }
                reference = ComputeTask.options(**options).remote(
                    transport="http",
                    cache_root=self.cache_root,
                    environment_digest=self.environment_digest,
                    service=self.service,
                    revision=self.revision,
                    endpoint_id=definition.id,
                    context=child_context,
                    payload=payload,
                    artifact_uri=self.artifact_uri,
                    artifact_digest=self.artifact_digest,
                    endpoint_manifest=[manifest],
                    capture=True,
                    invocation_log_max_bytes=self.invocation_log_max_bytes,
                    invocation_log_chunk_bytes=self.invocation_log_chunk_bytes,
                    capture_stderr=self.capture_stderr,
                )
                try:
                    result = await reference
                except asyncio.CancelledError:
                    ray.cancel(reference, force=True)
                    raise

        if not isinstance(result, ExecutionOutcome):
            return result
        replay_captured_output(result.logs)
        if not result.succeeded:
            detail = ": ".join(
                part for part in (result.error_type, result.error_message) if part
            )
            raise InternalRemoteCallError(detail or "internal endpoint failed")
        return result.value

    def _with_context(self, context: dict[str, Any]) -> ActorRemoteCallBackend:
        return ActorRemoteCallBackend(
            runtime=self.runtime,
            slots=self.slots,
            cache_root=self.cache_root,
            environment_digest=self.environment_digest,
            service=self.service,
            revision=self.revision,
            context=context,
            artifact_uri=self.artifact_uri,
            artifact_digest=self.artifact_digest,
            endpoint_manifest=self.endpoint_manifest,
            endpoints=self.endpoints,
            invocation_log_max_bytes=self.invocation_log_max_bytes,
            invocation_log_chunk_bytes=self.invocation_log_chunk_bytes,
            capture_stderr=self.capture_stderr,
            worker_pool=self.worker_pool,
            use_label_selector=self.use_label_selector,
            worker_pool_label_key=self.worker_pool_label_key,
            compute_task_num_cpus=self.compute_task_num_cpus,
            compute_task_num_gpus=self.compute_task_num_gpus,
        )


@ray.remote(max_concurrency=1000)
class UnifiedActor:
    """A hot-reloadable executor bound to one immutable runtime profile."""

    def __init__(
        self,
        runtime_profile: str,
        cache_root: str,
        max_io: int = 100,
        lease_ttl_seconds: float = 15.0,
        invocation_log_max_bytes: int = 64 * 1024,
        invocation_log_chunk_bytes: int = 4 * 1024,
        capture_stderr: bool = True,
        worker_pool: str | None = None,
        use_label_selector: bool = False,
        worker_pool_label_key: str = "pyscripts.worker-pool",
        compute_task_num_cpus: float = 1.0,
        compute_task_num_gpus: float = 0.0,
        internal_compute_max_pending: int = 100,
    ):
        self.runtime_profile = runtime_profile
        self.cache_root = cache_root
        self.invocation_log_max_bytes = invocation_log_max_bytes
        self.invocation_log_chunk_bytes = invocation_log_chunk_bytes
        self.capture_stderr = capture_stderr
        self.worker_pool = worker_pool
        self.use_label_selector = use_label_selector
        self.worker_pool_label_key = worker_pool_label_key
        self.compute_task_num_cpus = compute_task_num_cpus
        self.compute_task_num_gpus = compute_task_num_gpus
        self.remote_call_slots = asyncio.Semaphore(internal_compute_max_pending)
        self.runtime = VersionedRuntime(
            Path(cache_root),
            max_io=max_io,
            invocation_log_max_bytes=invocation_log_max_bytes,
            invocation_log_chunk_bytes=invocation_log_chunk_bytes,
            capture_stderr=capture_stderr,
        )
        self.admission = AdmissionController(max_io, lease_ttl_seconds)

    async def try_reserve(
        self,
        request_id: str,
        task_type: str,
        service: str,
        revision: str,
    ) -> str | None:
        if task_type != "io":
            raise ValueError("compute endpoints must run as Ray tasks")
        return await self.admission.try_reserve(
            request_id,
            task_type,
            service,
            revision,
        )

    async def execute(
        self,
        lease_id: str,
        endpoint_id: str,
        context: dict[str, Any],
        params: dict[str, Any],
        artifact_uri: str,
        artifact_digest: str,
        endpoint_manifest: list[dict[str, Any]],
    ) -> Any:
        lease = await self.admission.start(lease_id)
        try:
            endpoints = [
                EndpointDefinition.from_manifest(item) for item in endpoint_manifest
            ]
            endpoint = next(
                (item for item in endpoints if item.id == endpoint_id), None
            )
            if endpoint is None:
                raise ValueError(f"unknown endpoint: {endpoint_id}")
            if endpoint.task_type != lease.task_type:
                raise ValueError("lease task type does not match endpoint task type")
            backend = ActorRemoteCallBackend(
                runtime=self.runtime,
                slots=self.remote_call_slots,
                cache_root=self.cache_root,
                environment_digest=self.runtime_profile,
                service=lease.service,
                revision=lease.revision,
                context=dict(context),
                artifact_uri=artifact_uri,
                artifact_digest=artifact_digest,
                endpoint_manifest=endpoint_manifest,
                endpoints=endpoints,
                invocation_log_max_bytes=self.invocation_log_max_bytes,
                invocation_log_chunk_bytes=self.invocation_log_chunk_bytes,
                capture_stderr=self.capture_stderr,
                worker_pool=self.worker_pool,
                use_label_selector=self.use_label_selector,
                worker_pool_label_key=self.worker_pool_label_key,
                compute_task_num_cpus=self.compute_task_num_cpus,
                compute_task_num_gpus=self.compute_task_num_gpus,
            )
            return await self.runtime.execute(
                lease.service,
                lease.revision,
                endpoint_id,
                context,
                params,
                artifact_uri=artifact_uri,
                artifact_digest=artifact_digest,
                endpoints=endpoints,
                capture=True,
                remote_call_backend=backend,
            )
        finally:
            await self.admission.finish(lease_id)

    async def execute_grpc(
        self,
        lease_id: str,
        endpoint_id: str,
        context: dict[str, Any],
        payload: bytes,
        artifact_uri: str,
        artifact_digest: str,
        endpoint_manifest: list[dict[str, Any]],
    ) -> Any:
        lease = await self.admission.start(lease_id)
        try:
            endpoints = [
                EndpointDefinition.from_manifest(item) for item in endpoint_manifest
            ]
            endpoint = next(
                (item for item in endpoints if item.id == endpoint_id), None
            )
            if endpoint is None or endpoint.grpc is None:
                raise ValueError(f"unknown gRPC endpoint: {endpoint_id}")
            if endpoint.task_type != lease.task_type:
                raise ValueError("lease task type does not match endpoint task type")
            backend = ActorRemoteCallBackend(
                runtime=self.runtime,
                slots=self.remote_call_slots,
                cache_root=self.cache_root,
                environment_digest=self.runtime_profile,
                service=lease.service,
                revision=lease.revision,
                context=dict(context),
                artifact_uri=artifact_uri,
                artifact_digest=artifact_digest,
                endpoint_manifest=endpoint_manifest,
                endpoints=endpoints,
                invocation_log_max_bytes=self.invocation_log_max_bytes,
                invocation_log_chunk_bytes=self.invocation_log_chunk_bytes,
                capture_stderr=self.capture_stderr,
                worker_pool=self.worker_pool,
                use_label_selector=self.use_label_selector,
                worker_pool_label_key=self.worker_pool_label_key,
                compute_task_num_cpus=self.compute_task_num_cpus,
                compute_task_num_gpus=self.compute_task_num_gpus,
            )
            return await self.runtime.execute_grpc(
                lease.service,
                lease.revision,
                endpoint_id,
                context,
                payload,
                artifact_uri=artifact_uri,
                artifact_digest=artifact_digest,
                endpoints=endpoints,
                capture=True,
                remote_call_backend=backend,
            )
        finally:
            await self.admission.finish(lease_id)

    async def cancel_reservation(self, lease_id: str) -> bool:
        return await self.admission.cancel_reservation(lease_id)

    async def drain_actor(self) -> None:
        await self.admission.drain_actor()

    async def drain(self, service: str, revision: str) -> bool:
        return await self.runtime.drain(service, revision)

    async def status(self) -> dict[str, Any]:
        admission = await self.admission.status()
        runtime = await self.runtime.status()
        return {
            **admission,
            **runtime,
            "runtime_profile": self.runtime_profile,
        }
