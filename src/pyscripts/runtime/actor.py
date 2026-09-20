from __future__ import annotations

from pathlib import Path
from typing import Any

import ray

from pyscripts.runtime.admission import AdmissionController
from pyscripts.runtime.loader import EndpointDefinition, VersionedRuntime


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
    ):
        self.runtime_profile = runtime_profile
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
        endpoint_manifest: list[dict[str, str]],
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
