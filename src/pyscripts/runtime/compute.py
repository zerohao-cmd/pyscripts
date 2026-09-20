from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import ray

from pyscripts.runtime.loader import EndpointDefinition, VersionedRuntime

Transport = Literal["http", "grpc"]

_event_loop: asyncio.AbstractEventLoop | None = None
_runtimes: dict[tuple[str, str, int, int, bool], VersionedRuntime] = {}


@dataclass(frozen=True, slots=True)
class ComputeTaskResult:
    value: Any
    node_id: str


def _worker_runtime(
    cache_root: str,
    environment_digest: str,
    invocation_log_max_bytes: int,
    invocation_log_chunk_bytes: int,
    capture_stderr: bool,
) -> VersionedRuntime:
    key = (
        cache_root,
        environment_digest,
        invocation_log_max_bytes,
        invocation_log_chunk_bytes,
        capture_stderr,
    )
    runtime = _runtimes.get(key)
    if runtime is None:
        root = Path(cache_root)
        runtime = VersionedRuntime(
            root / "compute-workers" / environment_digest / str(os.getpid()),
            max_io=1,
            artifact_cache_root=root / "artifacts",
            retain_extracted_on_unload=True,
            invocation_log_max_bytes=invocation_log_max_bytes,
            invocation_log_chunk_bytes=invocation_log_chunk_bytes,
            capture_stderr=capture_stderr,
        )
        _runtimes[key] = runtime
    return runtime


def _loop() -> asyncio.AbstractEventLoop:
    global _event_loop
    if _event_loop is None or _event_loop.is_closed():
        _event_loop = asyncio.new_event_loop()
    return _event_loop


def run_compute_task(
    *,
    transport: Transport,
    cache_root: str,
    environment_digest: str,
    service: str,
    revision: str,
    endpoint_id: str,
    context: dict[str, Any],
    payload: dict[str, Any] | bytes,
    artifact_uri: str,
    artifact_digest: str,
    endpoint_manifest: list[dict[str, Any]],
    include_metadata: bool = False,
    capture: bool = False,
    invocation_log_max_bytes: int = 64 * 1024,
    invocation_log_chunk_bytes: int = 4 * 1024,
    capture_stderr: bool = True,
) -> Any:
    """Execute one immutable compute snapshot inside a reusable Ray worker."""
    runtime = _worker_runtime(
        cache_root,
        environment_digest,
        invocation_log_max_bytes,
        invocation_log_chunk_bytes,
        capture_stderr,
    )
    endpoints = [EndpointDefinition.from_manifest(item) for item in endpoint_manifest]
    loop = _loop()
    try:
        if transport == "grpc":
            if not isinstance(payload, bytes):
                raise TypeError("gRPC compute payload must be bytes")
            operation = runtime.execute_grpc(
                service,
                revision,
                endpoint_id,
                context,
                payload,
                artifact_uri=artifact_uri,
                artifact_digest=artifact_digest,
                endpoints=endpoints,
                capture=capture,
            )
        else:
            if not isinstance(payload, dict):
                raise TypeError("HTTP compute payload must be a mapping")
            operation = runtime.execute(
                service,
                revision,
                endpoint_id,
                context,
                payload,
                artifact_uri=artifact_uri,
                artifact_digest=artifact_digest,
                endpoints=endpoints,
                capture=capture,
            )
        value = loop.run_until_complete(operation)
        if include_metadata:
            node_id = str(ray.get_runtime_context().get_node_id())
            return ComputeTaskResult(value=value, node_id=node_id)
        return value
    finally:
        loop.run_until_complete(runtime.drain(service, revision))


ComputeTask = ray.remote(max_retries=0)(run_compute_task)
