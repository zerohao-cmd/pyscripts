from __future__ import annotations

import asyncio
import json
import os
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import ray

from pyscripts.config import Settings
from pyscripts.ray_client import ensure_ray


class WorkerPoolDiscoveryError(RuntimeError):
    pass


class UnknownWorkerPoolError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class WorkerPool:
    """A container-level scheduling target advertised by Ray worker nodes."""

    name: str
    label_key: str
    node_count: int
    source: str


class WorkerPoolCatalog(Protocol):
    async def list(self) -> list[WorkerPool]: ...

    async def require(self, name: str) -> WorkerPool: ...


class StaticWorkerPoolCatalog:
    """Development catalog used when Ray label scheduling is disabled."""

    def __init__(self, label_key: str, names: list[str]):
        self._pools = {
            name: WorkerPool(
                name=name,
                label_key=label_key,
                node_count=0,
                source="CONFIG",
            )
            for name in names
        }

    async def list(self) -> list[WorkerPool]:
        return sorted(self._pools.values(), key=lambda item: item.name)

    async def require(self, name: str) -> WorkerPool:
        try:
            return self._pools[name]
        except KeyError as error:
            raise UnknownWorkerPoolError(
                f"worker pool {name!r} is not advertised by the Ray cluster"
            ) from error


class RayWorkerPoolCatalog:
    """Read worker-pool values from labels on live Ray nodes.

    KubeRay owns these labels. The control plane deliberately exposes no
    mutation method for them; it only discovers values already present in the
    cluster and validates references made by Python runtime profiles.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._lock = asyncio.Lock()

    async def list(self) -> list[WorkerPool]:
        await self._ensure_ray()
        try:
            nodes = await asyncio.to_thread(ray.nodes)
        except Exception as error:
            raise WorkerPoolDiscoveryError(
                f"failed to discover Ray worker pools: {error}"
            ) from error

        counts: dict[str, int] = {}
        for node in nodes:
            if not bool(node.get("Alive", node.get("alive", False))):
                continue
            labels = node.get("Labels", node.get("labels", {}))
            if not isinstance(labels, dict):
                continue
            name = labels.get(self.settings.ray_worker_pool_label_key)
            if isinstance(name, str) and name:
                counts[name] = counts.get(name, 0) + 1
        return [
            WorkerPool(
                name=name,
                label_key=self.settings.ray_worker_pool_label_key,
                node_count=count,
                source="RAY",
            )
            for name, count in sorted(counts.items())
        ]

    async def require(self, name: str) -> WorkerPool:
        # Serialize discovery during concurrent profile creation so a burst of
        # requests does not issue an equivalent burst of Ray state queries.
        async with self._lock:
            pools = await self.list()
        for pool in pools:
            if pool.name == name:
                return pool
        raise UnknownWorkerPoolError(
            f"worker pool {name!r} is not advertised by any live Ray node"
        )

    async def _ensure_ray(self) -> None:
        try:
            await ensure_ray(self.settings)
        except Exception as error:
            raise WorkerPoolDiscoveryError(
                f"failed to connect to Ray for worker-pool discovery: {error}"
            ) from error


class KubeRayWorkerPoolCatalog:
    """Discover declared worker types from a KubeRay RayCluster CR.

    KubeRay 1.5+ mirrors top-level worker-group ``labels`` into both Ray node
    labels and Kubernetes Pod labels. Reading the CR keeps scale-to-zero worker
    types selectable even when no corresponding Ray node is currently alive.
    """

    def __init__(self, settings: Settings):
        if not settings.kuberay_cluster_name:
            raise ValueError("kuberay_cluster_name is required")
        self.settings = settings
        self.service_account_root = settings.kubernetes_service_account_root

    async def list(self) -> list[WorkerPool]:
        try:
            cluster = await asyncio.to_thread(self._read_cluster)
        except Exception as error:
            if isinstance(error, WorkerPoolDiscoveryError):
                raise
            raise WorkerPoolDiscoveryError(
                f"failed to discover KubeRay worker pools: {error}"
            ) from error
        return self._parse_worker_pools(cluster)

    async def require(self, name: str) -> WorkerPool:
        for pool in await self.list():
            if pool.name == name:
                return pool
        raise UnknownWorkerPoolError(
            f"worker pool {name!r} is not declared by the configured RayCluster"
        )

    def _read_cluster(self) -> dict:
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise WorkerPoolDiscoveryError(
                "KUBERNETES_SERVICE_HOST is unavailable; run in-cluster or "
                "leave PYSCRIPTS_KUBERAY_CLUSTER_NAME unset"
            )
        namespace = self.settings.kubernetes_namespace or self._read_text(
            self.service_account_root / "namespace"
        )
        token = self._read_text(self.service_account_root / "token")
        cluster_name = urllib.parse.quote(
            self.settings.kuberay_cluster_name or "", safe=""
        )
        namespace_path = urllib.parse.quote(namespace, safe="")
        url = (
            f"https://{host}:{port}/apis/ray.io/v1/namespaces/"
            f"{namespace_path}/rayclusters/{cluster_name}"
        )
        request = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
        )
        ca_path = self.service_account_root / "ca.crt"
        context = ssl.create_default_context(cafile=str(ca_path))
        try:
            with urllib.request.urlopen(
                request,
                timeout=10,
                context=context,
            ) as response:
                value = json.load(response)
        except urllib.error.HTTPError as error:
            raise WorkerPoolDiscoveryError(
                f"Kubernetes API returned HTTP {error.code} for RayCluster "
                f"{namespace}/{self.settings.kuberay_cluster_name}"
            ) from error
        if not isinstance(value, dict):
            raise WorkerPoolDiscoveryError("Kubernetes API returned an invalid RayCluster")
        return value

    def _parse_worker_pools(self, cluster: dict) -> list[WorkerPool]:
        groups = cluster.get("spec", {}).get("workerGroupSpecs", [])
        if not isinstance(groups, list):
            raise WorkerPoolDiscoveryError(
                "RayCluster spec.workerGroupSpecs must be a list"
            )
        pools: dict[str, WorkerPool] = {}
        for group in groups:
            if not isinstance(group, dict):
                continue
            labels = group.get("labels", {})
            if not isinstance(labels, dict):
                continue
            name = labels.get(self.settings.ray_worker_pool_label_key)
            if not isinstance(name, str) or not name:
                continue
            if name in pools:
                raise WorkerPoolDiscoveryError(
                    f"worker pool label {name!r} is declared by multiple worker groups"
                )
            replicas = group.get("replicas", group.get("minReplicas", 0))
            pools[name] = WorkerPool(
                name=name,
                label_key=self.settings.ray_worker_pool_label_key,
                node_count=max(0, int(replicas or 0)),
                source="KUBERAY",
            )
        return sorted(pools.values(), key=lambda item: item.name)

    @staticmethod
    def _read_text(path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8").strip()
        except OSError as error:
            raise WorkerPoolDiscoveryError(
                f"cannot read Kubernetes service-account file {path}: {error}"
            ) from error


def create_worker_pool_catalog(settings: Settings) -> WorkerPoolCatalog:
    if not settings.ray_use_label_selector:
        return StaticWorkerPoolCatalog(
            settings.ray_worker_pool_label_key,
            [settings.default_worker_pool],
        )
    if settings.kuberay_cluster_name and os.environ.get("KUBERNETES_SERVICE_HOST"):
        return KubeRayWorkerPoolCatalog(settings)
    return RayWorkerPoolCatalog(settings)
