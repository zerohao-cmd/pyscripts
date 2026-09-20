from __future__ import annotations

import pytest
from pydantic import SecretStr

from pyscripts.config import Settings
from pyscripts.runtime.worker_pools import (
    KubeRayWorkerPoolCatalog,
    WorkerPoolDiscoveryError,
)


def settings() -> Settings:
    return Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        kuberay_cluster_name="pyscripts-ray",
        ray_worker_pool_label_key="pyscripts.worker-pool",
    )


def test_kuberay_catalog_reads_declared_scale_to_zero_worker_types() -> None:
    catalog = KubeRayWorkerPoolCatalog(settings())
    pools = catalog._parse_worker_pools(
        {
            "spec": {
                "workerGroupSpecs": [
                    {
                        "groupName": "default-workers",
                        "replicas": 0,
                        "minReplicas": 0,
                        "maxReplicas": 10,
                        "labels": {"pyscripts.worker-pool": "default"},
                    },
                    {
                        "groupName": "gpu-workers",
                        "replicas": 2,
                        "labels": {"pyscripts.worker-pool": "cuda12"},
                    },
                ]
            }
        }
    )

    assert [(pool.name, pool.node_count, pool.source) for pool in pools] == [
        ("cuda12", 2, "KUBERAY"),
        ("default", 0, "KUBERAY"),
    ]


def test_kuberay_catalog_rejects_duplicate_worker_type_labels() -> None:
    catalog = KubeRayWorkerPoolCatalog(settings())
    with pytest.raises(WorkerPoolDiscoveryError, match="multiple worker groups"):
        catalog._parse_worker_pools(
            {
                "spec": {
                    "workerGroupSpecs": [
                        {"labels": {"pyscripts.worker-pool": "default"}},
                        {"labels": {"pyscripts.worker-pool": "default"}},
                    ]
                }
            }
        )
