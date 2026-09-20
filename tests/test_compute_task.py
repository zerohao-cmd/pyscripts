from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

from pyscripts.runtime.compute import run_compute_task
from pyscripts.runtime.loader import ArtifactArchiveCache


def build_artifact(path: Path) -> str:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "service.py",
            "COUNT = 0\n"
            "def multiply(context, left, right):\n"
            "    global COUNT\n"
            "    COUNT += 1\n"
            "    return {\n"
            "        'value': left * right,\n"
            "        'request_id': context['request_id'],\n"
            "        'module_count': COUNT,\n"
            "    }\n",
        )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_compute_task_executes_immutable_artifact_snapshot(tmp_path: Path) -> None:
    artifact = tmp_path / "compute.zip"
    digest = build_artifact(artifact)
    result = run_compute_task(
        transport="http",
        cache_root=str(tmp_path / "cache"),
        environment_digest="environment-v1",
        service="math-service",
        revision="rev-compute-1",
        endpoint_id="multiply",
        context={"request_id": "request-1"},
        payload={"left": 6, "right": 7},
        artifact_uri=artifact.as_uri(),
        artifact_digest=digest,
        endpoint_manifest=[
            {
                "id": "multiply",
                "task_type": "compute",
                "entrypoint": "service:multiply",
            }
        ],
    )
    artifact.unlink()

    repeated = run_compute_task(
        transport="http",
        cache_root=str(tmp_path / "cache"),
        environment_digest="environment-v1",
        service="math-service",
        revision="rev-compute-1",
        endpoint_id="multiply",
        context={"request_id": "request-2"},
        payload={"left": 3, "right": 5},
        artifact_uri=artifact.as_uri(),
        artifact_digest=digest,
        endpoint_manifest=[
            {
                "id": "multiply",
                "task_type": "compute",
                "entrypoint": "service:multiply",
            }
        ],
    )

    assert result == {
        "value": 42,
        "request_id": "request-1",
        "module_count": 1,
    }
    assert repeated == {
        "value": 15,
        "request_id": "request-2",
        "module_count": 1,
    }


def test_artifact_archive_cache_survives_source_removal(tmp_path: Path) -> None:
    artifact = tmp_path / "compute.zip"
    digest = build_artifact(artifact)
    cache = ArtifactArchiveCache(tmp_path / "cache")
    cached = cache.obtain(artifact.as_uri(), digest)
    artifact.unlink()

    assert cache.obtain(artifact.as_uri(), digest) == cached
    assert hashlib.sha256(cached.read_bytes()).hexdigest() == digest
