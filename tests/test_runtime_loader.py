from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

from pyscripts.runtime.loader import EndpointDefinition, VersionedRuntime


def build_artifact(path: Path, offset: int) -> str:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("helper.py", f"OFFSET = {offset}\n")
        archive.writestr(
            "service.py",
            "from .helper import OFFSET\n"
            "def add(context, x, y):\n"
            "    return {'value': x + y + OFFSET, 'request_id': context['request_id']}\n",
        )
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def test_two_revisions_are_isolated_and_drained(tmp_path: Path) -> None:
    first_artifact = tmp_path / "first.zip"
    second_artifact = tmp_path / "second.zip"
    first_digest = build_artifact(first_artifact, 1)
    second_digest = build_artifact(second_artifact, 10)
    runtime = VersionedRuntime(tmp_path / "cache")
    endpoints = [
        EndpointDefinition(id="add", task_type="compute", entrypoint="service:add")
    ]

    first = await runtime.execute(
        "math",
        "rev-1",
        "add",
        {"request_id": "one"},
        {"x": 2, "y": 3},
        artifact_uri=first_artifact.as_uri(),
        artifact_digest=first_digest,
        endpoints=endpoints,
    )
    second = await runtime.execute(
        "math",
        "rev-2",
        "add",
        {"request_id": "two"},
        {"x": 2, "y": 3},
        artifact_uri=second_artifact.as_uri(),
        artifact_digest=second_digest,
        endpoints=endpoints,
    )

    assert first == {"value": 6, "request_id": "one"}
    assert second == {"value": 15, "request_id": "two"}
    assert await runtime.drain("math", "rev-1") is True
    status = await runtime.status()
    assert [(item["revision"], item["state"]) for item in status["versions"]] == [
        ("rev-2", "READY")
    ]


async def test_flat_http_parameters_do_not_require_context(tmp_path: Path) -> None:
    artifact = tmp_path / "flat.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "service.py",
            "def merge(x, y):\n"
            "    return {'x': x, 'y': y}\n",
        )
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    runtime = VersionedRuntime(tmp_path / "flat-cache")

    result = await runtime.execute(
        "flat",
        "rev-1",
        "merge",
        {"request_id": "not-injected"},
        {"x": {"customer_id": "a"}, "y": {"customer_id": "b"}},
        artifact_uri=artifact.as_uri(),
        artifact_digest=digest,
        endpoints=[
            EndpointDefinition(
                id="merge",
                task_type="io",
                entrypoint="service:merge",
            )
        ],
    )

    assert result == {
        "x": {"customer_id": "a"},
        "y": {"customer_id": "b"},
    }
