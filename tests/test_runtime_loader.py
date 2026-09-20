from __future__ import annotations

import hashlib
import asyncio
import zipfile
from pathlib import Path

from pyscripts.runtime.loader import EndpointDefinition, VersionedRuntime
from pyscripts.runtime.output import ExecutionOutcome


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


def build_absolute_import_artifact(path: Path, offset: int) -> str:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("src/__init__.py", "")
        archive.writestr("src/constants.py", f"OFFSET = {offset}\n")
        archive.writestr(
            "main.py",
            "from src.constants import OFFSET\n"
            "def eager(value):\n"
            "    return value + OFFSET\n"
            "def lazy(value):\n"
            "    import src.constants\n"
            "    return value + src.constants.OFFSET\n",
        )
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def test_absolute_local_imports_are_revision_isolated(tmp_path: Path) -> None:
    first_artifact = tmp_path / "absolute-first.zip"
    second_artifact = tmp_path / "absolute-second.zip"
    first_digest = build_absolute_import_artifact(first_artifact, 1)
    second_digest = build_absolute_import_artifact(second_artifact, 10)
    runtime = VersionedRuntime(tmp_path / "absolute-cache")
    endpoints = [
        EndpointDefinition(id="eager", task_type="io", entrypoint="main:eager"),
        EndpointDefinition(id="lazy", task_type="io", entrypoint="main:lazy"),
    ]

    first_eager = await runtime.execute(
        "absolute",
        "rev-1",
        "eager",
        {},
        {"value": 5},
        artifact_uri=first_artifact.as_uri(),
        artifact_digest=first_digest,
        endpoints=endpoints,
    )
    second_eager = await runtime.execute(
        "absolute",
        "rev-2",
        "eager",
        {},
        {"value": 5},
        artifact_uri=second_artifact.as_uri(),
        artifact_digest=second_digest,
        endpoints=endpoints,
    )
    first_lazy, second_lazy = await asyncio.gather(
        runtime.execute(
            "absolute",
            "rev-1",
            "lazy",
            {},
            {"value": 5},
            artifact_uri=first_artifact.as_uri(),
            artifact_digest=first_digest,
            endpoints=endpoints,
        ),
        runtime.execute(
            "absolute",
            "rev-2",
            "lazy",
            {},
            {"value": 5},
            artifact_uri=second_artifact.as_uri(),
            artifact_digest=second_digest,
            endpoints=endpoints,
        ),
    )

    assert (first_eager, second_eager) == (6, 15)
    assert (first_lazy, second_lazy) == (6, 15)
    assert "src" not in __import__("sys").modules


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


async def test_concurrent_io_output_is_isolated_by_invocation(tmp_path: Path) -> None:
    artifact = tmp_path / "output.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "service.py",
            "import asyncio\n"
            "async def run(context, name):\n"
            "    print(f'{name}:start')\n"
            "    await asyncio.sleep(0.01)\n"
            "    print(f'{name}:error', file=__import__('sys').stderr)\n"
            "    print(f'{name}:end')\n"
            "    return name\n",
        )
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    runtime = VersionedRuntime(tmp_path / "output-cache")
    endpoints = [EndpointDefinition(id="run", task_type="io", entrypoint="service:run")]

    async def invoke(name: str) -> ExecutionOutcome:
        result = await runtime.execute(
            "output",
            "rev-1",
            "run",
            {"request_id": name},
            {"name": name},
            artifact_uri=artifact.as_uri(),
            artifact_digest=digest,
            endpoints=endpoints,
            capture=True,
        )
        assert isinstance(result, ExecutionOutcome)
        return result

    first, second = await asyncio.gather(invoke("first"), invoke("second"))

    first_text = "".join(chunk.content for chunk in first.logs)
    second_text = "".join(chunk.content for chunk in second.logs)
    assert first.succeeded and first.value == "first"
    assert second.succeeded and second.value == "second"
    assert "first:start" in first_text and "first:end" in first_text
    assert "second:" not in first_text
    assert "second:start" in second_text and "second:end" in second_text
    assert "first:" not in second_text
    assert any(chunk.stream == "STDERR" for chunk in first.logs)
    assert all(chunk.emitted_at.tzinfo is not None for chunk in first.logs)
    assert list(first.logs) == sorted(first.logs, key=lambda chunk: chunk.emitted_at)


async def test_sync_output_capture_is_bounded(tmp_path: Path) -> None:
    artifact = tmp_path / "bounded-output.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "service.py",
            "def run():\n"
            "    print('x' * 4096)\n"
            "    return 7\n",
        )
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    runtime = VersionedRuntime(
        tmp_path / "bounded-cache",
        invocation_log_max_bytes=1024,
        invocation_log_chunk_bytes=256,
    )
    result = await runtime.execute(
        "bounded",
        "rev-1",
        "run",
        {},
        {},
        artifact_uri=artifact.as_uri(),
        artifact_digest=digest,
        endpoints=[EndpointDefinition(id="run", task_type="io", entrypoint="service:run")],
        capture=True,
    )

    assert isinstance(result, ExecutionOutcome)
    assert result.value == 7
    assert result.log_bytes == 1024
    assert result.logs_truncated is True
    assert len(result.logs) == 4
