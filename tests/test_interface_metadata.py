from __future__ import annotations

import hashlib
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from pyscripts.interface_metadata import (
    ArtifactProjectSpec,
    InterfaceMetadataError,
    RayArtifactInterfaceInspector,
    parse_artifact_interface,
    parse_interface_document,
)


def project_document(**pyscript) -> dict:
    return {
        "project": {
            "requires-python": ">=3.12,<3.13",
            "dependencies": [],
        },
        "tool": {
            "pyscript": {
                "spec_version": 2,
                "runtime": "test",
                **pyscript,
            }
        },
    }


def build_artifact(tmp_path: Path, source: str, pyproject: str | None = None):
    archive_path = tmp_path / "artifact.zip"
    config = pyproject or (
        "[project]\n"
        'name = "hook-service"\n'
        'version = "1.0.0"\n'
        'requires-python = ">=3.12,<3.13"\n'
        "dependencies = []\n"
        "\n[tool.pyscript]\n"
        "spec_version = 2\n"
        'runtime = "test"\n'
        'module = "service"\n'
    )
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("pyproject.toml", config)
        archive.writestr("service.py", source)
    return archive_path, hashlib.sha256(archive_path.read_bytes()).hexdigest()


def test_parses_v2_project_configuration() -> None:
    project = parse_interface_document(project_document(modules=["api", "jobs.tasks"]))

    assert project.runtime_profile == "test"
    assert project.modules == ("api", "jobs.tasks")
    assert project.requires_python == ">=3.12,<3.13"


def test_discovers_hook_endpoints_and_infers_task_types(tmp_path: Path) -> None:
    artifact, digest = build_artifact(
        tmp_path,
        '''from typing import TypedDict
from sc_hook import ArrowTable, Date, Datetime, Int64, IOType, String, hook

class Item(TypedDict):
    sku: String
    quantity: Int64

@hook(io_types=[IOType.Inner], num_cpus=2)
def total(items: list[Item]) -> Int64:
    return sum(item["quantity"] for item in items)

@hook(io_types=[IOType.Rest, IOType.Grpc])
async def submit(customer: String, items: list[Item]) -> Int64:
    return await total.remote_call(items)

@hook(io_types=[IOType.Inner])
def normalize(table: ArrowTable, day: Date, created_at: Datetime) -> ArrowTable:
    return table
''',
    )

    interface = parse_artifact_interface(artifact.as_uri(), digest)
    endpoints = {endpoint.id: endpoint for endpoint in interface.endpoints}

    assert endpoints["total"].task_type == "compute"
    assert endpoints["total"].io_type == ["inner"]
    assert endpoints["total"].num_cpus == 2
    assert endpoints["submit"].task_type == "io"
    assert endpoints["submit"].io_type == ["rest", "grpc"]
    assert endpoints["submit"].entrypoint == "service:submit"
    assert endpoints["submit"].parameters["items"]["type"] == "List"
    item_schema = endpoints["submit"].parameters["items"]["items"]
    assert item_schema["type"] == "Struct"
    assert item_schema["properties"]["sku"] == {"type": "String"}
    assert endpoints["submit"].response_schema == {"type": "Int64"}
    assert endpoints["normalize"].parameters["table"] == {"type": "ArrowTable"}
    assert endpoints["normalize"].parameters["day"] == {"type": "Date"}
    assert endpoints["normalize"].parameters["created_at"] == {
        "type": "Datetime"
    }
    assert endpoints["normalize"].response_schema == {"type": "ArrowTable"}


def test_rejects_v1_endpoint_tables() -> None:
    with pytest.raises(InterfaceMetadataError, match="unsupported.*endpoints"):
        parse_interface_document(
            {
                **project_document(module="service"),
                "tool": {
                    "pyscript": {
                        "spec_version": 2,
                        "runtime": "test",
                        "module": "service",
                        "endpoints": [],
                    }
                },
            }
        )


def test_requires_one_explicit_module() -> None:
    with pytest.raises(InterfaceMetadataError, match="module"):
        parse_interface_document(project_document())

    with pytest.raises(InterfaceMetadataError, match="both module and modules"):
        parse_interface_document(project_document(module="service", modules=["other"]))


def test_rejects_duplicate_endpoint_ids(tmp_path: Path) -> None:
    artifact, digest = build_artifact(
        tmp_path,
        '''from sc_hook import Int32, IOType, hook
@hook(io_types=[IOType.Rest], endpoint_id="same")
def first(value: Int32) -> Int32: return value
@hook(io_types=[IOType.Rest], endpoint_id="same")
def second(value: Int32) -> Int32: return value
''',
    )

    with pytest.raises(InterfaceMetadataError, match="unique"):
        parse_artifact_interface(artifact.as_uri(), digest)


def test_rejects_module_without_hook_endpoints(tmp_path: Path) -> None:
    artifact, digest = build_artifact(
        tmp_path,
        "def ordinary(value): return value\n",
    )

    with pytest.raises(InterfaceMetadataError, match="no endpoints"):
        parse_artifact_interface(artifact.as_uri(), digest)


def test_reports_hook_import_validation_errors(tmp_path: Path) -> None:
    artifact, digest = build_artifact(
        tmp_path,
        '''from sc_hook import Int32, IOType, hook
@hook(io_types=[IOType.Rest])
def invalid(value) -> Int32: return value
''',
    )

    with pytest.raises(InterfaceMetadataError, match="must be annotated"):
        parse_artifact_interface(artifact.as_uri(), digest)


def test_discovery_module_import_does_not_require_control_plane_settings() -> None:
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import builtins

original_import = builtins.__import__

def block_settings(name, *args, **kwargs):
    if name == "pydantic_settings" or name.startswith("pydantic_settings."):
        raise ModuleNotFoundError("No module named 'pydantic_settings'")
    return original_import(name, *args, **kwargs)

builtins.__import__ = block_settings
import pyscripts.interface_metadata
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr


@pytest.mark.asyncio
async def test_discovery_submission_errors_are_reported(monkeypatch) -> None:
    async def fake_ensure_ray(_settings):
        return None

    class FailingTask:
        def options(self, **_options):
            return self

        def remote(self, *_args):
            raise ModuleNotFoundError("No module named 'missing_worker_package'")

    monkeypatch.setattr("pyscripts.ray_client.ensure_ray", fake_ensure_ray)
    monkeypatch.setattr(
        "pyscripts.interface_metadata._inspect_artifact_task", FailingTask()
    )
    inspector = RayArtifactInterfaceInspector(
        SimpleNamespace(
            ray_use_label_selector=False,
            runtime_env_setup_timeout_seconds=30,
        )
    )
    project = ArtifactProjectSpec("test", ("service",), ">=3.12", ())

    with pytest.raises(
        InterfaceMetadataError,
        match="Hook interface discovery failed: ModuleNotFoundError: "
        "No module named 'missing_worker_package'",
    ):
        await inspector.inspect(
            "https://example.test/artifact.zip",
            "0" * 64,
            project,
            runtime_env={},
            worker_pool="default",
            pip_source="default",
            pip_index_url=None,
        )
