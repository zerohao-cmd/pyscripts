from __future__ import annotations

import asyncio
import hashlib
import importlib
import shutil
import sys
import tempfile
import tomllib
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import ray
from pydantic import ValidationError
from sc_hook import RemoteFunc, to_schema

from pyscripts.schemas import EndpointSpec, RevisionInterfaceSpec

if TYPE_CHECKING:
    from pyscripts.config import Settings


class InterfaceMetadataError(RuntimeError):
    pass


PYPROJECT_MAX_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ArtifactProjectSpec:
    runtime_profile: str
    modules: tuple[str, ...]
    requires_python: str
    dependencies: tuple[str, ...]


class ArtifactInterfaceInspector(Protocol):
    async def inspect(
        self,
        artifact_uri: str,
        expected_digest: str,
        project: ArtifactProjectSpec,
        *,
        runtime_env: Mapping[str, Any],
        worker_pool: str,
        pip_source: str,
        pip_index_url: str | None,
    ) -> RevisionInterfaceSpec: ...


def parse_artifact_project(
    artifact_uri: str,
    expected_digest: str,
) -> ArtifactProjectSpec:
    """Read service-level publication settings without importing user code."""

    with tempfile.TemporaryDirectory(prefix="pyscripts-project-") as temp_name:
        archive_path = Path(temp_name) / "artifact.zip"
        _download_artifact(artifact_uri, archive_path)
        _verify_digest(archive_path, expected_digest)
        document = _read_pyproject(archive_path)
    return parse_interface_document(document)


def parse_artifact_interface(
    artifact_uri: str,
    expected_digest: str,
) -> RevisionInterfaceSpec:
    """Discover Hook endpoints locally.

    Production publication uses :class:`RayArtifactInterfaceInspector` so the
    import occurs inside the selected runtime profile. This local form is used
    for development, tests and installations which allow unregistered runtime
    profiles.
    """

    with tempfile.TemporaryDirectory(prefix="pyscripts-interface-") as temp_name:
        temporary = Path(temp_name)
        archive_path = temporary / "artifact.zip"
        source_root = temporary / "source"
        _download_artifact(artifact_uri, archive_path)
        _verify_digest(archive_path, expected_digest)
        project = parse_interface_document(_read_pyproject(archive_path))
        _extract_archive(archive_path, source_root)
        endpoints = _discover_hook_endpoints(source_root, project.modules)
    return _build_interface(project, endpoints)


def parse_interface_document(document: Mapping[str, Any]) -> ArtifactProjectSpec:
    """Parse the v2 project configuration; endpoint metadata lives in code."""

    tool = document.get("tool")
    if not isinstance(tool, Mapping):
        raise InterfaceMetadataError("pyproject.toml is missing [tool.pyscript]")
    metadata = tool.get("pyscript")
    if not isinstance(metadata, Mapping):
        raise InterfaceMetadataError("pyproject.toml is missing [tool.pyscript]")

    unknown = set(metadata) - {"spec_version", "runtime", "module", "modules"}
    if unknown:
        fields = ", ".join(sorted(str(field) for field in unknown))
        raise InterfaceMetadataError(
            f"unsupported [tool.pyscript] field(s): {fields}"
        )
    if metadata.get("spec_version") != 2:
        raise InterfaceMetadataError("tool.pyscript.spec_version must be 2")

    runtime = metadata.get("runtime")
    if not isinstance(runtime, str) or not runtime.strip():
        raise InterfaceMetadataError("tool.pyscript.runtime must be a string")

    module = metadata.get("module")
    modules = metadata.get("modules")
    if module is not None and modules is not None:
        raise InterfaceMetadataError(
            "tool.pyscript cannot define both module and modules"
        )
    if module is not None:
        modules = [module]
    if (
        not isinstance(modules, list)
        or not modules
        or not all(
            isinstance(item, str) and _valid_module_name(item) for item in modules
        )
    ):
        raise InterfaceMetadataError(
            "tool.pyscript.module or tool.pyscript.modules must name importable modules"
        )
    if len(modules) != len(set(modules)):
        raise InterfaceMetadataError("tool.pyscript.modules values must be unique")

    project = document.get("project")
    if not isinstance(project, Mapping):
        raise InterfaceMetadataError("pyproject.toml is missing [project]")
    requires_python = project.get("requires-python")
    if not isinstance(requires_python, str):
        raise InterfaceMetadataError("project.requires-python must be a string")
    dependencies = project.get("dependencies", [])
    if not isinstance(dependencies, list) or not all(
        isinstance(item, str) for item in dependencies
    ):
        raise InterfaceMetadataError("project.dependencies must be an array of strings")

    return ArtifactProjectSpec(
        runtime_profile=runtime,
        modules=tuple(modules),
        requires_python=requires_python,
        dependencies=tuple(dependencies),
    )


def _valid_module_name(value: str) -> bool:
    return bool(value) and all(part.isidentifier() for part in value.split("."))


def _discover_hook_endpoints(
    source_root: Path,
    module_names: tuple[str, ...],
) -> list[EndpointSpec]:
    endpoints: list[EndpointSpec] = []
    source_text = str(source_root.resolve())
    sys.path.insert(0, source_text)
    try:
        importlib.invalidate_caches()
        for module_name in module_names:
            try:
                module = importlib.import_module(module_name)
            except Exception as error:
                raise InterfaceMetadataError(
                    f"failed to import Hook module {module_name!r}: "
                    f"{type(error).__name__}: {error}"
                ) from error
            for attribute, value in sorted(vars(module).items()):
                if not isinstance(value, RemoteFunc):
                    continue
                # Imported helpers are not automatically published. A module
                # containing endpoints must be explicitly listed.
                if value.func.__module__ != module.__name__:
                    continue
                transports = [item.value for item in value.io_types]
                endpoints.append(
                    EndpointSpec(
                        id=value.endpoint_id,
                        task_type=value.task_type.value,
                        entrypoint=f"{module_name}:{attribute}",
                        io_type=transports,
                        parameters={
                            name: to_schema(data_type)
                            for name, data_type in value.params_type.items()
                        },
                        response_schema=to_schema(value.return_type),
                        num_cpus=value.num_cpus,
                        num_gpus=value.num_gpus,
                    )
                )
    finally:
        if sys.path and sys.path[0] == source_text:
            sys.path.pop(0)
        # Ray discovery tasks use max_calls=1, but local development runs in a
        # long-lived process. Remove only modules loaded from this extraction.
        root = source_root.resolve()
        for name, module in tuple(sys.modules.items()):
            module_file = getattr(module, "__file__", None)
            if module_file is None:
                continue
            try:
                belongs_to_artifact = Path(module_file).resolve().is_relative_to(root)
            except (OSError, RuntimeError):
                belongs_to_artifact = False
            if belongs_to_artifact:
                sys.modules.pop(name, None)

    if not endpoints:
        raise InterfaceMetadataError("configured Hook modules contain no endpoints")
    ids = [endpoint.id for endpoint in endpoints]
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    if duplicates:
        raise InterfaceMetadataError(
            "Hook endpoint ids must be unique: " + ", ".join(duplicates)
        )
    return endpoints


def _build_interface(
    project: ArtifactProjectSpec,
    endpoints: list[EndpointSpec],
) -> RevisionInterfaceSpec:
    try:
        return RevisionInterfaceSpec(
            endpoints=endpoints,
            runtime_profile=project.runtime_profile,
            requires_python=project.requires_python,
            dependencies=list(project.dependencies),
        )
    except ValidationError as error:
        raise InterfaceMetadataError(
            f"invalid Hook interface metadata: {error}"
        ) from error


@ray.remote(max_calls=1, max_retries=0)
def _inspect_artifact_task(
    artifact_uri: str,
    expected_digest: str,
) -> dict[str, Any]:
    return parse_artifact_interface(artifact_uri, expected_digest).model_dump(
        mode="json"
    )


class RayArtifactInterfaceInspector:
    """Import service Hook modules in the selected immutable Runtime Env."""

    def __init__(self, settings: Settings):
        self.settings = settings

    async def inspect(
        self,
        artifact_uri: str,
        expected_digest: str,
        project: ArtifactProjectSpec,
        *,
        runtime_env: Mapping[str, Any],
        worker_pool: str,
        pip_source: str,
        pip_index_url: str | None,
    ) -> RevisionInterfaceSpec:
        from pyscripts.ray_client import ensure_ray
        from pyscripts.runtime.profiles import materialize_runtime_env

        await ensure_ray(self.settings)
        options: dict[str, Any] = {
            "num_cpus": 0.01,
            "runtime_env": materialize_runtime_env(
                dict(runtime_env), pip_source, self.settings, pip_index_url
            ),
        }
        if self.settings.ray_use_label_selector:
            options["label_selector"] = {
                self.settings.ray_worker_pool_label_key: worker_pool
            }
        reference = None
        try:
            reference = _inspect_artifact_task.options(**options).remote(
                artifact_uri,
                expected_digest,
            )
            payload = await asyncio.wait_for(
                reference,
                timeout=self.settings.runtime_env_setup_timeout_seconds + 120,
            )
        except TimeoutError as error:
            if reference is not None:
                ray.cancel(reference, force=True)
            raise InterfaceMetadataError(
                "Hook interface discovery timed out in the selected Runtime Env"
            ) from error
        except Exception as error:
            raise InterfaceMetadataError(
                f"Hook interface discovery failed: {type(error).__name__}: {error}"
            ) from error
        interface = RevisionInterfaceSpec.model_validate(payload)
        if (
            interface.runtime_profile != project.runtime_profile
            or interface.requires_python != project.requires_python
            or tuple(interface.dependencies) != project.dependencies
        ):
            raise InterfaceMetadataError(
                "artifact project metadata changed during Hook interface discovery"
            )
        return interface


class LocalArtifactInterfaceInspector:
    """Development/test inspector that imports Hook modules in this process."""

    async def inspect(
        self,
        artifact_uri: str,
        expected_digest: str,
        project: ArtifactProjectSpec,
        *,
        runtime_env: Mapping[str, Any],
        worker_pool: str,
        pip_source: str,
        pip_index_url: str | None,
    ) -> RevisionInterfaceSpec:
        del project, runtime_env, worker_pool, pip_source, pip_index_url
        return await asyncio.to_thread(
            parse_artifact_interface,
            artifact_uri,
            expected_digest,
        )


def _read_pyproject(archive_path: Path) -> dict[str, Any]:
    try:
        with zipfile.ZipFile(archive_path) as archive:
            entries = [
                info
                for info in archive.infolist()
                if info.filename == "pyproject.toml" and not info.is_dir()
            ]
            if len(entries) != 1:
                raise InterfaceMetadataError(
                    "artifact must contain one pyproject.toml at its root"
                )
            entry = entries[0]
            if entry.file_size > PYPROJECT_MAX_BYTES:
                raise InterfaceMetadataError("pyproject.toml is larger than 1 MiB")
            source = archive.read(entry)
    except InterfaceMetadataError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise InterfaceMetadataError(
            f"artifact is not a readable ZIP: {error}"
        ) from error

    try:
        return tomllib.loads(source.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise InterfaceMetadataError(f"invalid pyproject.toml: {error}") from error


def _extract_archive(archive_path: Path, destination: Path) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(archive_path) as archive:
            for member in archive.infolist():
                target = (destination / member.filename).resolve()
                if (
                    target != destination.resolve()
                    and destination.resolve() not in target.parents
                ):
                    raise InterfaceMetadataError(
                        f"unsafe artifact path: {member.filename}"
                    )
                file_mode = member.external_attr >> 16
                if file_mode & 0o170000 == 0o120000:
                    raise InterfaceMetadataError(
                        f"symbolic links are not allowed: {member.filename}"
                    )
            archive.extractall(destination)
    except InterfaceMetadataError:
        raise
    except (OSError, zipfile.BadZipFile, RuntimeError) as error:
        raise InterfaceMetadataError(
            f"artifact is not a readable ZIP: {error}"
        ) from error


def _download_artifact(artifact_uri: str, destination: Path) -> None:
    parsed = urllib.parse.urlparse(artifact_uri)
    try:
        if parsed.scheme in {"", "file"}:
            source = Path(urllib.request.url2pathname(parsed.path or artifact_uri))
            if not source.is_file():
                raise InterfaceMetadataError(f"artifact does not exist: {source}")
            shutil.copyfile(source, destination)
            return
        if parsed.scheme in {"http", "https"}:
            with (
                urllib.request.urlopen(artifact_uri, timeout=120) as response,
                destination.open("wb") as output,
            ):
                shutil.copyfileobj(response, output)
            return
    except InterfaceMetadataError:
        raise
    except OSError as error:
        raise InterfaceMetadataError(f"failed to download artifact: {error}") from error
    raise InterfaceMetadataError("artifact URI must use file, http, or https")


def _verify_digest(path: Path, expected_digest: str) -> None:
    expected = expected_digest.removeprefix("sha256:").lower()
    if len(expected) != 64 or any(
        character not in "0123456789abcdef" for character in expected
    ):
        raise InterfaceMetadataError("artifact digest must be a SHA-256 hex digest")
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected:
        raise InterfaceMetadataError(
            f"artifact digest mismatch: expected {expected}, got {actual}"
        )
