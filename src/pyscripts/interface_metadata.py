from __future__ import annotations

import hashlib
import re
import shutil
import tempfile
import tomllib
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from pyscripts.schemas import RevisionInterfaceSpec


class InterfaceMetadataError(RuntimeError):
    pass


PLATFORM_TYPES = {
    "Float",
    "Double",
    "Int32",
    "Int64",
    "Uint32",
    "Uint64",
    "Sint32",
    "Sint64",
    "Fixed32",
    "Fixed64",
    "Sfixed32",
    "Sfixed64",
    "Bool",
    "String",
    "Bytes",
    "List",
    "Struct",
    "Date",
    "Datetime",
    "DatetimeTz",
    "Int",
    "Bigint",
}
SCALAR_TYPES = PLATFORM_TYPES - {"List", "Struct"}
PYPROJECT_MAX_BYTES = 1024 * 1024
ENDPOINT_METADATA_FIELDS = {
    "id",
    "task_type",
    "entrypoint",
    "io_type",
    "para",
    "return",
}
PYTHON_PARAMETER_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def parse_artifact_interface(
    artifact_uri: str,
    expected_digest: str,
) -> RevisionInterfaceSpec:
    """Read and validate the interface-only metadata from an immutable ZIP."""

    with tempfile.TemporaryDirectory(prefix="pyscripts-interface-") as temp_name:
        archive_path = Path(temp_name) / "artifact.zip"
        _download_artifact(artifact_uri, archive_path)
        _verify_digest(archive_path, expected_digest)
        document = _read_pyproject(archive_path)
    return parse_interface_document(document)


def parse_interface_document(document: Mapping[str, Any]) -> RevisionInterfaceSpec:
    tool = document.get("tool")
    if not isinstance(tool, Mapping):
        raise InterfaceMetadataError("pyproject.toml is missing [tool.pyscript]")
    tool_metadata = tool.get("pyscript")
    if not isinstance(tool_metadata, Mapping):
        raise InterfaceMetadataError("pyproject.toml is missing [tool.pyscript]")

    unknown = set(tool_metadata) - {
        "spec_version",
        "runtime",
        "endpoints",
    }
    if unknown:
        fields = ", ".join(sorted(str(field) for field in unknown))
        raise InterfaceMetadataError(
            f"unsupported [tool.pyscript] field(s): {fields}"
        )
    if tool_metadata.get("spec_version") != 1:
        raise InterfaceMetadataError("tool.pyscript.spec_version must be 1")

    runtime = tool_metadata.get("runtime")
    if not isinstance(runtime, str):
        raise InterfaceMetadataError("tool.pyscript.runtime must be a string")

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

    endpoints = tool_metadata.get("endpoints")
    if not isinstance(endpoints, list) or not endpoints:
        raise InterfaceMetadataError(
            "tool.pyscript.endpoints must contain at least one endpoint"
        )
    normalized_endpoints: list[dict[str, Any]] = []
    for index, endpoint in enumerate(endpoints):
        if not isinstance(endpoint, Mapping):
            raise InterfaceMetadataError(
                f"tool.pyscript.endpoints[{index}] must be a table"
            )
        unknown_fields = set(endpoint) - ENDPOINT_METADATA_FIELDS
        if unknown_fields:
            raise InterfaceMetadataError(
                f"endpoints[{index}] contains unsupported field(s): "
                + ", ".join(sorted(str(field) for field in unknown_fields))
            )
        if "task_type" not in endpoint:
            raise InterfaceMetadataError(f"endpoints[{index}].task_type is required")
        entrypoint = endpoint.get("entrypoint")
        if not isinstance(entrypoint, str):
            raise InterfaceMetadataError(f"endpoints[{index}].entrypoint is required")
        endpoint_id = endpoint.get("id")
        if endpoint_id is None:
            _, separator, function_name = entrypoint.partition(":")
            if not separator or not function_name:
                raise InterfaceMetadataError(
                    f"endpoints[{index}].entrypoint must use "
                    "'package.module:function'"
                )
            endpoint_id = function_name

        if "para" not in endpoint:
            raise InterfaceMetadataError(f"endpoints[{index}].para is required")
        parameters = _normalize_parameters(
            endpoint["para"],
            f"endpoints[{index}].para",
        )
        if "return" not in endpoint:
            raise InterfaceMetadataError(f"endpoints[{index}].return is required")
        response_schema = _compact_schema(
            endpoint["return"],
            f"endpoints[{index}].return",
        )
        normalized_endpoints.append(
            {
                "id": endpoint_id,
                "task_type": endpoint["task_type"],
                "entrypoint": entrypoint,
                "io_type": endpoint.get("io_type", ["rest"]),
                "parameters": parameters,
                "response_schema": response_schema,
            }
        )

    try:
        return RevisionInterfaceSpec.model_validate(
            {
                "endpoints": normalized_endpoints,
                "runtime_profile": runtime,
                "requires_python": requires_python,
                "dependencies": dependencies,
            }
        )
    except ValidationError as error:
        raise InterfaceMetadataError(
            f"invalid tool.pyscript interface metadata: {error}"
        ) from error


def _normalize_parameters(value: Any, path: str) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise InterfaceMetadataError(f"{path} must be a non-empty table")
    if not value:
        raise InterfaceMetadataError(f"{path} must not be empty")
    parameters: dict[str, dict[str, Any]] = {}
    for name, schema in value.items():
        _validate_field_name(name, f"{path}.{name}")
        if name == "context":
            raise InterfaceMetadataError(f"{path}.context is reserved by the platform")
        parameters[name] = _compact_schema(schema, f"{path}.{name}")
    return parameters


def _compact_schema(
    value: Any,
    path: str,
) -> dict[str, Any]:
    if isinstance(value, str):
        if value not in SCALAR_TYPES:
            raise InterfaceMetadataError(
                f"{path} must be a supported scalar pyscripts data type"
            )
        return {"type": value}
    if not isinstance(value, Mapping):
        raise InterfaceMetadataError(f"{path} must be a type string or table")
    if not value:
        raise InterfaceMetadataError(f"{path} must not be an empty table")
    if "_item" in value:
        if set(value) != {"_item"}:
            raise InterfaceMetadataError(
                f"{path} uses reserved _item and cannot contain other fields"
            )
        return {
            "type": "List",
            "items": _compact_schema(value["_item"], f"{path}._item"),
        }
    properties: dict[str, dict[str, Any]] = {}
    for name, child in value.items():
        _validate_field_name(name, f"{path}.{name}")
        properties[name] = _compact_schema(child, f"{path}.{name}")
    return {
        "type": "Struct",
        "required": list(properties),
        "additionalProperties": False,
        "properties": properties,
    }


def _validate_field_name(value: Any, path: str) -> None:
    if not isinstance(value, str) or not PYTHON_PARAMETER_NAME.fullmatch(value):
        raise InterfaceMetadataError(
            f"{path} is not a valid Python/protobuf field name"
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
        raise InterfaceMetadataError(
            f"failed to download artifact: {error}"
        ) from error
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
