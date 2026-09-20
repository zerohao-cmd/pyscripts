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

SCHEMA_METADATA_FIELDS = {"type", "description", "nullable"}
STRUCT_FIELDS = SCHEMA_METADATA_FIELDS | {
    "properties",
    "required",
    "additionalProperties",
}
LIST_FIELDS = SCHEMA_METADATA_FIELDS | {"items"}
PYPROJECT_MAX_BYTES = 1024 * 1024
ENDPOINT_METADATA_FIELDS = {
    "id",
    "task_type",
    "entrypoint",
    "io_type",
    "response_schema",
    "grpc",
    "num_cpus",
    "num_gpus",
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
        "grpc_contract",
    }
    if unknown:
        fields = ", ".join(sorted(str(field) for field in unknown))
        raise InterfaceMetadataError(
            f"unsupported [tool.pyscript] field(s): {fields}"
        )
    if tool_metadata.get("spec_version") != 1:
        raise InterfaceMetadataError("tool.pyscript.spec_version must be 1")

    runtime = tool_metadata.get("runtime")
    if not isinstance(runtime, Mapping):
        raise InterfaceMetadataError(
            "pyproject.toml is missing [tool.pyscript.runtime]"
        )
    runtime_unknown = set(runtime) - {"label"}
    if runtime_unknown:
        raise InterfaceMetadataError(
            "unsupported [tool.pyscript.runtime] field(s): "
            + ", ".join(sorted(str(field) for field in runtime_unknown))
        )
    runtime_profile = runtime.get("label")
    if not isinstance(runtime_profile, str):
        raise InterfaceMetadataError("tool.pyscript.runtime.label must be a string")

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
        if "request_schema" in endpoint:
            raise InterfaceMetadataError(
                f"endpoints[{index}].request_schema is not supported; declare "
                "function parameters directly under the endpoint"
            )

        endpoint_metadata = {
            name: value
            for name, value in endpoint.items()
            if name in ENDPOINT_METADATA_FIELDS
        }
        if endpoint.get("grpc") is not None and "io_type" not in endpoint_metadata:
            endpoint_metadata["io_type"] = ["grpc"]
        parameters: dict[str, dict[str, Any]] = {}
        for name, schema in endpoint.items():
            if name in ENDPOINT_METADATA_FIELDS:
                continue
            if not isinstance(name, str) or not PYTHON_PARAMETER_NAME.fullmatch(name):
                raise InterfaceMetadataError(
                    f"endpoints[{index}] parameter {name!r} is not a valid "
                    "Python parameter name"
                )
            if name == "context":
                raise InterfaceMetadataError(
                    f"endpoints[{index}].context is reserved by the platform"
                )
            _validate_schema(schema, f"endpoints[{index}].{name}")
            parameters[name] = dict(schema)

        if endpoint.get("grpc") is not None and parameters:
            raise InterfaceMetadataError(
                f"endpoints[{index}] is a native gRPC endpoint and cannot declare "
                "flattened HTTP parameters; its protobuf request is passed as one "
                "positional argument"
            )

        response_schema = endpoint.get("response_schema")
        if response_schema is not None:
            _validate_schema(
                response_schema,
                f"endpoints[{index}].response_schema",
            )
        normalized_endpoints.append(
            {**endpoint_metadata, "parameters": parameters}
        )

    try:
        return RevisionInterfaceSpec.model_validate(
            {
                "endpoints": normalized_endpoints,
                "grpc_contract": tool_metadata.get("grpc_contract"),
                "runtime_profile": runtime_profile,
                "requires_python": requires_python,
                "dependencies": dependencies,
            }
        )
    except ValidationError as error:
        raise InterfaceMetadataError(
            f"invalid tool.pyscript interface metadata: {error}"
        ) from error


def _validate_schema(value: Any, path: str) -> None:
    if not isinstance(value, Mapping):
        raise InterfaceMetadataError(f"{path} must be a table")
    schema_type = value.get("type")
    if not isinstance(schema_type, str) or schema_type not in PLATFORM_TYPES:
        raise InterfaceMetadataError(
            f"{path}.type must be a supported pyscripts data type"
        )
    if schema_type == "Struct":
        _reject_unknown_schema_fields(value, STRUCT_FIELDS, path)
        properties = value.get("properties", {})
        if not isinstance(properties, Mapping):
            raise InterfaceMetadataError(f"{path}.properties must be a table")
        required = value.get("required", [])
        if (
            not isinstance(required, list)
            or not all(isinstance(item, str) for item in required)
            or len(required) != len(set(required))
        ):
            raise InterfaceMetadataError(
                f"{path}.required must be an array of unique field names"
            )
        additional = value.get("additionalProperties", True)
        if not isinstance(additional, bool):
            raise InterfaceMetadataError(
                f"{path}.additionalProperties must be a boolean"
            )
        for name, child in properties.items():
            if not isinstance(name, str) or not name:
                raise InterfaceMetadataError(
                    f"{path}.properties keys must be non-empty strings"
                )
            _validate_schema(child, f"{path}.properties.{name}")
    elif schema_type == "List":
        _reject_unknown_schema_fields(value, LIST_FIELDS, path)
        if "items" not in value:
            raise InterfaceMetadataError(f"{path}.items is required for List")
        _validate_schema(value["items"], f"{path}.items")
    else:
        _reject_unknown_schema_fields(value, SCHEMA_METADATA_FIELDS, path)

    description = value.get("description")
    if description is not None and not isinstance(description, str):
        raise InterfaceMetadataError(f"{path}.description must be a string")
    nullable = value.get("nullable")
    if nullable is not None and not isinstance(nullable, bool):
        raise InterfaceMetadataError(f"{path}.nullable must be a boolean")


def _reject_unknown_schema_fields(
    value: Mapping[str, Any],
    allowed: set[str],
    path: str,
) -> None:
    unknown = set(value) - allowed
    if unknown:
        raise InterfaceMetadataError(
            f"{path} contains unsupported field(s): "
            + ", ".join(sorted(str(field) for field in unknown))
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
