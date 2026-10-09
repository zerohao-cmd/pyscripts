from __future__ import annotations

import uuid
from pathlib import Path

from google.protobuf import descriptor_pb2
from grpc_tools import protoc

from pyscripts.contracts.generator import generate_contract
from pyscripts.schemas import EndpointSpec


SERVICE_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _endpoint(*, x_type: str = "Int64", include_x: bool = True) -> EndpointSpec:
    parameters = {"y": {"type": "Int64"}}
    if include_x:
        parameters["x"] = {"type": x_type}
    return EndpointSpec(
        id="add",
        task_type="compute",
        entrypoint="service:add",
        io_type=["rest", "grpc"],
        parameters=parameters,
        response_schema={"type": "Int64"},
    )


def _compile_descriptor(tmp_path: Path, source: str) -> bytes:
    proto = tmp_path / "service.proto"
    descriptor = tmp_path / "descriptor.pb"
    proto.write_text(source, encoding="utf-8")
    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{tmp_path}",
            f"--descriptor_set_out={descriptor}",
            "service.proto",
        ]
    )
    assert result == 0
    return descriptor.read_bytes()


def _request_fields(payload: bytes) -> dict[str, int]:
    descriptor = descriptor_pb2.FileDescriptorSet.FromString(payload)
    request = next(
        message
        for file_proto in descriptor.file
        for message in file_proto.message_type
        if message.name == "AddRequest"
    )
    return {field.name: field.number for field in request.field}


def test_generated_proto_is_deterministic_and_preserves_field_numbers(
    tmp_path: Path,
) -> None:
    first = generate_contract("math-service", [_endpoint()], service_id=SERVICE_ID)
    assert first is not None
    first_descriptor = _compile_descriptor(tmp_path, first.proto_source)
    assert first.contract.version == "1.0.0"
    assert first.package == (
        "pyscripts.generated."
        "math_service_s11111111222233334444555555555555.v1"
    )
    assert _request_fields(first_descriptor) == {"x": 1, "y": 2}
    assert first.endpoints[0].grpc is not None
    assert first.endpoints[0].grpc.generated is True
    assert "PyscriptsExecution execution" in first.proto_source
    assert "message PyscriptsExecution {" in first.proto_source
    assert "repeated PyscriptsExecutionLog logs" in first.proto_source

    unchanged = generate_contract(
        "math-service",
        [_endpoint()],
        service_id=SERVICE_ID,
        previous_descriptor=first_descriptor,
        previous_version="1.0.0",
    )
    assert unchanged is not None
    assert unchanged.breaking is False
    assert unchanged.proto_source == first.proto_source
    assert _request_fields(
        _compile_descriptor(tmp_path, unchanged.proto_source)
    ) == {"x": 1, "y": 2}

    changed = generate_contract(
        "math-service",
        [_endpoint(x_type="String")],
        service_id=SERVICE_ID,
        previous_descriptor=first_descriptor,
        previous_version="1.0.0",
    )
    assert changed is not None
    assert changed.breaking is True
    assert changed.contract.version == "2.0.0"
    assert changed.package == (
        "pyscripts.generated."
        "math_service_s11111111222233334444555555555555.v2"
    )
    assert "reserved 1;" in changed.proto_source
    assert "string x = 3;" in changed.proto_source
    assert "int64 y = 2;" in changed.proto_source


def test_removed_field_is_reserved(tmp_path: Path) -> None:
    first = generate_contract("math-service", [_endpoint()], service_id=SERVICE_ID)
    assert first is not None
    descriptor = _compile_descriptor(tmp_path, first.proto_source)

    removed = generate_contract(
        "math-service",
        [_endpoint(include_x=False)],
        service_id=SERVICE_ID,
        previous_descriptor=descriptor,
        previous_version="1.0.0",
    )
    assert removed is not None
    assert removed.breaking is True
    assert 'reserved "x";' in removed.proto_source
    assert "reserved 1;" in removed.proto_source
    assert "int64 y = 2;" in removed.proto_source


def test_arrow_table_uses_arrow_ipc_bytes_in_generated_proto() -> None:
    generated = generate_contract(
        "frame-service",
        [_endpoint(x_type="ArrowTable")],
        service_id=SERVICE_ID,
    )

    assert generated is not None
    assert "bytes x = 1;" in generated.proto_source


def test_legacy_response_without_execution_requires_major_upgrade(
    tmp_path: Path,
) -> None:
    current = generate_contract("math-service", [_endpoint()], service_id=SERVICE_ID)
    assert current is not None
    legacy_source = "\n".join(
        line
        for line in current.proto_source.splitlines()
        if "PyscriptsExecution execution" not in line
    )
    legacy_descriptor = _compile_descriptor(tmp_path, legacy_source)

    upgraded = generate_contract(
        "math-service",
        [_endpoint()],
        service_id=SERVICE_ID,
        previous_descriptor=legacy_descriptor,
        previous_version="1.4.0",
    )

    assert upgraded is not None
    assert upgraded.breaking is True
    assert upgraded.contract.version == "2.0.0"


def test_service_namespace_distinguishes_normalized_name_collisions() -> None:
    first = generate_contract(
        "foo-bar",
        [_endpoint()],
        service_id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
    )
    second = generate_contract(
        "foo_bar",
        [_endpoint()],
        service_id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000002"),
    )

    assert first is not None
    assert second is not None
    assert first.package != second.package
    assert first.endpoints[0].grpc is not None
    assert second.endpoints[0].grpc is not None
    assert first.endpoints[0].grpc.service != second.endpoints[0].grpc.service


def test_legacy_slug_namespace_migration_bumps_major(tmp_path: Path) -> None:
    first = generate_contract(
        "math-service",
        [_endpoint()],
        service_id=SERVICE_ID,
    )
    assert first is not None
    legacy_source = first.proto_source.replace(
        first.package,
        "pyscripts.generated.math_service.v2",
    )
    legacy_descriptor = _compile_descriptor(tmp_path, legacy_source)

    migrated = generate_contract(
        "math-service",
        [_endpoint()],
        service_id=SERVICE_ID,
        previous_descriptor=legacy_descriptor,
        previous_version="2.0.0",
    )

    assert migrated is not None
    assert migrated.breaking is True
    assert migrated.contract.version == "3.0.0"
    assert migrated.package.endswith(".v3")
