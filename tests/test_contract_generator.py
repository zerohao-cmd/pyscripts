from __future__ import annotations

from pathlib import Path

from google.protobuf import descriptor_pb2
from grpc_tools import protoc

from pyscripts.contracts.generator import generate_contract
from pyscripts.schemas import EndpointSpec


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
    first = generate_contract("math-service", [_endpoint()])
    assert first is not None
    first_descriptor = _compile_descriptor(tmp_path, first.proto_source)
    assert first.contract.version == "1.0.0"
    assert first.package == "pyscripts.generated.math_service.v1"
    assert _request_fields(first_descriptor) == {"x": 1, "y": 2}
    assert first.endpoints[0].grpc is not None
    assert first.endpoints[0].grpc.generated is True

    unchanged = generate_contract(
        "math-service",
        [_endpoint()],
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
        previous_descriptor=first_descriptor,
        previous_version="1.0.0",
    )
    assert changed is not None
    assert changed.breaking is True
    assert changed.contract.version == "2.0.0"
    assert changed.package == "pyscripts.generated.math_service.v2"
    assert "reserved 1;" in changed.proto_source
    assert "string x = 3;" in changed.proto_source
    assert "int64 y = 2;" in changed.proto_source


def test_removed_field_is_reserved(tmp_path: Path) -> None:
    first = generate_contract("math-service", [_endpoint()])
    assert first is not None
    descriptor = _compile_descriptor(tmp_path, first.proto_source)

    removed = generate_contract(
        "math-service",
        [_endpoint(include_x=False)],
        previous_descriptor=descriptor,
        previous_version="1.0.0",
    )
    assert removed is not None
    assert removed.breaking is True
    assert 'reserved "x";' in removed.proto_source
    assert "reserved 1;" in removed.proto_source
    assert "int64 y = 2;" in removed.proto_source
