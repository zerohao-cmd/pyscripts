from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from grpc_tools import protoc

from pyscripts.contracts.generator import (
    GENERATED_DESCRIPTOR_PATH,
    generate_contract,
)
from pyscripts.runtime.loader import EndpointDefinition, VersionedRuntime
from pyscripts.runtime.protobuf import GrpcEndpointDefinition
from pyscripts.schemas import EndpointSpec


def _contract() -> tuple[bytes, type, type]:
    file_proto = descriptor_pb2.FileDescriptorProto(
        name="calculator.proto",
        package="examples.calculator.v1",
        syntax="proto3",
    )
    request = file_proto.message_type.add(name="MultiplyRequest")
    request.field.add(
        name="value",
        number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    )
    response = file_proto.message_type.add(name="MultiplyResponse")
    response.field.add(
        name="value",
        number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    )
    response.field.add(
        name="revision",
        number=2,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    )
    service = file_proto.service.add(name="Calculator")
    method = service.method.add(name="Multiply")
    method.input_type = ".examples.calculator.v1.MultiplyRequest"
    method.output_type = ".examples.calculator.v1.MultiplyResponse"

    descriptor_set = descriptor_pb2.FileDescriptorSet()
    descriptor_set.file.add().CopyFrom(file_proto)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    request_type = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("examples.calculator.v1.MultiplyRequest")
    )
    response_type = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("examples.calculator.v1.MultiplyResponse")
    )
    return descriptor_set.SerializeToString(), request_type, response_type


def _artifact(path: Path, factor: int, descriptor: bytes) -> str:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("descriptor.pb", descriptor)
        archive.writestr(
            "service.py",
            "def multiply(request):\n"
            f"    return {{'value': request.value * {factor}, "
            "'revision': 'rev-1'}\n",
        )
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def test_grpc_payload_is_decoded_and_encoded_inside_revision(
    tmp_path: Path,
) -> None:
    descriptor, request_type, response_type = _contract()
    artifact = tmp_path / "calculator.zip"
    digest = _artifact(artifact, 3, descriptor)
    runtime = VersionedRuntime(tmp_path / "cache")
    endpoints = [
        EndpointDefinition(
            id="multiply",
            task_type="compute",
            entrypoint="service:multiply",
            grpc=GrpcEndpointDefinition(
                service="examples.calculator.v1.Calculator",
                method="Multiply",
            ),
        )
    ]

    payload = await runtime.execute_grpc(
        "calculator",
        "rev-1",
        "multiply",
        {"request_id": "request-1", "revision": "rev-1"},
        request_type(value=7).SerializeToString(),
        artifact_uri=artifact.as_uri(),
        artifact_digest=digest,
        endpoints=endpoints,
    )

    result = response_type.FromString(payload)
    assert result.value == 21
    assert result.revision == "rev-1"


async def test_generated_grpc_flattens_request_into_python_arguments(
    tmp_path: Path,
) -> None:
    spec = EndpointSpec(
        id="add",
        task_type="compute",
        entrypoint="service:add",
        io_type=["rest", "grpc"],
        parameters={"x": {"type": "Int64"}, "y": {"type": "Int64"}},
        response_schema={"type": "Int64"},
    )
    generated = generate_contract("math-service", [spec])
    assert generated is not None

    proto_root = tmp_path / "proto"
    proto_root.mkdir()
    proto = proto_root / "service.proto"
    proto.write_text(generated.proto_source, encoding="utf-8")
    descriptor_path = tmp_path / "descriptor.pb"
    assert protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{proto_root}",
            f"--descriptor_set_out={descriptor_path}",
            "service.proto",
        ]
    ) == 0
    descriptor = descriptor_pb2.FileDescriptorSet.FromString(
        descriptor_path.read_bytes()
    )
    pool = descriptor_pool.DescriptorPool()
    for file_proto in descriptor.file:
        pool.Add(file_proto)
    request_type = message_factory.GetMessageClass(
        pool.FindMessageTypeByName(f"{generated.package}.AddRequest")
    )
    response_type = message_factory.GetMessageClass(
        pool.FindMessageTypeByName(f"{generated.package}.AddResponse")
    )

    artifact = tmp_path / "generated.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            GENERATED_DESCRIPTOR_PATH,
            descriptor_path.read_bytes(),
        )
        archive.writestr("service.py", "def add(x, y):\n    return x + y\n")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    generated_grpc = generated.endpoints[0].grpc
    assert generated_grpc is not None
    runtime = VersionedRuntime(tmp_path / "generated-cache")
    endpoints = [
        EndpointDefinition(
            id="add",
            task_type="compute",
            entrypoint="service:add",
            parameters=spec.parameters,
            response_schema=spec.response_schema,
            io_type=("rest", "grpc"),
            grpc=GrpcEndpointDefinition(
                service=generated_grpc.service,
                method=generated_grpc.method,
                descriptor_path=generated_grpc.descriptor_path,
                generated=True,
                response_wrapped=True,
            ),
        )
    ]

    payload = await runtime.execute_grpc(
        "math-service",
        "rev-1",
        "add",
        {"request_id": "request-1", "revision": "rev-1"},
        request_type(x=7, y=5).SerializeToString(),
        artifact_uri=artifact.as_uri(),
        artifact_digest=digest,
        endpoints=endpoints,
    )

    assert response_type.FromString(payload).result == 12
