from __future__ import annotations

import uuid

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pydantic import SecretStr

from pyscripts.config import Settings
from pyscripts.grpc_gateway.registry import GrpcRouteRegistry
from pyscripts.grpc_gateway.server import GrpcGateway
from pyscripts.repository import ResolvedEndpoint


def _messages() -> tuple[type, type]:
    file_proto = descriptor_pb2.FileDescriptorProto(
        name="echo.proto",
        package="examples.echo.v1",
        syntax="proto3",
    )
    request = file_proto.message_type.add(name="EchoRequest")
    request.field.add(
        name="value",
        number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    )
    response = file_proto.message_type.add(name="EchoResponse")
    response.field.add(
        name="value",
        number=1,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    )
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    return (
        message_factory.GetMessageClass(
            pool.FindMessageTypeByName("examples.echo.v1.EchoRequest")
        ),
        message_factory.GetMessageClass(
            pool.FindMessageTypeByName("examples.echo.v1.EchoResponse")
        ),
    )


def _target(revision: str) -> ResolvedEndpoint:
    grpc_spec = {
        "service": "examples.echo.v1.EchoService",
        "method": "Echo",
        "descriptor_path": "descriptor.pb",
    }
    return ResolvedEndpoint(
        service_id=uuid.uuid4(),
        service_name="echo-service",
        revision_id=uuid.uuid4(),
        revision=revision,
        artifact_uri="file:///unused.zip",
        artifact_digest="a" * 64,
        runtime_profile="test",
        endpoint_manifest=[],
        endpoint_id="echo",
        task_type="io",
        entrypoint="service:echo",
        grpc=grpc_spec,
    )


async def test_generated_stub_style_call_uses_dynamic_native_method() -> None:
    request_type, response_type = _messages()
    registry = GrpcRouteRegistry()
    registry.replace([_target("rev-1")])

    async def dispatch(route, payload, context):
        request = request_type.FromString(payload)
        return response_type(
            value=f"{route.target.revision}:{request.value}"
        ).SerializeToString()

    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        grpc_host="127.0.0.1",
        grpc_port=0,
    )
    gateway = GrpcGateway(settings, registry, dispatch)
    port = await gateway.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    echo = channel.unary_unary(
        "/examples.echo.v1.EchoService/Echo",
        request_serializer=request_type.SerializeToString,
        response_deserializer=response_type.FromString,
    )
    try:
        first = await echo(request_type(value="hello"))
        assert first.value == "rev-1:hello"

        registry.replace([_target("rev-2")])
        second = await echo(request_type(value="hello"))
        assert second.value == "rev-2:hello"
    finally:
        await channel.close()
        await gateway.stop()
