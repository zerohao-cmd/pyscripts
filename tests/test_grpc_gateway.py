from __future__ import annotations

import uuid
from dataclasses import replace

import grpc
import pytest
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from pydantic import SecretStr

from pyscripts.api_tokens import hash_api_token, issue_api_token
from pyscripts.config import Settings
from pyscripts.db import (
    create_engine,
    create_schema,
    create_session_factory,
    session_scope,
)
from pyscripts.grpc_gateway.registry import GrpcRoute, GrpcRouteRegistry
from pyscripts.grpc_gateway.server import GrpcGateway, GrpcInvocationDispatcher
from pyscripts.repository import PlatformRepository, ResolvedEndpoint
from pyscripts.runtime.output import CapturedLogChunk, ExecutionOutcome


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


class GrpcAuthenticationError(RuntimeError):
    def __init__(self, code: grpc.StatusCode, detail: str):
        super().__init__(detail)
        self.code = code


class FakeGrpcContext:
    def __init__(self, authorization: str | None):
        self.authorization = authorization
        self.trailing_metadata = ()

    def invocation_metadata(self):
        if self.authorization is None:
            return ()
        return (("authorization", self.authorization),)

    async def abort(self, code, detail):
        raise GrpcAuthenticationError(code, detail)

    def set_trailing_metadata(self, metadata):
        self.trailing_metadata = metadata


async def test_dispatcher_requires_valid_api_token(tmp_path) -> None:
    settings = Settings(
        database_url=SecretStr(f"sqlite+aiosqlite:///{tmp_path / 'grpc-auth.db'}"),
        grpc_enabled=False,
    )
    engine = create_engine(settings)
    await create_schema(engine)
    factory = create_session_factory(engine)

    class Scheduler:
        async def execute_grpc(self, target, request_id, payload):
            return b"authorized-response"

    dispatcher = GrpcInvocationDispatcher(factory, Scheduler(), 5)
    route = GrpcRoute(
        "/examples.echo.v1.EchoService/Echo",
        _target("rev-auth"),
    )

    for authorization in (None, "Bearer pst_invalid"):
        with pytest.raises(GrpcAuthenticationError) as rejected:
            await dispatcher(route, b"request", FakeGrpcContext(authorization))
        assert rejected.value.code == grpc.StatusCode.UNAUTHENTICATED

    value, token_hash, token_prefix = issue_api_token()
    async with session_scope(factory) as session:
        await PlatformRepository(session).create_api_token(
            name="grpc-test",
            token_hash=token_hash,
            token_prefix=token_prefix,
        )

    context = FakeGrpcContext(f"Bearer {value}")
    response = await dispatcher(route, b"request", context)
    assert response == b"authorized-response"
    assert dict(context.trailing_metadata)["pyscripts-revision"] == "rev-auth"
    assert dict(context.trailing_metadata)["pyscripts-status"] == "SUCCEEDED"
    assert await _token_is_valid(factory, value)

    class FailingScheduler:
        async def execute_grpc(self, target, request_id, payload):
            return ExecutionOutcome(
                succeeded=False,
                value=b"typed-failure-response",
                logs=(CapturedLogChunk(0, "STDERR", "failed\n"),),
                log_bytes=7,
                error_type="ValueError",
                error_message="invalid input",
            )

    generated_target = replace(
        _target("rev-failed"),
        grpc={
            "service": "examples.echo.v1.EchoService",
            "method": "Echo",
            "descriptor_path": "descriptor.pb",
            "generated": True,
        },
    )
    failure_context = FakeGrpcContext(f"Bearer {value}")
    failure_response = await GrpcInvocationDispatcher(
        factory,
        FailingScheduler(),
        5,
    )(
        GrpcRoute("/examples.echo.v1.EchoService/Echo", generated_target),
        b"request",
        failure_context,
    )
    assert failure_response == b"typed-failure-response"
    assert dict(failure_context.trailing_metadata)["pyscripts-status"] == "FAILED"
    await engine.dispose()


async def _token_is_valid(factory, value: str) -> bool:
    async with factory() as session:
        return await PlatformRepository(session).validate_api_token(
            hash_api_token(value)
        )
