from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from google.protobuf import (
    descriptor_pb2,
    descriptor_pool,
    json_format,
    message_factory,
)
from google.protobuf.message import DecodeError, Message


class ProtobufContractError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class GrpcEndpointDefinition:
    service: str
    method: str
    descriptor_path: str = "descriptor.pb"

    @property
    def method_path(self) -> str:
        return f"/{self.service}/{self.method}"


@dataclass(frozen=True, slots=True)
class GrpcCodec:
    method_path: str
    request_type: type[Message]
    response_type: type[Message]

    def parse_request(self, payload: bytes) -> Message:
        try:
            return self.request_type.FromString(payload)
        except DecodeError as error:
            raise ProtobufContractError(
                f"request is not valid {self.request_type.DESCRIPTOR.full_name}"
            ) from error

    def serialize_response(self, value: Any) -> bytes:
        if isinstance(value, bytes):
            try:
                self.response_type.FromString(value)
            except DecodeError as error:
                raise ProtobufContractError(
                    f"handler returned invalid {self.response_type.DESCRIPTOR.full_name} bytes"
                ) from error
            return value

        if isinstance(value, Message):
            expected = self.response_type.DESCRIPTOR.full_name
            if value.DESCRIPTOR.full_name != expected:
                raise ProtobufContractError(
                    f"handler returned {value.DESCRIPTOR.full_name}, expected {expected}"
                )
            return value.SerializeToString()

        response = self.response_type()
        if isinstance(value, Mapping):
            try:
                json_format.ParseDict(dict(value), response)
            except (TypeError, ValueError, json_format.ParseError) as error:
                raise ProtobufContractError(
                    f"handler result does not match {response.DESCRIPTOR.full_name}"
                ) from error
            return response.SerializeToString()

        if value is None and not response.DESCRIPTOR.fields:
            return response.SerializeToString()

        raise ProtobufContractError(
            "gRPC handler must return a protobuf Message, serialized bytes, "
            "or a mapping compatible with the response message"
        )


def load_grpc_codecs(
    artifact_root: Path,
    endpoints: Sequence[tuple[str, GrpcEndpointDefinition]],
) -> tuple[descriptor_pool.DescriptorPool | None, dict[str, GrpcCodec]]:
    if not endpoints:
        return None, {}

    descriptor_files: dict[str, descriptor_pb2.FileDescriptorProto] = {}
    for descriptor_path in {definition.descriptor_path for _, definition in endpoints}:
        path = _artifact_file(artifact_root, descriptor_path)
        descriptor_set = descriptor_pb2.FileDescriptorSet()
        try:
            descriptor_set.ParseFromString(path.read_bytes())
        except DecodeError as error:
            raise ProtobufContractError(
                f"invalid FileDescriptorSet: {descriptor_path}"
            ) from error
        if not descriptor_set.file:
            raise ProtobufContractError(
                f"FileDescriptorSet contains no files: {descriptor_path}"
            )
        for file_proto in descriptor_set.file:
            existing = descriptor_files.get(file_proto.name)
            if (
                existing is not None
                and existing.SerializeToString() != file_proto.SerializeToString()
            ):
                raise ProtobufContractError(
                    f"conflicting protobuf descriptor: {file_proto.name}"
                )
            descriptor_files[file_proto.name] = file_proto

    pool = descriptor_pool.DescriptorPool()
    local_names = set(descriptor_files)
    external_dependencies = {
        dependency
        for file_proto in descriptor_files.values()
        for dependency in file_proto.dependency
        if dependency not in local_names
    }
    loaded_external: set[str] = set()
    for dependency in sorted(external_dependencies):
        _copy_default_descriptor(pool, dependency, loaded_external)

    pending = dict(descriptor_files)
    added = set(loaded_external)
    while pending:
        progressed = False
        for name, file_proto in list(pending.items()):
            if all(dependency in added for dependency in file_proto.dependency):
                pool.Add(file_proto)
                added.add(name)
                pending.pop(name)
                progressed = True
        if not progressed:
            unresolved = ", ".join(sorted(pending))
            raise ProtobufContractError(
                f"protobuf descriptors have unresolved dependencies: {unresolved}"
            )

    codecs: dict[str, GrpcCodec] = {}
    for endpoint_id, definition in endpoints:
        try:
            service = pool.FindServiceByName(definition.service)
            method = service.methods_by_name[definition.method]
        except (KeyError, RuntimeError) as error:
            raise ProtobufContractError(
                f"descriptor does not define {definition.method_path}"
            ) from error
        if method.client_streaming or method.server_streaming:
            raise ProtobufContractError(
                f"streaming RPC is not supported yet: {definition.method_path}"
            )
        codecs[endpoint_id] = GrpcCodec(
            method_path=definition.method_path,
            request_type=message_factory.GetMessageClass(method.input_type),
            response_type=message_factory.GetMessageClass(method.output_type),
        )
    return pool, codecs


def _artifact_file(root: Path, relative_path: str) -> Path:
    path = (root / relative_path).resolve()
    resolved_root = root.resolve()
    if not path.is_relative_to(resolved_root) or not path.is_file():
        raise ProtobufContractError(
            f"protobuf descriptor does not exist in artifact: {relative_path}"
        )
    return path


def _copy_default_descriptor(
    pool: descriptor_pool.DescriptorPool,
    name: str,
    loaded: set[str],
) -> None:
    if name in loaded:
        return
    try:
        descriptor = descriptor_pool.Default().FindFileByName(name)
    except KeyError as error:
        raise ProtobufContractError(
            f"protobuf dependency is missing from the descriptor set: {name}"
        ) from error
    for dependency in descriptor.dependencies:
        _copy_default_descriptor(pool, dependency.name, loaded)
    pool.AddSerializedFile(descriptor.serialized_pb)
    loaded.add(name)
