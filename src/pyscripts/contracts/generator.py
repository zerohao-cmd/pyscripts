from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from google.protobuf import descriptor_pb2
from packaging.version import Version

from pyscripts.schemas import EndpointSpec, GrpcContractSpec, GrpcEndpointSpec


class ProtoGenerationError(RuntimeError):
    pass


GENERATED_PROTO_ROOT = ".pyscripts/generated/proto"
GENERATED_DESCRIPTOR_PATH = ".pyscripts/generated/descriptor.pb"

_SCALAR_TYPES = {
    "Float": "float",
    "Double": "double",
    "Int32": "int32",
    "Int64": "int64",
    "Uint32": "uint32",
    "Uint64": "uint64",
    "Sint32": "sint32",
    "Sint64": "sint64",
    "Fixed32": "fixed32",
    "Fixed64": "fixed64",
    "Sfixed32": "sfixed32",
    "Sfixed64": "sfixed64",
    "Bool": "bool",
    "String": "string",
    "Bytes": "bytes",
    "Datetime": "sint64",
    "Int": "sint32",
    "Bigint": "sint64",
}
_PROTO_TYPE_NAMES = {
    descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE: "double",
    descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT: "float",
    descriptor_pb2.FieldDescriptorProto.TYPE_INT64: "int64",
    descriptor_pb2.FieldDescriptorProto.TYPE_UINT64: "uint64",
    descriptor_pb2.FieldDescriptorProto.TYPE_INT32: "int32",
    descriptor_pb2.FieldDescriptorProto.TYPE_FIXED64: "fixed64",
    descriptor_pb2.FieldDescriptorProto.TYPE_FIXED32: "fixed32",
    descriptor_pb2.FieldDescriptorProto.TYPE_BOOL: "bool",
    descriptor_pb2.FieldDescriptorProto.TYPE_STRING: "string",
    descriptor_pb2.FieldDescriptorProto.TYPE_BYTES: "bytes",
    descriptor_pb2.FieldDescriptorProto.TYPE_UINT32: "uint32",
    descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED32: "sfixed32",
    descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED64: "sfixed64",
    descriptor_pb2.FieldDescriptorProto.TYPE_SINT32: "sint32",
    descriptor_pb2.FieldDescriptorProto.TYPE_SINT64: "sint64",
}


@dataclass(frozen=True, slots=True)
class GeneratedContract:
    proto_source: str
    proto_path: str
    endpoints: list[EndpointSpec]
    contract: GrpcContractSpec
    package: str
    service: str
    breaking: bool


@dataclass(slots=True)
class FieldShape:
    name: str
    type_name: str
    repeated: bool = False
    number: int = 0

    @property
    def signature(self) -> tuple[str, bool]:
        return self.type_name, self.repeated


@dataclass(slots=True)
class MessageShape:
    name: str
    direction: str
    origin: str
    fields: list[FieldShape] = field(default_factory=list)
    reserved_numbers: set[int] = field(default_factory=set)
    reserved_names: set[str] = field(default_factory=set)


@dataclass(frozen=True, slots=True)
class PreviousField:
    number: int
    signature: tuple[str, bool]


@dataclass(frozen=True, slots=True)
class PreviousMessage:
    fields: dict[str, PreviousField]
    reserved_numbers: frozenset[int]
    reserved_names: frozenset[str]


def generate_contract(
    service_name: str,
    endpoints: list[EndpointSpec],
    *,
    previous_descriptor: bytes | None = None,
    previous_version: str | None = None,
) -> GeneratedContract | None:
    grpc_endpoints = [endpoint for endpoint in endpoints if "grpc" in endpoint.io_type]
    if not grpc_endpoints:
        return None
    if any(endpoint.grpc is not None for endpoint in grpc_endpoints):
        return None

    previous_messages, previous_methods = _read_previous(previous_descriptor)
    messages: dict[str, MessageShape] = {}
    origins: dict[str, str] = {}
    methods: list[tuple[str, str, str, EndpointSpec]] = []
    breaking = False

    for endpoint in grpc_endpoints:
        method = _pascal(endpoint.id)
        request_name = f"{method}Request"
        response_name = f"{method}Response"
        if any(existing[0] == method for existing in methods):
            raise ProtoGenerationError(
                f"endpoint ids produce the same gRPC method name: {method}"
            )
        request = _message(
            messages,
            origins,
            request_name,
            "request",
            f"endpoint:{endpoint.id}:request",
        )
        for name, schema in sorted(endpoint.parameters.items()):
            request.fields.append(
                _field_from_schema(
                    messages,
                    origins,
                    request_name,
                    name,
                    schema,
                    "request",
                    f"endpoint:{endpoint.id}:request.{name}",
                )
            )

        response_schema = endpoint.response_schema
        response_type = response_schema.get("type")
        if response_type == "Struct":
            _require_closed_struct(
                response_schema, f"endpoint {endpoint.id} response_schema"
            )
            for name, schema in sorted(
                dict(response_schema.get("properties", {})).items()
            ):
                response = _message(
                    messages,
                    origins,
                    response_name,
                    "response",
                    f"endpoint:{endpoint.id}:response",
                )
                response.fields.append(
                    _field_from_schema(
                        messages,
                        origins,
                        response_name,
                        name,
                        schema,
                        "response",
                        f"endpoint:{endpoint.id}:response.{name}",
                    )
                )
            _message(
                messages,
                origins,
                response_name,
                "response",
                f"endpoint:{endpoint.id}:response",
            )
            response_wrapped = False
        else:
            response = _message(
                messages,
                origins,
                response_name,
                "response",
                f"endpoint:{endpoint.id}:response",
            )
            response.fields.append(
                _field_from_schema(
                    messages,
                    origins,
                    response_name,
                    "result",
                    response_schema,
                    "response",
                    f"endpoint:{endpoint.id}:response.result",
                )
            )
            response_wrapped = True
        methods.append((method, request_name, response_name, endpoint))

    for message in messages.values():
        if _assign_numbers(message, previous_messages.get(message.name)):
            breaking = True

    method_names = {method for method, _, _, _ in methods}
    if previous_methods - method_names:
        breaking = True

    previous = Version(previous_version or "0")
    if previous.major < 1:
        version = Version("1.0.0")
    elif breaking:
        version = Version(f"{previous.major + 1}.0.0")
    else:
        version = Version(f"{previous.major}.{previous.minor + 1}.0")

    package_segment = re.sub(r"[^a-z0-9_]", "_", service_name.lower())
    package = f"pyscripts.generated.{package_segment}.v{version.major}"
    service_base = _pascal(service_name)
    service = (
        service_base
        if service_base.endswith("Service")
        else f"{service_base}Service"
    )
    python_module_root = f"pyscripts_{package_segment}_sdk_proto"
    proto_path = (
        f"{GENERATED_PROTO_ROOT}/{python_module_root}/"
        f"v{version.major}/service.proto"
    )
    source = _render_proto(package, service, messages.values(), methods)

    generated_endpoints: list[EndpointSpec] = []
    response_wrapped_by_id = {
        endpoint.id: endpoint.response_schema.get("type") != "Struct"
        for _, _, _, endpoint in methods
    }
    method_by_id = {endpoint.id: method for method, _, _, endpoint in methods}
    for endpoint in endpoints:
        if endpoint.id not in method_by_id:
            generated_endpoints.append(endpoint)
            continue
        generated_endpoints.append(
            endpoint.model_copy(
                update={
                    "grpc": GrpcEndpointSpec(
                        service=f"{package}.{service}",
                        method=method_by_id[endpoint.id],
                        descriptor_path=GENERATED_DESCRIPTOR_PATH,
                        generated=True,
                        response_wrapped=response_wrapped_by_id[endpoint.id],
                    )
                }
            )
        )

    return GeneratedContract(
        proto_source=source,
        proto_path=proto_path,
        endpoints=generated_endpoints,
        contract=GrpcContractSpec(
            version=str(version),
            proto_root=GENERATED_PROTO_ROOT,
        ),
        package=package,
        service=service,
        breaking=breaking,
    )


def _message(
    messages: dict[str, MessageShape],
    origins: dict[str, str],
    name: str,
    direction: str,
    origin: str,
) -> MessageShape:
    existing_origin = origins.get(name)
    if existing_origin is not None and existing_origin != origin:
        raise ProtoGenerationError(
            f"interface paths {existing_origin!r} and {origin!r} generate "
            f"the same protobuf message {name!r}"
        )
    origins[name] = origin
    return messages.setdefault(name, MessageShape(name, direction, origin))


def _field_from_schema(
    messages: dict[str, MessageShape],
    origins: dict[str, str],
    parent_name: str,
    field_name: str,
    schema: dict[str, Any],
    direction: str,
    origin: str,
) -> FieldShape:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field_name):
        raise ProtoGenerationError(
            f"{origin} field name is not a valid protobuf identifier"
        )
    schema_type = schema.get("type")
    scalar = _SCALAR_TYPES.get(schema_type)
    if scalar is not None:
        return FieldShape(field_name, scalar)
    if schema_type == "Date":
        _ensure_date(messages, origins, direction)
        return FieldShape(field_name, "PyscriptsDate")
    if schema_type == "DatetimeTz":
        _ensure_datetime_tz(messages, origins, direction)
        return FieldShape(field_name, "PyscriptsDatetimeTz")
    if schema_type == "Struct":
        _require_closed_struct(schema, origin)
        nested_name = f"{parent_name}{_pascal(field_name)}"
        nested = _message(messages, origins, nested_name, direction, origin)
        for name, child in sorted(dict(schema.get("properties", {})).items()):
            nested.fields.append(
                _field_from_schema(
                    messages,
                    origins,
                    nested_name,
                    name,
                    child,
                    direction,
                    f"{origin}.{name}",
                )
            )
        return FieldShape(field_name, nested_name)
    if schema_type == "List":
        item = schema.get("items")
        if not isinstance(item, dict):
            raise ProtoGenerationError(f"{origin}.items is required")
        if item.get("type") == "List":
            raise ProtoGenerationError(
                f"{origin} uses nested List, which is not supported by generated gRPC"
            )
        item_shape = _field_from_schema(
            messages,
            origins,
            parent_name,
            field_name,
            item,
            direction,
            f"{origin}.items",
        )
        item_shape.repeated = True
        return item_shape
    raise ProtoGenerationError(f"{origin} has unsupported type {schema_type!r}")


def _require_closed_struct(schema: dict[str, Any], origin: str) -> None:
    if schema.get("additionalProperties", True):
        raise ProtoGenerationError(
            f"{origin} must set additionalProperties = false for generated gRPC"
        )


def _ensure_date(
    messages: dict[str, MessageShape],
    origins: dict[str, str],
    direction: str,
) -> None:
    message = _message(
        messages, origins, "PyscriptsDate", direction, "platform:date"
    )
    if not message.fields:
        message.fields.extend(
            [
                FieldShape("year", "sint32"),
                FieldShape("month", "sint32"),
                FieldShape("day", "sint32"),
            ]
        )


def _ensure_datetime_tz(
    messages: dict[str, MessageShape],
    origins: dict[str, str],
    direction: str,
) -> None:
    message = _message(
        messages,
        origins,
        "PyscriptsDatetimeTz",
        direction,
        "platform:datetime_tz",
    )
    if not message.fields:
        message.fields.extend(
            [
                FieldShape("timestamp", "sint64"),
                FieldShape("offset_minutes", "sint32"),
            ]
        )


def _assign_numbers(
    message: MessageShape,
    previous: PreviousMessage | None,
) -> bool:
    previous_fields = previous.fields if previous else {}
    reserved_numbers = set(previous.reserved_numbers if previous else ())
    reserved_names = set(previous.reserved_names if previous else ())
    desired_names = {item.name for item in message.fields}
    reused_numbers: set[int] = set()
    breaking = False

    forbidden_names = desired_names & reserved_names
    if forbidden_names:
        raise ProtoGenerationError(
            f"protobuf message {message.name} reuses reserved field name(s): "
            + ", ".join(sorted(forbidden_names))
        )

    for item in message.fields:
        old = previous_fields.get(item.name)
        if old is not None and old.signature == item.signature:
            item.number = old.number
            reused_numbers.add(old.number)
        elif old is not None:
            reserved_numbers.add(old.number)
            breaking = True

    removed = set(previous_fields) - desired_names
    if removed:
        breaking = True
        for name in removed:
            reserved_numbers.add(previous_fields[name].number)
            reserved_names.add(name)

    occupied = reserved_numbers | {item.number for item in message.fields if item.number}
    occupied.update(item.number for item in previous_fields.values())
    next_number = max(occupied, default=0) + 1
    for item in sorted(message.fields, key=lambda field: field.name):
        if item.number:
            continue
        while next_number in occupied or 19000 <= next_number <= 19999:
            next_number += 1
        if next_number > 536_870_911:
            raise ProtoGenerationError(f"protobuf field space exhausted in {message.name}")
        item.number = next_number
        occupied.add(next_number)
        next_number += 1
        if previous is not None and message.direction == "request":
            breaking = True

    message.reserved_numbers = reserved_numbers
    message.reserved_names = reserved_names
    return breaking


def _read_previous(
    payload: bytes | None,
) -> tuple[dict[str, PreviousMessage], set[str]]:
    if not payload:
        return {}, set()
    descriptor = descriptor_pb2.FileDescriptorSet()
    try:
        descriptor.ParseFromString(payload)
    except Exception as error:
        raise ProtoGenerationError("previous descriptor is invalid") from error
    messages: dict[str, PreviousMessage] = {}
    methods: set[str] = set()
    for file_proto in descriptor.file:
        for message in file_proto.message_type:
            reserved_numbers: set[int] = set()
            for item in message.reserved_range:
                if item.end - item.start > 10_000:
                    raise ProtoGenerationError(
                        f"reserved range is too large in previous {message.name}"
                    )
                reserved_numbers.update(range(item.start, item.end))
            fields = {
                item.name: PreviousField(
                    item.number,
                    (_previous_type_name(item), item.label == item.LABEL_REPEATED),
                )
                for item in message.field
            }
            messages[message.name] = PreviousMessage(
                fields,
                frozenset(reserved_numbers),
                frozenset(message.reserved_name),
            )
        for service in file_proto.service:
            methods.update(method.name for method in service.method)
    return messages, methods


def _previous_type_name(field: descriptor_pb2.FieldDescriptorProto) -> str:
    if field.type == field.TYPE_MESSAGE:
        return field.type_name.rsplit(".", 1)[-1]
    return _PROTO_TYPE_NAMES.get(field.type, f"unsupported:{field.type}")


def _render_proto(
    package: str,
    service: str,
    messages: Iterable[MessageShape],
    methods: list[tuple[str, str, str, EndpointSpec]],
) -> str:
    lines = [
        'syntax = "proto3";',
        "",
        f"package {package};",
        "",
        f"service {service} {{",
    ]
    for method, request, response, _ in methods:
        lines.append(f"  rpc {method}({request}) returns ({response});")
    lines.extend(["}", ""])
    for message in sorted(messages, key=lambda item: item.name):
        lines.append(f"message {message.name} {{")
        if message.reserved_numbers:
            numbers = ", ".join(str(item) for item in sorted(message.reserved_numbers))
            lines.append(f"  reserved {numbers};")
        if message.reserved_names:
            names = ", ".join(f'"{item}"' for item in sorted(message.reserved_names))
            lines.append(f"  reserved {names};")
        for item in sorted(message.fields, key=lambda field: field.number):
            repeated = "repeated " if item.repeated else ""
            lines.append(
                f"  {repeated}{item.type_name} {item.name} = {item.number};"
            )
        lines.extend(["}", ""])
    return "\n".join(lines)


def _pascal(value: str) -> str:
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", value) if part]
    if not parts:
        raise ProtoGenerationError(f"cannot derive protobuf identifier from {value!r}")
    result = "".join(part[:1].upper() + part[1:] for part in parts)
    if result[0].isdigit():
        result = f"N{result}"
    return result
