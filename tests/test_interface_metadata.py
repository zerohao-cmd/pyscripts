from __future__ import annotations

import pytest

from pyscripts.interface_metadata import (
    InterfaceMetadataError,
    parse_interface_document,
)


def document_with_interface(pyscript: dict) -> dict:
    return {
        "project": {
            "requires-python": ">=3.12,<3.13",
            "dependencies": [],
        },
        "tool": {
            "pyscript": {
                "runtime": {"label": "test"},
                **pyscript,
            }
        },
    }


def test_parses_flat_function_parameters() -> None:
    interface = parse_interface_document(
        document_with_interface(
            {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "id": "create_order",
                            "task_type": "io",
                            "entrypoint": "orders:create_order",
                            "x": {
                                "type": "Struct",
                                "required": ["customer_id", "items"],
                                "additionalProperties": False,
                            },
                            "y": {
                                "type": "Struct",
                                "required": ["customer_id", "items"],
                                "additionalProperties": False,
                            },
                            "response_schema": {"type": "String"},
                        }
                    ],
            }
        )
    )

    endpoint = interface.endpoints[0]
    assert interface.runtime_profile == "test"
    assert list(endpoint.parameters) == ["x", "y"]
    assert endpoint.parameters["x"]["required"] == ["customer_id", "items"]
    assert endpoint.request_schema == {
        "type": "Struct",
        "required": ["x", "y"],
        "additionalProperties": False,
        "properties": endpoint.parameters,
    }
    manifest = endpoint.to_manifest()
    assert manifest["x"]["type"] == "Struct"
    assert manifest["y"]["type"] == "Struct"
    assert "parameters" not in manifest
    assert "request_schema" not in manifest


def test_generated_grpc_uses_the_same_flat_function_parameters() -> None:
    interface = parse_interface_document(
        document_with_interface(
            {
                "spec_version": 1,
                "endpoints": [
                    {
                        "id": "add",
                        "task_type": "compute",
                        "entrypoint": "service:add",
                        "io_type": ["rest", "grpc"],
                        "x": {"type": "Int64"},
                        "y": {"type": "Int64"},
                        "response_schema": {"type": "Int64"},
                    }
                ],
            }
        )
    )

    endpoint = interface.endpoints[0]
    assert endpoint.io_type == ["rest", "grpc"]
    assert endpoint.grpc is None
    assert list(endpoint.parameters) == ["x", "y"]
    assert interface.grpc_contract is None


def test_generated_grpc_requires_a_response_schema() -> None:
    with pytest.raises(
        InterfaceMetadataError, match="generated gRPC endpoints require response_schema"
    ):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "id": "add",
                            "task_type": "compute",
                            "entrypoint": "service:add",
                            "io_type": ["grpc"],
                            "x": {"type": "Int64"},
                        }
                    ],
                }
            )
        )


def test_rejects_split_type_and_format_schema() -> None:
    with pytest.raises(InterfaceMetadataError, match="unsupported field.*format"):
        parse_interface_document(
            document_with_interface(
                {
                        "spec_version": 1,
                        "endpoints": [
                            {
                                "id": "add",
                                "task_type": "compute",
                                "entrypoint": "service:add",
                                "x": {
                                    "type": "Struct",
                                    "properties": {
                                        "value": {
                                            "type": "Int64",
                                            "format": "int64",
                                        }
                                    },
                                },
                            }
                        ],
                }
            )
        )


def test_rejects_wrapped_request_schema() -> None:
    with pytest.raises(InterfaceMetadataError, match="request_schema is not supported"):
        parse_interface_document(
            document_with_interface(
                {
                        "spec_version": 1,
                        "endpoints": [
                            {
                                "id": "add",
                                "task_type": "compute",
                                "entrypoint": "service:add",
                                "request_schema": {"type": "Struct"},
                            }
                        ],
                }
            )
        )


def test_native_grpc_does_not_accept_flat_http_parameters() -> None:
    with pytest.raises(InterfaceMetadataError, match="native gRPC endpoint"):
        parse_interface_document(
            document_with_interface(
                {
                        "spec_version": 1,
                        "grpc_contract": {"version": "1.0.0"},
                        "endpoints": [
                            {
                                "id": "add",
                                "task_type": "compute",
                                "entrypoint": "service:add",
                                "x": {"type": "Int64"},
                                "grpc": {
                                    "service": "examples.math.v1.MathService",
                                    "method": "Add",
                                },
                            }
                        ],
                }
            )
        )


def test_rejects_runtime_configuration_in_interface_metadata() -> None:
    with pytest.raises(InterfaceMetadataError, match="runtime_profile"):
        parse_interface_document(
            {
                "tool": {
                    "pyscript": {
                        "spec_version": 1,
                        "runtime_profile": "data-default",
                        "endpoints": [
                            {
                                "id": "run",
                                "task_type": "io",
                                "entrypoint": "service:run",
                            }
                        ],
                    }
                }
            }
        )
