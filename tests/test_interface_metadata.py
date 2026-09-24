from __future__ import annotations

import tomllib

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
        "tool": {"pyscript": {"runtime": "test", **pyscript}},
    }


def test_documented_nested_toml_is_valid() -> None:
    document = tomllib.loads(
        """[project]
name = "nested"
version = "1.0.0"
requires-python = ">=3.12,<3.13"

[tool.pyscript]
spec_version = 1
runtime = "test"

[[tool.pyscript.endpoints]]
task_type = "io"
entrypoint = "main:test_2"

[tool.pyscript.endpoints.return]
a = "Int64"
b = "Int64"

[tool.pyscript.endpoints.para]
a = "Int64"

[tool.pyscript.endpoints.para.b]
a = "Int64"
b = "Int64"

[tool.pyscript.endpoints.para.c]
_item = "Int64"

[tool.pyscript.endpoints.para.d._item]
a = "Int64"
b = "Int64"
c = "String"

[tool.pyscript.endpoints.para.d._item.d]
_item = "String"
"""
    )

    endpoint = parse_interface_document(document).endpoints[0]

    assert endpoint.id == "test_2"
    assert endpoint.parameters["d"]["type"] == "List"
    assert endpoint.parameters["d"]["items"]["type"] == "Struct"


def test_parses_compact_nested_interface_and_flattens_para() -> None:
    interface = parse_interface_document(
        document_with_interface(
            {
                "spec_version": 1,
                "endpoints": [
                    {
                        "id": "test_1",
                        "task_type": "io",
                        "entrypoint": "main:test_sync_io",
                        "io_type": ["rest", "grpc"],
                        "return": "Int64",
                        "para": {"a": "Int64", "b": "Int64"},
                    },
                    {
                        "task_type": "io",
                        "entrypoint": "main:test_2",
                        "return": {"a": "Int64", "b": "Int64"},
                        "para": {
                            "a": "Int64",
                            "b": {"a": "Int64", "b": "Int64"},
                            "c": {"_item": "Int64"},
                            "d": {
                                "_item": {
                                    "a": "Int64",
                                    "b": "Int64",
                                    "c": "String",
                                    "d": {"_item": "String"},
                                }
                            },
                        },
                    },
                ],
            }
        )
    )

    first, second = interface.endpoints
    assert interface.runtime_profile == "test"
    assert first.id == "test_1"
    assert first.io_type == ["rest", "grpc"]
    assert first.parameters == {
        "a": {"type": "Int64"},
        "b": {"type": "Int64"},
    }
    assert first.response_schema == {"type": "Int64"}

    assert second.id == "test_2"
    assert second.io_type == ["rest"]
    assert second.parameters["b"] == {
        "type": "Struct",
        "required": ["a", "b"],
        "additionalProperties": False,
        "properties": {
            "a": {"type": "Int64"},
            "b": {"type": "Int64"},
        },
    }
    assert second.parameters["c"] == {
        "type": "List",
        "items": {"type": "Int64"},
    }
    assert second.parameters["d"]["items"]["properties"]["d"] == {
        "type": "List",
        "items": {"type": "String"},
    }
    assert second.response_schema == {
        "type": "Struct",
        "required": ["a", "b"],
        "additionalProperties": False,
        "properties": {
            "a": {"type": "Int64"},
            "b": {"type": "Int64"},
        },
    }
    assert second.request_schema["required"] == ["a", "b", "c", "d"]


def test_id_defaults_to_entrypoint_function() -> None:
    interface = parse_interface_document(
        document_with_interface(
            {
                "spec_version": 1,
                "endpoints": [
                    {
                        "task_type": "compute",
                        "entrypoint": "service:refresh_cache",
                        "para": {"force": "Bool"},
                        "return": "Bool",
                    }
                ],
            }
        )
    )

    endpoint = interface.endpoints[0]
    assert endpoint.id == "refresh_cache"
    assert endpoint.io_type == ["rest"]
    assert endpoint.parameters == {"force": {"type": "Bool"}}
    assert endpoint.response_schema == {"type": "Bool"}


def test_rejects_previous_runtime_and_endpoint_schema() -> None:
    with pytest.raises(InterfaceMetadataError, match="runtime must be a string"):
        parse_interface_document(
            {
                "project": {"requires-python": ">=3.12"},
                "tool": {
                    "pyscript": {
                        "spec_version": 1,
                        "runtime": {"label": "test"},
                        "endpoints": [],
                    }
                },
            }
        )

    with pytest.raises(InterfaceMetadataError, match="unsupported field.*x"):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "task_type": "io",
                            "entrypoint": "service:run",
                            "x": {"type": "Int64"},
                            "return": "Int64",
                        }
                    ],
                }
            )
        )


def test_rejects_missing_para_or_return() -> None:
    with pytest.raises(InterfaceMetadataError, match="para is required"):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "task_type": "io",
                            "entrypoint": "service:run",
                            "return": "Int64",
                        }
                    ],
                }
            )
        )

    with pytest.raises(InterfaceMetadataError, match="return is required"):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "task_type": "io",
                            "entrypoint": "service:run",
                            "para": {"value": "Int64"},
                        }
                    ],
                }
            )
        )

def test_rejects_ambiguous_list() -> None:
    with pytest.raises(InterfaceMetadataError, match="reserved _item"):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "task_type": "io",
                            "entrypoint": "service:run",
                            "para": {
                                "values": {"_item": "Int64", "other": "String"}
                            },
                            "return": "Int64",
                        }
                    ],
                }
            )
        )


def test_rejects_null_and_empty_interfaces() -> None:
    with pytest.raises(InterfaceMetadataError, match="supported scalar"):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "task_type": "io",
                            "entrypoint": "service:run",
                            "para": {"value": "Int64"},
                            "return": "NULL",
                        }
                    ],
                }
            )
        )

    with pytest.raises(InterfaceMetadataError, match="para must not be empty"):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "task_type": "io",
                            "entrypoint": "service:run",
                            "para": {},
                            "return": "Int64",
                        }
                    ],
                }
            )
        )

    with pytest.raises(InterfaceMetadataError, match="return must not be an empty"):
        parse_interface_document(
            document_with_interface(
                {
                    "spec_version": 1,
                    "endpoints": [
                        {
                            "task_type": "io",
                            "entrypoint": "service:run",
                            "para": {"value": "Int64"},
                            "return": {},
                        }
                    ],
                }
            )
        )
