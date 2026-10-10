"""Strict validation for Hook contracts on the JSON transport.

Validation never coerces values and errors deliberately exclude input values:
request payloads may contain credentials or other private business data.
"""

from __future__ import annotations

import math
from typing import Any, Mapping


_SIGNED_32 = {"Int32", "Sint32", "Sfixed32", "Int"}
_SIGNED_64 = {"Int64", "Sint64", "Sfixed64", "Bigint"}
_UNSIGNED_32 = {"Uint32", "Fixed32"}
_UNSIGNED_64 = {"Uint64", "Fixed64"}


class ContractValidationError(ValueError):
    def __init__(self, location: tuple[str | int, ...], message: str):
        self.location = location
        self.message = message
        super().__init__(f"{'.'.join(map(str, location))}: {message}")

    def detail(self) -> list[dict[str, Any]]:
        return [{"type": "contract_validation", "loc": list(self.location), "msg": self.message}]


def validate_value(
    value: Any,
    schema: Mapping[str, Any],
    location: tuple[str | int, ...] = ("body",),
) -> None:
    """Validate JSON-native Hook types, including all nested fields and ranges."""
    kind = schema.get("type")

    def reject(message: str) -> None:
        raise ContractValidationError(location, message)

    if kind == "Struct":
        if not isinstance(value, dict):
            reject("expected an object")
        properties = schema.get("properties", {})
        for name in schema.get("required", list(properties)):
            if name not in value:
                raise ContractValidationError((*location, name), "field is required")
        for name, child in value.items():
            if name not in properties:
                if not schema.get("additionalProperties", False):
                    raise ContractValidationError((*location, name), "unknown field")
            else:
                validate_value(child, properties[name], (*location, name))
        return
    if kind == "List":
        if not isinstance(value, list):
            reject("expected an array")
        for index, child in enumerate(value):
            validate_value(child, schema["items"], (*location, index))
        return
    if kind in _SIGNED_32 | _SIGNED_64 | _UNSIGNED_32 | _UNSIGNED_64:
        if type(value) is not int:
            reject(f"expected {kind} integer")
        bits = 32 if kind in _SIGNED_32 | _UNSIGNED_32 else 64
        unsigned = kind in _UNSIGNED_32 | _UNSIGNED_64
        lower = 0 if unsigned else -(1 << (bits - 1))
        upper = (1 << (bits if unsigned else bits - 1)) - 1
        if not lower <= value <= upper:
            reject(f"integer outside {kind} range")
        return
    if kind in {"Float", "Double"}:
        if type(value) not in {int, float}:
            reject(f"expected {kind} number")
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite or (kind == "Float" and abs(value) > 3.4028234663852886e38):
            reject(f"number outside finite {kind} range")
        return
    if kind == "Bool":
        if type(value) is not bool:
            reject("expected a boolean")
        return
    if kind == "String":
        if not isinstance(value, str):
            reject("expected a string")
        return
    reject(f"type {kind} is not supported by the JSON transport; use gRPC")
