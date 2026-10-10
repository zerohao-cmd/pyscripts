import math

import pytest

from pyscripts.contracts.validation import ContractValidationError, validate_value


@pytest.mark.parametrize("kind,bits,unsigned", [
    ("Int32", 32, False), ("Sint32", 32, False), ("Sfixed32", 32, False),
    ("Int", 32, False), ("Int64", 64, False), ("Sint64", 64, False),
    ("Sfixed64", 64, False), ("Bigint", 64, False),
    ("Uint32", 32, True), ("Fixed32", 32, True),
    ("Uint64", 64, True), ("Fixed64", 64, True),
])
def test_integer_boundaries(kind, bits, unsigned):
    lower = 0 if unsigned else -(1 << (bits - 1))
    upper = (1 << (bits if unsigned else bits - 1)) - 1
    for value in (lower, upper, 0):
        validate_value(value, {"type": kind})
    for value in (lower - 1, upper + 1, True, 1.0, "1", None):
        with pytest.raises(ContractValidationError):
            validate_value(value, {"type": kind})


@pytest.mark.parametrize("kind,bad", [
    ("Float", math.inf), ("Double", math.nan), ("Float", 3.5e38),
    ("Double", 10**400), ("Float", True), ("Double", "1.0"),
    ("Bool", 1), ("String", None), ("List", {}), ("Struct", []),
])
def test_strict_json_types(kind, bad):
    with pytest.raises(ContractValidationError):
        validate_value(bad, {"type": kind})


def test_nested_fields_and_errors_exclude_values():
    schema = {"type": "Struct", "properties": {
        "rows": {"type": "List", "items": {"type": "Struct", "properties": {
            "secret": {"type": "Int32"},
        }}},
    }}
    validate_value({"rows": []}, schema)
    validate_value({"rows": [{"secret": 42}]}, schema)
    for payload, location in [
        ({}, ("body", "rows")),
        ({"rows": [], "extra": 1}, ("body", "extra")),
        ({"rows": [{}]}, ("body", "rows", 0, "secret")),
        ({"rows": [{"secret": "private-password"}]}, ("body", "rows", 0, "secret")),
    ]:
        with pytest.raises(ContractValidationError) as caught:
            validate_value(payload, schema)
        assert caught.value.location == location
        assert "private-password" not in str(caught.value.detail())


def test_nested_lists_and_valid_scalars():
    validate_value([[1], []], {"type": "List", "items": {"type": "List", "items": {"type": "Int32"}}})
    for kind, value in [("Float", 1), ("Double", 1.5), ("Bool", False), ("String", "你好")]:
        validate_value(value, {"type": kind})
