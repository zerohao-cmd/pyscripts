from datetime import date, datetime
from enum import StrEnum
from typing import Annotated, Any, get_args, get_origin, get_type_hints, is_typeddict

try:
    from pyarrow import Table as _ArrowTableType
except ImportError:
    _ArrowTableType = object


class BaseType(StrEnum):
    """
    pyscripts支持的数据类型
    """

    # 基础数值
    Float = "Float"
    Double = "Double"
    ArrowTable = "ArrowTable"
    Int32 = "Int32"
    Int64 = "Int64"
    Uint32 = "Uint32"
    Uint64 = "Uint64"
    Sint32 = "Sint32"
    Sint64 = "Sint64"
    Fixed32 = "Fixed32"
    Fixed64 = "Fixed64"
    Sfixed32 = "Sfixed32"
    Sfixed64 = "Sfixed64"
    Bool = "Bool"
    String = "String"
    Bytes = "Bytes"
    Date = "Date"
    Datetime = "Datetime"
    DatetimeTz = "DatetimeTz"
    Int = "Int"
    Bigint = "Bigint"

    def __repr__(self) -> str:
        return self.value


class List:
    def __init__(self, item_type: "DATA_TYPE"):
        self.item_type = item_type

    def __repr__(self):
        return f"List<{self.item_type}>"


type DATA_TYPE = dict[str, "DATA_TYPE"] | List | BaseType


def get_type(t) -> DATA_TYPE:
    if is_typeddict(t):
        fields = get_type_hints(t, include_extras=True)
        return {k: get_type(v) for k, v in fields.items()}

    else:
        origin = get_origin(t)
        if origin is Annotated:
            # 基础值类型
            if len(args := get_args(t)) != 2:
                raise ValueError("not accepted base type with length != 2")
            base_type: str = args[1]
            if not isinstance(base_type, BaseType):
                raise ValueError(f"not accepted base type : {base_type}")
            return base_type
        elif origin is list:
            # 嵌套列表
            inner_type = get_args(t)[0]
            return List(get_type(inner_type))
        else:
            raise ValueError(
                "not accepted type, please use typeddict, list, and basetype"
            )


def to_schema(value: DATA_TYPE) -> dict[str, Any]:
    """Convert Hook type metadata to the platform's immutable schema format."""

    if isinstance(value, BaseType):
        return {"type": value.value}
    if isinstance(value, List):
        return {"type": "List", "items": to_schema(value.item_type)}
    if isinstance(value, dict):
        properties = {name: to_schema(item) for name, item in value.items()}
        return {
            "type": "Struct",
            "required": list(properties),
            "additionalProperties": False,
            "properties": properties,
        }
    raise TypeError(f"unsupported Hook data type: {value!r}")


Float = Annotated[float, BaseType.Float]
Double = Annotated[float, BaseType.Double]
ArrowTable = Annotated[_ArrowTableType, BaseType.ArrowTable]
Int32 = Annotated[int, BaseType.Int32]
Int64 = Annotated[int, BaseType.Int64]
Uint32 = Annotated[int, BaseType.Uint32]
Uint64 = Annotated[int, BaseType.Uint64]
Sint32 = Annotated[int, BaseType.Sint32]
Sint64 = Annotated[int, BaseType.Sint64]
Fixed32 = Annotated[int, BaseType.Fixed32]
Fixed64 = Annotated[int, BaseType.Fixed64]
Sfixed32 = Annotated[int, BaseType.Sfixed32]
Sfixed64 = Annotated[int, BaseType.Sfixed64]
Bool = Annotated[bool, BaseType.Bool]
String = Annotated[str, BaseType.String]
Bytes = Annotated[bytes, BaseType.Bytes]
Date = Annotated[date, BaseType.Date]
Datetime = Annotated[datetime, BaseType.Datetime]
DatetimeTz = Annotated[datetime, BaseType.DatetimeTz]
Int = Annotated[int, BaseType.Int]
Bigint = Annotated[int, BaseType.Bigint]
