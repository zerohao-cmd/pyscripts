# `pyproject.toml` 中的 pyscript 接口定义规范

本文定义 `tool.pyscript` version 1 的业务接口格式。业务仓库只需要声明运行环境、
Python 函数入口、参数、返回值和允许的调用协议；平台负责生成 Revision、业务
Artifact、gRPC Proto、Descriptor 和稳定字段编号。

本规范是一次不兼容升级，不接受旧格式。

## 核心语义

- `para` 描述函数参数，并在调用时展开为关键字参数；
- `return` 描述一个返回值。表类型返回值对应 Python `dict`；
- 字符串表示标量类型；
- 普通 TOML 表自动推断为封闭 `Struct`；
- 只包含 `_item` 的表自动推断为 `List`；
- 每个 endpoint 都必须声明非空的 `para` 和 `return`；
- endpoint 的 `id` 省略时，默认使用 `entrypoint` 中的函数名；
- `io_type` 省略时，默认只开放 `rest`。

例如：

```toml
para = { a = "Int64", b = "Int64" }
return = "Int64"
```

对应：

```python
def func(a: int, b: int) -> int:
    ...
```

而：

```toml
[tool.pyscript.endpoints.return]
a = "Int64"
b = "Int64"
```

对应：

```python
def func(...) -> dict[str, int]:
    return {"a": 1, "b": 2}
```

## 完整示例

```toml
[project]
name = "example-service"
version = "1.0.0"
requires-python = ">=3.12,<3.13"
dependencies = []

[tool.pyscript]
spec_version = 1
runtime = "test"

[[tool.pyscript.endpoints]]
id = "test_1"
task_type = "io"
entrypoint = "main:test_sync_io"
io_type = ["rest", "grpc"]
return = "Int64"

[tool.pyscript.endpoints.para]
a = "Int64"
b = "Int64"

[[tool.pyscript.endpoints]]
# id 省略，自动使用函数名 test_2
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
```

对应的第二个函数可以写成：

```python
async def test_2(
    a: int,
    b: dict[str, int],
    c: list[int],
    d: list[dict],
) -> dict[str, int]:
    return {"a": a, "b": len(c) + len(d)}
```

## `[tool.pyscript]`

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `spec_version` | 是 | 当前必须为整数 `1`。 |
| `runtime` | 是 | 逻辑运行环境标签，例如 `test`。 |
| `endpoints` | 是 | 至少包含一个 `[[tool.pyscript.endpoints]]`。 |

`runtime = "test"` 默认跟随 `test` 标签当前活动版本。系统也接受
`test@latest` 和显式固定版本 `test@v2`。平台会在发布前校验
`project.requires-python`、`project.dependencies` 与该环境是否兼容。

旧写法不再支持：

```toml
# 无效
[tool.pyscript.runtime]
label = "test"
```

## Endpoint 字段

| 字段 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `id` | 否 | `entrypoint` 的函数名 | REST 路径标识和接口标识。 |
| `task_type` | 是 | 无 | `io` 或 `compute`。 |
| `entrypoint` | 是 | 无 | `package.module:function`。 |
| `io_type` | 否 | `["rest"]` | 可包含 `rest`、`grpc`。 |
| `para` | 是 | 无 | 至少包含一个参数的展平函数参数表。 |
| `return` | 是 | 无 | 非空返回类型或返回对象。 |

`task_type = "io"` 的函数可以是同步或异步函数，并由异步 Actor 调度。
`task_type = "compute"` 的函数会包装为独立 Ray Task 调度。

`context` 是平台保留参数名，不能出现在 `para` 中。需要调用上下文时，函数可以
显式把 `context` 声明为第一个参数；它不属于公开接口：

```python
def calculate(context, x: int, y: int) -> int:
    return x + y
```

## `para`：展平函数参数

多参数单行写法：

```toml
para = { a = "Int64", b = "Int64" }
```

多参数表写法：

```toml
[tool.pyscript.endpoints.para]
a = "Int64"
b = "Int64"
```

两种写法都调用：

```python
func(a=..., b=...)
```

REST 请求体为：

```json
{
  "a": 1,
  "b": 2
}
```

`para` 不能省略、不能为空，也不能直接写成单个类型。顶层必须是参数名到类型的映射。
没有业务输入的定时任务、健康检查和系统控制操作应使用平台内部任务，不作为公开
Pipeline；公开的副作用操作应至少接收 `request_id`、`trigger` 或业务命令对象。

## `return`：返回值

标量返回值：

```toml
return = "String"
```

对应 `return "value"`。

对象返回值：

```toml
[tool.pyscript.endpoints.return]
order_id = "String"
quantity = "Int64"
```

对应：

```python
return {"order_id": "order-1", "quantity": 3}
```

`return` 不能省略，也不能是空表。纯副作用操作也必须返回明确结果，例如
`accepted`、`request_id` 或状态对象，以便审计、重试和幂等处理。

## 类型推断

### 标量

| pyscript 类型 | Python 值 | Proto 类型 |
| --- | --- | --- |
| `Float` | `float` | `float` |
| `Double` | `float` | `double` |
| `Int32`、`Int` | `int` | `int32` |
| `Int64`、`Bigint` | `int` | `int64` |
| `Uint32`、`Uint64` | `int` | 对应无符号整数 |
| `Sint32`、`Sint64` | `int` | 对应 zigzag 整数 |
| `Fixed32`、`Fixed64` | `int` | 对应 fixed 整数 |
| `Sfixed32`、`Sfixed64` | `int` | 对应 signed fixed 整数 |
| `Bool` | `bool` | `bool` |
| `String` | `str` | `string` |
| `Bytes` | `bytes` | `bytes` |
| `Date` | `datetime.date` | 平台日期消息 |
| `Datetime` | `datetime.datetime` | 毫秒时间戳 |
| `DatetimeTz` | 带时区的 `datetime.datetime` | 平台时区日期消息 |

`Struct` 和 `List` 不作为字符串填写，而是由表结构推断。

### Struct

任何不包含 `_item` 的普通表都会被推断为封闭 Struct，所有声明字段都是必填字段：

```toml
[tool.pyscript.endpoints.para.order]
customer_id = "String"
quantity = "Int64"
```

对应一个名为 `order` 的参数：

```python
def func(order: dict) -> ...:
    ...
```

### List

只包含 `_item` 的表会被推断为 List：

```toml
[tool.pyscript.endpoints.para.ids]
_item = "Int64"
```

对应 `ids: list[int]`。

List 中嵌套 Struct：

```toml
[tool.pyscript.endpoints.para.items._item]
sku = "String"
quantity = "Int64"
```

对应 `items: list[dict]`。

`_item` 是保留键。同一张表出现 `_item` 后不能再定义其他字段。

## REST 与 gRPC

```toml
io_type = ["rest", "grpc"]
```

表示同一 Python 函数同时开放 REST 和 gRPC：

- REST 固定路径为 `POST /v1/services/{service}/{endpoint_id}`；
- REST JSON 对象的键直接对应 `para` 中的参数名；
- gRPC Request 字段直接对应这些展平参数；
- Struct 自动生成嵌套 message；
- List 自动生成 repeated 字段；
- 标量 `return` 使用 Response 的 `result` 字段；
- Struct `return` 的字段直接成为 Response 字段；
- Proto、Descriptor、字段编号和契约版本全部由平台生成。

当前 gRPC 支持 unary-unary。为避免 protobuf 无法直接表达，gRPC 接口暂不允许
直接的 `List[List[...]]`；可以用 Struct 包装其中一层。

接口未变化时生成相同 Descriptor 并复用契约。删除字段、修改字段类型或删除 RPC
会保留旧字段编号并提升 major；兼容新增提升 minor。Proto 源码包持久化到对象存储：

```text
GET /v1/services/{service_name}/grpc-contract
GET /v1/contracts/{contract_id}/proto.zip
```

客户端从 Proto 使用自己的语言工具链生成标准 Message 和 Stub。

## 不再支持的旧格式

以下字段和结构会直接导致发布校验失败：

- `[tool.pyscript.runtime]` 和其中的 `label`；
- endpoint 下以参数名直接建立的旧子表；
- `request_schema`、`response_schema`；
- 显式的 `type = "Struct"`、`properties`、`items`；
- `"NULL"`、空 `para` 或空 `return`；
- `grpc_contract` 和 endpoint 的手写 `grpc`；
- 用户手工填写的 Proto、Descriptor 或契约版本。

## 发布流程

1. 用户提交业务代码与 `pyproject.toml`；
2. manager 读取并校验本规范；
3. 解析 `runtime`，校验 Python 与依赖版本；
4. 将 `para`/`return` 转换为内部不可变接口 manifest；
5. 为 gRPC endpoint 生成 Proto 和 Descriptor；
6. 生成 Git-SHA Revision 和业务 Artifact digest；
7. 把业务 Artifact 与 Proto 契约写入对象存储；
8. 原子激活新 Revision，已有请求继续完成旧 Revision。

## 检查清单

- `[tool.pyscript].spec_version = 1`；
- `runtime` 是字符串，并指向可用环境标签；
- 每个 endpoint 都显式填写 `task_type`、`entrypoint` 和 `return`；
- `id` 省略时，函数名适合作为公开接口 ID；
- `para` 顶层是参数映射，不是单个类型；
- 普通表表示 Struct，只含 `_item` 的表表示 List；
- `para` 和 `return` 均已声明且非空；
- `context` 未出现在 `para`；
- gRPC 接口没有直接嵌套 List。
