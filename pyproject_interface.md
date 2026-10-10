# pyscripts Hook 接口定义规范

接口不再写在 `pyproject.toml` 中。业务函数通过轻量包
`pyscripts-hook` 的 `@hook` 装饰器声明，平台在发布阶段导入指定模块并生成
不可变 Endpoint Manifest、REST 路由和 Proto 契约。

Web UI 注册服务和修改仓库地址时，只接受 HTTP/HTTPS Git 地址，例如
`https://git.example.com/team/orders.git`。SSH、`git://`、`file://` 和本地路径
均不允许用于服务注册。

## `pyproject.toml`

`pyproject.toml` 只保留项目和发布级信息：

```toml
[project]
name = "order-pipeline"
version = "1.0.0"
requires-python = ">=3.12,<3.13"
dependencies = ["httpx>=0.28", "pyscripts-hook"]

[tool.pyscript]
spec_version = 2
runtime = "default"
module = "service"
```

多个接口模块使用：

```toml
[tool.pyscript]
spec_version = 2
runtime = "default"
modules = ["service.api", "service.jobs"]
```

`module` 和 `modules` 不能同时存在。模块必须可以在项目 ZIP 根目录下导入。
发布校验会在所选 Runtime Label 的临时 Ray Runtime Env 中导入这些模块，因此
业务依赖不需要安装在 API Server 中。
`pyscripts-hook` 由平台作为 Ray job-level module 提供，不要求 Runtime Label
重复安装；业务仓库仍可把它声明为开发/类型检查依赖。

旧的 `[[tool.pyscript.endpoints]]`、`para`、`return`、`task_type` 和
`entrypoint` TOML 格式不再接受。

## Endpoint

```python
from typing import TypedDict

from sc_hook import Int64, IOType, String, hook


class Item(TypedDict):
    sku: String
    quantity: Int64


@hook(io_types=[IOType.Inner], num_cpus=2)
def calculate_total(items: list[Item]) -> Int64:
    return sum(item["quantity"] for item in items)


@hook(io_types=[IOType.Rest, IOType.Grpc])
async def submit_order(customer: String, items: list[Item]) -> Int64:
    return await calculate_total.remote_call(items)
```

规则：

- `async def` 自动成为 IO endpoint；
- 普通 `def` 自动成为 Compute endpoint；
- 每个参数和返回值都必须有受支持的类型注解；
- endpoint ID 默认是函数名，可用 `endpoint_id="..."` 覆盖；
- `IOType.Rest` 和 `IOType.Grpc` 暴露公共接口；
- `IOType.Inner` 只允许服务当前 Revision 内部调用，不出现在公共 API 文档；
- `num_cpus`、`num_gpus` 仅适用于 Compute endpoint；
- 同一 Revision 内的 endpoint ID 必须唯一。

## 类型

Hook 包提供与 Proto 对齐的显式类型：

```text
Float, Double, Int32, Int64, Uint32, Uint64,
Sint32, Sint64, Fixed32, Fixed64, Sfixed32, Sfixed64,
Bool, String, Bytes, Date, Datetime, DatetimeTz, Int, Bigint
```

使用 `list[T]` 定义列表，使用 `TypedDict` 定义闭合结构。函数参数展平为
REST JSON 对象和 gRPC request message 字段；返回注解生成 response message。

## 本地执行与内部编排

直接调用保持普通 Python 语义：

```python
calculate_total(items)       # 同步本地调用
await submit_order(...)      # 异步本地调用
```

`remote_call()`只能在 Hook IO endpoint 执行期间使用：

```python
result = await calculate_total.remote_call(items)
```

- 非 Ray 环境：Compute 直接同步阻塞执行，IO 被 `await`；
- Ray IO Actor：调用固定在当前 service/revision；
- Compute 目标作为通用 Ray Task 执行并等待 ObjectRef；
- IO 目标在当前异步 Actor 中作为嵌套异步调用执行；
- Compute endpoint 不能使用 `remote_call()`；
- 内部调用深度上限为 16；取消父调用会取消正在等待的 Compute ObjectRef；
- 子任务 stdout/stderr 会合并到父调用的日志结果中。

## 发布流程

1. 读取 `pyproject.toml` 中的 Runtime Label、模块列表和依赖约束；
2. 解析并验证 Runtime Label；
3. 将不可变 ZIP 发布到对象存储；
4. 在目标 Runtime Env 的临时 Ray Task 中导入 Hook 模块；
5. 校验 endpoint ID、函数类型、注解和资源设置；
6. 生成不可变 Endpoint Manifest；
7. 为 gRPC endpoint 生成稳定字段编号的 Proto/Descriptor；
8. 生成 Revision、Artifact digest 并激活。

运行时只消费持久化 Manifest，不会在每次请求时重新发现接口。
