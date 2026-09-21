# 脚本接入 pyscripts 服务指南

本文定义 pyscripts 业务 Artifact 的接口元信息格式。接口配置位于
`pyproject.toml` 的 `[tool.pyscript]` 命名空间中，用于描述入口函数、
请求与返回结构、执行类型以及允许的调用协议。gRPC 的 Proto 和内部 Descriptor
由平台生成，不需要业务仓库维护；平台只发布 Proto 源码包，不发布语言 SDK。

manager 检测到 Git 提交变化后，直接读取该提交的 `pyproject.toml`。只有环境与
接口校验全部通过，才会从相同提交自动生成 Revision 和 Artifact，并将本文结构
转换成 Revision。Git commit、Artifact 和接口 manifest 保持不可变；逻辑标签对应的
具体环境绑定可以在兼容性校验通过后热切换。

## 配置边界

一次 Git 提交必须完整描述该 Revision 需要的接口和运行环境约束：

- `[tool.pyscript.runtime].label` 指定逻辑环境标签，例如 `data-default`；裸标签等价于
  `data-default@latest`，默认跟随该标签的活动版本；
- `[project].requires-python` 声明脚本支持的 Python 版本范围；
- `[project].dependencies` 声明脚本需要的外部 Python 包版本；
- `[[tool.pyscript.endpoints]]` 声明接口、参数和执行类型。

Runtime Label 的实际内容仍由平台管理，包括 Python 版本、已经解析安装的依赖、
Runtime Env 和 K8s/Ray Worker Pool。`pyproject.toml` 只引用逻辑标签并声明兼容性
约束，不允许携带密钥、Token 或部署环境变量。

平台不会根据 `[project].dependencies` 临时安装包。它只验证所引用 Runtime
Label 中已安装的版本是否满足项目约束；不兼容时本次 Git 更新失败。

## 接入流程概览

一个脚本成为可调用服务需要经过以下步骤：

1. 编写业务函数；
2. 在 `pyproject.toml` 中声明 endpoint、展平参数和返回类型；
3. 将代码提交并推送到 Git 仓库；
4. 在 WebUI 注册服务，填写 Git 地址与跟踪方式；
5. manager 检测 Git 提交变化并读取 `pyproject.toml`；
6. 校验环境标签、Python 版本和外部依赖兼容性；
7. 校验成功后自动生成 Revision、Artifact URI 和 SHA-256 digest；
8. 激活 Revision，通过固定 HTTP 路径或由发布 Proto 生成的强类型 gRPC 客户端调用。

用户不需要手工输入 Revision、Artifact URI 或 digest。Revision 默认使用完整 Git
commit SHA；Artifact 由 manager 从同一提交构建并上传到内容寻址对象存储。

## 项目目录

最小 HTTP 服务可以采用以下结构：

```text
calculator-service/
├── pyproject.toml
└── service.py
```

`pyproject.toml` 必须位于 Git 仓库根目录。manager 自动构建 Artifact 时会保持它
位于 ZIP 根目录，即 `artifact.zip!/pyproject.toml`，不会额外包裹项目目录。

## 最小配置

```toml
[project]
name = "calculator-service"
version = "1.0.0"
requires-python = ">=3.12,<3.13"
dependencies = [
    "numpy>=2.1,<3",
]

[tool.pyscript]
spec_version = 1

[tool.pyscript.runtime]
label = "data-default"

[[tool.pyscript.endpoints]]
id = "add"
task_type = "compute"
entrypoint = "service:add"

[tool.pyscript.endpoints.x]
type = "Int64"

[tool.pyscript.endpoints.y]
type = "Int64"

[tool.pyscript.endpoints.response_schema]
type = "Int64"
```

对应的 Python 入口：

```python
def add(x: int, y: int) -> int:
    return x + y
```

HTTP 调用地址固定为：

```text
POST /v1/services/{service_name}/{endpoint_id}
```

HTTP 请求体由这些展平参数自动组成一个 JSON 对象：

```json
{"x": 1, "y": 2}
```

对象中的键会作为关键字参数传给入口函数，因此公开的业务参数就是 `(x, y)`。
`context` 不属于接口参数；为兼容已有服务，只有入口函数显式将第一个参数命名
为 `context` 时，平台才会额外注入调用上下文。

## 注册、同步与自动发布

### 1. 注册服务

注册时只配置代码来源和跟踪策略，不选择运行环境，也不上传 Artifact：

```http
POST /admin/services
Content-Type: application/json

{
  "name": "calculator-service",
  "git_url": "https://git.example.com/team/calculator-service.git",
  "tracking_mode": "poll",
  "check_interval_seconds": 60
}
```

- `manual`：用户在 WebUI 点击同步后检查 Git；
- `poll`：按照 `check_interval_seconds` 定时检查默认分支；
- `webhook`：收到已验证的 Git Webhook 后检查对应提交。

私有仓库凭据属于平台侧 Secret，不写入 Git URL 或 `pyproject.toml`。

服务创建后，WebUI 使用以下管理接口：

- `GET /admin/services/{service_id}`：返回服务属性、活动 Revision、运行环境和
  Endpoint 列表；
- `PATCH /admin/services/{service_id}`：修改 `git_url`、`tracking_mode` 和
  `check_interval_seconds`。服务名构成公开路径与 Proto 契约身份，创建后不允许修改。

切换到 `poll` 时必须同时提供至少 10 秒的检查间隔；切换到 `manual` 或 `webhook`
时平台清除轮询间隔。属性修改不会直接改变当前活动 Revision。

Webhook 模式下，在 WebUI 的服务设置中生成回调地址，或调用：

```text
POST   /admin/services/{service_id}/webhook/rotate
GET    /admin/services/{service_id}/webhook
DELETE /admin/services/{service_id}/webhook
```

`rotate` 返回可直接粘贴到 Gitea/GitLab 的完整 URL，且明文令牌只返回一次；`GET`
只显示是否已启用。Gitea 选择 Push 事件，GitLab 选择 Push events。公网或反向代理
部署应设置 `PYSCRIPTS_PUBLIC_BASE_URL`，以免生成内部地址。

### 2. 检测 Git 更新

manager 获取远端目标提交后，先读取该提交中的 `pyproject.toml`，但此时不创建
Revision，也不上传 Artifact。相同 commit SHA 已经处理过时必须保持幂等，不重复
构建。

### 3. 运行环境兼容性校验

正式构建前必须按顺序完成：

1. `[tool.pyscript.runtime].label` 必须是已经存在的逻辑标签；`data-default` 与
   `data-default@latest` 都解析到当前活动版本；
2. 当前活动版本必须已经通过环境校验并处于 `ACTIVE` 状态；
3. 标签的 Python 版本必须满足 `[project].requires-python`；
4. 对 `[project].dependencies` 中的每个 PEP 508 Requirement，标签环境中的已解析
   包版本必须满足 Specifier 和适用的 marker；
5. 接口元数据、入口格式和 gRPC 可生成性必须有效。

任一步失败时，将该 Git 提交记录为构建失败并展示具体原因；不得生成 Revision、
不得上传 Artifact，也不得影响当前活动 Revision。

### 4. 自动生成 Revision 与 Artifact

兼容性校验全部通过后，manager 才执行：

1. 使用完整 Git commit SHA 作为不可变 Revision 标识；
2. 从该提交的受版本控制文件生成 ZIP Artifact，不包含 `.git`、本地虚拟环境、
   缓存或未提交文件；
3. 计算最终 ZIP 字节内容的 SHA-256；
4. 按 digest 上传到内容寻址对象存储并得到稳定 Artifact URI；
5. 写入 Revision、不可变接口 manifest，以及当前解析到的 Runtime Label 版本与
   environment digest；
6. gRPC schema 变化时生成 Proto 源码包，并按 digest 持久化到对象存储；
7. 将构建成功的 Revision 置为 `READY`；
8. 服务处于启用状态时原子激活该 Revision；服务已停止时保留为 `READY`，等待
   用户重新启用。

这三个值都由同一个 Git 提交派生，用户不能覆盖：

```text
revision       = <full-git-commit-sha>
artifact_uri   = s3://<bucket>/<prefix>/sha256/<digest>.zip
artifact_digest = <sha256-of-final-zip>
```

### 5. 上线与路由切换

跟踪任务成功完成时自动切换活动 Revision，不要求用户填写或确认 Revision、
Artifact URI、digest。新请求立即路由到新 Revision；旧 Revision 已经开始的请求
会继续完成，随后进入排空状态。构建或兼容性校验失败时不发生路由切换。

### 6. 调用 HTTP Endpoint

```http
POST /v1/services/calculator-service/add
Content-Type: application/json

{
  "x": 2,
  "y": 3
}
```

响应由控制面包装，业务函数返回值位于 `result`：

```json
{
  "request_id": "...",
  "service": "calculator-service",
  "revision": "git-sha-or-version",
  "result": 5
}
```

## 顶层字段

### `[tool.pyscript]`

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `spec_version` | integer | 是 | 本文格式版本，当前固定为 `1`。 |
| `runtime` | table | 是 | 当前 Git 提交引用的逻辑 Runtime Label。 |
| `endpoints` | array of tables | 是 | 使用 `[[tool.pyscript.endpoints]]` 声明，至少一个。 |
| `grpc_contract` | table | 否 | 仅供旧版手写 Proto 兼容模式使用；新服务不要声明。 |

服务名称、Git 地址和跟踪方式属于 WebUI 注册信息，不在此处重复定义。

### `[tool.pyscript.runtime]`

```toml
[tool.pyscript.runtime]
label = "data-default"
```

推荐只写 `<label>`；它等价于 `<label>@latest`。发布 Revision 时平台会解析并记录
当时具体的 `<label>@v<version>`。以后激活该标签的新版本时，平台先使用每个
Revision 的 `requires-python` 和 `dependencies` 重新校验；全部兼容才把新请求热切换
到新环境，旧 Actor 和正在执行的 Task 排空后释放。高级回滚场景仍可显式写
`<label>@v<version>`，此时该 Revision 固定版本，不跟随后续升级。

## Endpoint 字段

每个接口使用一个 `[[tool.pyscript.endpoints]]` 表。

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `id` | string | 是 | Revision 内唯一的接口 ID，同时构成 HTTP 路径。 |
| `task_type` | string | 是 | 只能是 `io` 或 `compute`。 |
| `entrypoint` | string | 是 | 格式为 `模块路径:函数名`，相对于 Artifact 根目录。 |
| `io_type` | array of string | 否 | 可包含 `rest`、`grpc`；默认 `["rest"]`。 |
| `response_schema` | table | 否 | HTTP 返回值结构；省略时为空对象。 |
| `num_cpus` | float | 否 | 单个 Compute Task 申请的 CPU，只允许用于 `compute`。 |
| `num_gpus` | float | 否 | 单个 Compute Task 申请的 GPU，只允许用于 `compute`。 |
| `grpc` | table | 否 | 仅供旧版手写 Proto 兼容模式使用。 |

除上述保留字段外，endpoint 下的直接子表就是同名 Python 函数参数。例如
`[tool.pyscript.endpoints.x]` 对应 `function(..., x=...)`。参数名必须是合法的
Python 参数名，`context` 由平台保留。

推荐 `id` 使用小写字母、数字、下划线和连字符，并在发布后保持稳定。
例如 `get_order`、`daily-forecast`。

### `task_type = "io"`

用于网络、数据库、对象存储等以等待为主的任务。多个 IO 请求可以在同一
异步 Actor 中并发执行。入口推荐使用 `async def`：

```toml
[[tool.pyscript.endpoints]]
id = "load_order"
task_type = "io"
entrypoint = "orders:load_order"
```

```python
async def load_order(order_id: str) -> dict:
    ...
```

同步 IO 函数也可以执行，但会占用 Actor 的线程池，不适合大量长时间阻塞
调用。

### `task_type = "compute"`

用于 CPU/GPU 密集任务。每次调用作为独立 Ray Task 调度，不进入 IO Actor。
资源字段只对 Compute 接口有效：

```toml
[[tool.pyscript.endpoints]]
id = "forecast"
task_type = "compute"
entrypoint = "forecast:run"
num_cpus = 2.0
num_gpus = 0.25
```

未填写资源字段时使用控制面的默认 Compute Task 配置。

## 展平参数与响应 Schema

请求参数直接定义在 endpoint 下，不使用 `request_schema` 或额外的根
`Struct`。每个参数节点只用一个 `type` 字段表示完整类型，不再把基础类型和
`format` 分开。HTTP 传输层会把全部参数包装成一个 JSON 对象，但执行层会将
它重新展开成关键字参数；`response_schema` 可以使用任意允许的平台类型。

`type` 必须使用 [`data_design.md`](./data_design.md) 中定义的平台类型：

- 标量：`Float`、`Double`、`Int32`、`Int64`、`Uint32`、`Uint64`、
  `Sint32`、`Sint64`、`Fixed32`、`Fixed64`、`Sfixed32`、`Sfixed64`、
  `Bool`、`String`、`Bytes`；
- 包装类型：`Date`、`Datetime`、`DatetimeTz`；
- 嵌套类型：`Struct`、`List`；
- 简写别名：`Int`、`Bigint`。

嵌套结构继续使用 JSON Schema 风格的子表：

- `properties`：`Struct` 的字段定义；
- `items`：`List` 的元素定义；
- `required`：必填字段名称数组；
- `additionalProperties`：是否允许未声明字段；
- `description`：面向 SDK 和文档的说明；
- `nullable`：是否允许 `null`。

例如，`Int64` 直接写成 `type = "Int64"`，不再拆分成基础类型与格式两个字段。

嵌套对象示例：

```toml
[[tool.pyscript.endpoints]]
id = "create_order"
task_type = "io"
entrypoint = "orders:create_order"

[tool.pyscript.endpoints.x]
type = "Struct"
required = ["customer_id", "items"]
additionalProperties = false

[tool.pyscript.endpoints.x.properties.customer_id]
type = "String"

[tool.pyscript.endpoints.x.properties.items]
type = "List"

[tool.pyscript.endpoints.x.properties.items.items]
type = "Struct"
required = ["sku", "quantity"]

[tool.pyscript.endpoints.x.properties.items.items.properties.sku]
type = "String"

[tool.pyscript.endpoints.x.properties.items.items.properties.quantity]
type = "Int32"

[tool.pyscript.endpoints.y]
type = "Struct"
required = ["customer_id", "items"]
additionalProperties = false

[tool.pyscript.endpoints.response_schema]
type = "Struct"
required = ["order_id"]

[tool.pyscript.endpoints.response_schema.properties.order_id]
type = "String"
```

平台类型与 JSON、Python、Protobuf 的详细映射见
[`data_design.md`](./data_design.md)。`ArrowTable` 是内部 Python 数据类型，
不能直接作为 HTTP 或 gRPC 的外部接口字段。

展平参数和 `response_schema` 会进入 Revision manifest。HTTP 执行路径当前负责
对象解包，但尚未依据参数 Schema 自动完成强制校验；入口函数仍应对业务约束
进行校验。

## gRPC 接口

在 endpoint 的 `io_type` 中加入 `grpc` 即可。系统使用同一份展平参数与
`response_schema` 自动生成 Proto，不要求用户编写 `.proto`、Descriptor 或版本号。

```toml
[[tool.pyscript.endpoints]]
id = "create_order"
task_type = "io"
entrypoint = "orders:create_order"
io_type = ["rest", "grpc"]

[tool.pyscript.endpoints.order]
type = "Struct"
required = ["customer_id", "items"]
additionalProperties = false

[tool.pyscript.endpoints.order.properties.customer_id]
type = "String"

[tool.pyscript.endpoints.order.properties.items]
type = "List"

[tool.pyscript.endpoints.order.properties.items.items]
type = "String"

[tool.pyscript.endpoints.response_schema]
type = "Struct"
required = ["order_id"]
additionalProperties = false

[tool.pyscript.endpoints.response_schema.properties.order_id]
type = "String"
```

REST 请求会使用 `{"order": ...}` 作为 JSON body；由 Proto 生成的 gRPC 客户端提供展开后的
`CreateOrderRequest.order` 字段。两条协议最终都调用同一个 Python 签名：

```python
async def create_order(order: dict) -> dict:
    return {"order_id": f"order-{order['customer_id']}"}
```

自动生成规则：

- 首次发布按字段名确定稳定编号；后续发布从上一份 Descriptor 继承编号；
- 参数顺序变化不会改变 Proto；字段删除或类型变化会保留旧编号和字段名为
  `reserved`，不会被后续字段复用；
- 接口不变时 Descriptor 字节保持一致，复用原有契约与 Proto 源码包；
- 破坏性变化自动提升 major，兼容变化自动提升 minor，用户不填写契约版本；
- 标量返回值在生成的 Response 中使用 `result` 字段；`Struct` 返回值保持其字段；
- 当前只支持 unary-unary；用于 gRPC 的 `Struct` 必须设置
  `additionalProperties = false`，且暂不支持嵌套 `List[List[...]]`。

Revision 激活后可以获取契约元数据与 Proto 源码包：

```text
GET /v1/services/{service_name}/grpc-contract
GET /v1/contracts/{contract_id}/proto.zip
```

客户端使用 `protoc`、`grpc_tools.protoc`、Buf 或目标语言的标准生成器从 Proto
生成 protobuf Message 和 Stub，不需要调用通用 `Invoke(bytes)` 接口，也不需要
手动序列化请求或响应。

已有的手写 Proto 服务仍可通过 `grpc_contract` 和 endpoint `grpc` 字段运行，
但它属于兼容模式，不能与自动生成模式混用，新服务应只使用 `io_type`。

## Artifact 目录示例

```text
orders-service.zip
├── pyproject.toml
└── orders.py
```

`entrypoint = "orders:create_order"` 对应 Artifact 根目录中的 `orders.py`。
包内模块可以使用 `package.module:function`，例如
`entrypoint = "src.orders:create_order"`。平台发布时会把生成的 Proto 和 Descriptor
加入最终的不可变 Artifact；Git 仓库不需要包含它们。

## 完整示例

```toml
[project]
name = "orders-service"
version = "2.1.0"
requires-python = ">=3.12,<3.13"
dependencies = [
    "pydantic>=2.12,<3",
]

[tool.pyscript]
spec_version = 1

[tool.pyscript.runtime]
label = "orders-default"

[[tool.pyscript.endpoints]]
id = "calculate_total"
task_type = "compute"
entrypoint = "orders:calculate_total"
io_type = ["rest", "grpc"]
num_cpus = 1.0

[tool.pyscript.endpoints.prices]
type = "List"

[tool.pyscript.endpoints.prices.items]
type = "Double"

[tool.pyscript.endpoints.response_schema]
type = "Double"

[[tool.pyscript.endpoints]]
id = "create_order"
task_type = "io"
entrypoint = "orders:create_order"
io_type = ["grpc"]

[tool.pyscript.endpoints.order]
type = "Struct"
required = ["customer_id"]
additionalProperties = false

[tool.pyscript.endpoints.order.properties.customer_id]
type = "String"

[tool.pyscript.endpoints.response_schema]
type = "Struct"
required = ["order_id"]
additionalProperties = false

[tool.pyscript.endpoints.response_schema.properties.order_id]
type = "String"
```

## 发布校验规则

manager 解析并发布 Revision 时应执行以下校验：

1. `spec_version` 必须受支持；
2. `runtime.label` 必须引用存在且拥有活动版本的逻辑环境标签；
3. 环境 Python 版本必须满足 `requires-python`；
4. 环境中已解析的包版本必须满足 `dependencies`；
5. 至少声明一个 endpoint，且所有 `id` 唯一；
6. `entrypoint` 必须符合 `module:function` 格式；
7. `task_type` 只能是 `io` 或 `compute`；
8. `num_cpus` 和 `num_gpus` 只能用于 Compute endpoint；
9. `io_type` 只能包含 `rest`、`grpc`，且 gRPC endpoint 必须声明
   `response_schema`；
10. 自动生成的 Proto 必须可编译，字段编号必须能与上一份 Descriptor 稳定合并；
11. 不接受 Worker Pool、Runtime Env、密钥或 Token 字段；
12. 校验成功后，解析结果与自动生成的 Artifact digest 一起写入 Revision；代码与
    接口不可变，裸逻辑标签的已解析环境绑定允许兼容热替换。

## 常见接入错误

| 现象 | 原因与处理 |
| --- | --- |
| Git 同步找不到 `pyproject.toml` | 文件未提交到仓库根目录，或 manager 跟踪了错误分支。 |
| 构建提示 Runtime Label 不存在 | 确认逻辑标签已创建并有活动版本，例如 `runtime.label = "data-default"`。 |
| Python 版本不兼容 | 调整 `requires-python`，或引用 Python 版本匹配的 Runtime Label。 |
| 外部依赖不兼容 | 为该 Runtime Label 创建兼容的新版本；激活前平台会校验所有跟随它的 Revision。 |
| 提示 `request_schema is not supported` | 已改为展平参数；使用 `[tool.pyscript.endpoints.x]`。 |
| HTTP 调用返回脚本执行失败 | 检查 JSON 键是否与函数参数名一致，并检查调用日志中的具体异常。 |
| Worker 导入第三方包失败 | 检查构建记录中的依赖校验结果与实际 import 名称是否一致。 |
| gRPC Struct 提示必须关闭额外字段 | 添加 `additionalProperties = false`，保证 Schema 能映射为固定 Proto 字段。 |
| 激活返回 `409` | Revision 状态、服务状态或 gRPC Proto 契约尚不满足激活条件。 |

## 接入检查清单

- `pyproject.toml` 已提交到 Git 仓库根目录；
- `runtime.label` 指向已有活动版本的逻辑标签；
- `requires-python` 与 `dependencies` 已准确声明；
- endpoint `id` 在 Revision 内唯一；
- `entrypoint` 与 ZIP 内实际模块和函数一致；
- HTTP 参数名是合法 Python 参数名，并与函数签名一致；
- IO/Compute 类型选择正确；
- Runtime Label 的 Python 和第三方依赖满足项目约束；
- 需要 gRPC 的 endpoint 已在 `io_type` 中加入 `grpc` 并声明返回 Schema；
- Git 同步构建成功并自动上线后再调用服务。

早期设计稿中的 `[[tool.pyscript.api]]`、`type`、`entrance`、`params` 和拼写错误
的 `resturn` 不属于本规范。统一使用 `endpoints`、`task_type`、`entrypoint`、
endpoint 下的展平参数子表和 `response_schema`。
