# 用户使用指南

本文介绍平台部署后的日常使用，不涉及集群安装。尚未部署请先看
[部署指南](./deploy/README.md)，完整脚本规范见 [Hook 接入规范](./pyproject_interface.md)。

首次使用按这条顺序操作：**登录 → 创建运行环境 → 准备脚本 → 注册并发布服务 → 创建 Token → 调用和查看日志**。
只需要调用现有接口的用户，可以直接阅读第 6、7 节。

## 1. 进入平台

向部署人员获取管理页面地址、业务 HTTP/gRPC 地址及初始管理员密码。
本机默认管理地址为 `http://127.0.0.1:8000`，实际部署可能使用域名或其他端口。

- 管理页面只有一个固定账户 `admin`，登录时只输入密码。
- 左侧包括“概览”“服务”“运行环境”“API Token”“调用记录”。
- 点击左下角“修改密码”，输入当前密码及至少 12 个字符的新密码；修改后所有管理员会话失效，需要重新登录。
- `/api-docs` 是公开的业务接口文档，无需登录，可查看参数、返回值结构及允许的调用方式。

管理员登录用于操作平台，不能替代业务调用 Token。初始密码由部署人员设置，没有通用默认密码。

## 2. 创建运行环境

进入“运行环境”，点击“创建环境”，填写：

| 字段 | 如何填写 |
| --- | --- |
| 标签名称 | 如 `default`、`data-default`；业务仓库引用这个名称 |
| Python | 当前默认集群使用 `3.12`，必须与所选 Worker 的解释器匹配 |
| Worker 类型 | 选择平台发现的容器类型，如 `default`；由运维提供，不能在页面修改 |
| 依赖锁定 | 每行一个精确版本，如 `httpx==0.28.1`；无第三方依赖可留空 |
| PyPI 来源 | 官方源、自定义 PyPI server 或平台私有源 |
| 导入校验 | 每行一个需要测试导入的模块，如 `httpx`；无额外模块可留空 |

点击“创建并校验”，等待临时 Ray 环境安装并验证。首个通过校验的环境版本会自动激活。
失败时查看版本上的错误原因，修正依赖或包源后重新创建版本。

使用私有源时：

- 自定义地址填写 Simple Index，例如 `http://pypi.example.internal:12341/simple/`，Ray Worker 必须能够访问。
- devpi 填写完整索引地址，例如 `http://pypi.example.internal:12342/shrx/pkg/+simple/`；已有的 `/simple/` 和 `/+simple/` 后缀会保留，省略索引后缀时默认补全 `/simple/`。
- 自定义地址不能放用户名和密码；需要认证请选择“平台私有源”，由部署人员配置凭据。
- 环境依赖使用 `包名==版本`，不要写成 `>=` 或未固定版本的包名；URL 依赖必须包含 `#sha256=`。

Python 层标签和 Worker 类型不是同一个概念：标签定义 Python 依赖，Worker 类型选择运行容器。
Hook 包由平台分发，不要求在环境中重复安装；使用 `ArrowTable` 的业务环境则需要提供 `pyarrow`。
填写另一个 Python 版本不会自动更换 Worker 的解释器。

## 3. 准备业务脚本

下面使用无额外依赖的 `default` 环境，建立一个名为 `demo` 的服务。
Git 仓库根目录放两个文件：

```text
demo/
├── pyproject.toml
└── main.py
```

`pyproject.toml`：

```toml
[project]
name = "demo"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = ["pyscripts-hook"]

[tool.pyscript]
spec_version = 2
runtime = "default"
module = "main"
```

`main.py`：

```python
from sc_hook import IOType, Int64, hook


@hook(io_types=[IOType.Inner])
def add_compute(a: Int64, b: Int64) -> Int64:
    print(f"compute: {a}+{b}")
    return a + b


@hook(io_types=[IOType.Rest, IOType.Grpc])
async def add(a: Int64, b: Int64) -> Int64:
    print(f"io: a={a}, b={b}")
    return await add_compute.remote_call(a, b)
```

将文件提交并推送到 Git 仓库。关键规则：

- 接口由 `@hook` 和类型注解定义，不再在 TOML 中声明 Endpoint。
- `async def` 为 IO，普通 `def` 为 Compute；参数和返回值必须有受支持的类型注解。
- `IOType.Rest` / `IOType.Grpc` 开放对应公共接口；`IOType.Inner` 只供同一服务 Revision 内部调用。
- `await endpoint.remote_call(...)` 只允许在 Hook IO 函数执行期间使用，用于等待内部任务结果。
- 函数参数展平传入；服务保持无状态，不依赖请求之间共享的可变全局变量。
- `runtime = "default"` 跟随标签的活动版本，不必手动填写环境版本号。
- `module = "main"` 对应根目录 `main.py`；多个模块使用 `modules = ["module_a", "module_b"]`，两者不能同时设置。

业务开发环境需要能导入 `sc_hook`，可从你们的包源安装 `pyscripts-hook`。
本地可用 `asyncio.run(add(1, 2))` 验证返回 `3`；这只验证本地逻辑，不等于远程调度已通过测试。

## 4. 注册与发布服务

进入“服务”，点击“注册服务”：

1. 服务名称填 `demo`。名称以小写字母开头，总长 2–64 个字符，可含数字、下划线和连字符；创建后不可修改。
2. 填写 HTTP/HTTPS Git 仓库地址，不支持 SSH 或本地路径。私有仓库的读取权限由部署人员配置。
3. 选择跟踪分支，如 `main`；留空跟随仓库默认分支。
4. 首次建议选择“手动发布”，注册后打开服务详情。
5. 点击“同步并发布”，再确认“同步并发布”，等待校验和构建完成。

平台自动读取仓库中的运行环境、检查 Python/依赖兼容性、发现 Hook 接口，并生成
Revision、Artifact、digest 和 gRPC Proto。无需手动上传这些文件或指定 Revision。
发布成功后查看服务状态、Endpoint 和发布历史；本例的公共接口是 `add`，内部 `add_compute` 不公开给调用者。

常用服务操作：

| 操作 | 效果 |
| --- | --- |
| 同步并发布 | 拉取目标分支最新 commit；服务未停止时自动激活新版本 |
| 停止 | 拒绝新的业务调用，保留当前活动 Revision；不以强行终止在途调用代替排空 |
| 启动 | 恢复已停止服务的当前活动 Revision，不拉 Git、不重新构建 |
| 发布历史中的“激活” | 把一个 READY Revision 切换为活动版本 |
| 设置 | 修改仓库、跟踪分支、更新策略和 Endpoint 调用记录保留数量 |
| 删除 | 先停止并等待在途调用结束，再输入服务名确认；删除服务及其版本、契约、调用记录和日志，不可在页面恢复 |

**停止后发布新代码只创建 READY Revision，不会自动启动。**
直接点击“启动”恢复的是此前的活动版本；要上线新代码，先在发布历史中激活目标 READY Revision，再点击“启动”。
如果从未有活动版本，也需要先发布并激活。

修改 Git 地址或分支不会立即切换正在运行的代码，下一次成功发布后才生效。
同一 Git commit 的重复同步复用已有 Revision，修改代码后应先提交并推送。

## 5. 更新代码与环境

### 手动或 Webhook 更新代码

手动更新：提交并推送代码，然后点击“同步并发布”。

Webhook 更新：

1. 在“服务 → 设置”把更新策略设为 Webhook 并保存。
2. 点击“生成地址”，复制链接到 Gitea/GitLab 仓库的 Push Webhook，使用 JSON 请求体。
3. 推送目标分支后，在发布历史确认新 Revision 已成功上线。

Webhook 地址含密钥，不要公开。地址遗失可“轮换令牌”重新生成，并同步修改仓库配置；旧链接失效。
Git 服务器必须能访问此链接；如果生成的是错误内网地址，请让部署人员设置公开访问域名。
Webhook 接受通知不代表发布成功，后台失败需要运维查看控制面日志并手动补发。

页面虽然有“定时检查”，当前未接入周期性 Git 同步，请使用手动或 Webhook。

### 更新运行环境

点击环境标签名称或任意版本行，可查看该版本的完整依赖配置、已解析依赖、包源地址、导入检查及校验结果。

在标签卡片点击“新版本”，默认载入最新版本的配置，未激活或校验失败的版本也可作为修改基础。
也可以在历史版本详情中点击“基于此版本创建”，或在新版本表单的“基于版本”中选择其他版本。
切换基础版本会重新载入配置；修改并提交后生成新版本，旧版本配置保持不变。新版本通过校验后，点击“激活”。
系统先检查跟随此标签的 Revision 是否兼容，全部通过才切换；在途请求保持原执行快照。
依赖不兼容时不能强行激活，应修改业务依赖约束或环境版本。

页面“服务引用”按服务统计，不按 Endpoint 数量统计。正在被引用或仍有执行的环境版本不能停用；
活动版本也不能直接停用。需要先激活兼容的新版本并处理旧引用，等待旧执行结束。

## 6. 创建和管理 API Token

管理员进入“API Token”，填写用途名称，如 `demo-client`，点击“创建 Token”。
立即复制保存完整 `pst_...` 值；刷新/关闭后不能再次找回，列表只展示前缀。

- 一个有效 Token 可以调用所有已启动的公共接口，不提供每服务权限。
- Token 放在 HTTP Header 或 gRPC metadata，不放在业务参数中。
- 点击“删除”后立即失效，并从列表移除；调用者必须更换 Token。
- Token 不提交 Git、不打印到脚本日志；不受信任网络使用 HTTPS/TLS。

## 7. 调用接口

调用者先打开平台 `/api-docs`，选择服务查看参数、返回结构、REST 路径和 gRPC 信息。
文档可公开查看，**执行接口仍需要 Token**。公共调用地址由部署人员提供，不是 Ray Client 地址。

### REST

本例请求体为 `{"a":1,"b":2}`，键直接对应函数参数，不加 `params` 包装，也不加入 `pyscripts_token`。
在 Bash 中交互输入 Token 后调用；把地址替换为实际入口：

```bash
read -r -s -p 'API Token: ' PYSCRIPTS_TEST_TOKEN
curl --fail-with-body 'http://127.0.0.1:8000/v1/services/demo/add' \
  -H "Authorization: Bearer ${PYSCRIPTS_TEST_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"a":1,"b":2}'
unset PYSCRIPTS_TEST_TOKEN
```

响应不是裸数字，而是执行结果对象：

| 字段 | 说明 |
| --- | --- |
| `request_id` / `service` / `revision` | 定位调用及实际代码版本 |
| `status` / `error` | `SUCCEEDED` 或 `FAILED`，以及业务异常信息 |
| `result` | 业务返回值，本例成功时为 `3` |
| `logs` | 输出列表，含 `sequence`、`stream`、`content`、`emitted_at` |
| `log_bytes` / `logs_truncated` | 捕获字节数及是否达到上限而截断 |

业务异常可以以 HTTP 200 和 `status="FAILED"` 返回，不能只检查 HTTP 状态码。
认证、调度或超时等错误则可能返回非 2xx 和 `detail`。请求必须是 JSON 对象，不要二次编码成字符串。

### gRPC

1. 从公开文档或服务详情点击“下载 Proto”，解压到客户端目录 `proto/`。
2. 使用所需语言的工具生成客户端。平台只提供 Proto，不提供 SDK 安装包。
3. 使用实际 Proto 中的 Service/RPC 调用，附加 `authorization` metadata，并检查 `execution.status`。

Python 客户端在自己的虚拟环境安装生成工具，然后编译；替换下方实际 Proto 路径：

```bash
python -m pip install grpcio grpcio-tools
mkdir -p client
PYSCRIPTS_PROTO_FILE='proto/实际生成的包名/v1/service.proto'
python -m grpc_tools.protoc -I proto \
  --python_out=client --pyi_out=client --grpc_python_out=client \
  "$PYSCRIPTS_PROTO_FILE"
```

生成工具和 Stub 使用方式见 [gRPC Python 指南](https://grpc.io/docs/languages/python/quickstart/)。
将 `client/` 加入客户端 Python 模块搜索路径，按实际生成包导入 `service_pb2` / `service_pb2_grpc`。
本例 Service 为 `DemoService`、RPC 为 `Add`、请求为 `AddRequest`：

```python
response = stub.Add(
    service_pb2.AddRequest(a=1, b=2),
    metadata=(("authorization", "Bearer pst_..."),),
    timeout=30,
)
```

`stub` 用生成的 `DemoServiceStub(channel)` 创建。受信任明文测试入口用
`grpc.insecure_channel("host:50051")`；TLS 入口用部署人员提供的域名和 TLS Channel。
生成包名包含服务身份，必须使用下载得到的真实目录，不能直接复用其他服务的 Stub。

本例标量返回值在 `response.result`，状态/错误/日志在 `response.execution`。
Struct 返回值的业务字段直接位于 Response 中，不统一包成 `result`。
`execution.status` 使用 `PYSCRIPTS_EXECUTION_STATUS_SUCCEEDED` / `PYSCRIPTS_EXECUTION_STATUS_FAILED` 枚举；
业务失败可能正常返回 Response，连接、认证或超时则可能抛出 `grpc.RpcError`。
接口变化后重新下载并核对 Proto，破坏性变化需要更新客户端；当前没有 streaming 接口。

## 8. 查看调用记录和日志

进入“调用记录”，或“服务 → 最近调用”，点击刷新按钮获取最新记录，再点击一条记录打开“调用输出”。
可以查看状态、调用方式（REST/gRPC）、Request ID、耗时、输出大小、错误，以及带时间点的 stdout/stderr。
本例应同时看到 IO 和内部 Compute 的打印内容。当前不保存调用参数内容。

日志在执行结束后展示和随结果返回，不实时推送。平台捕获 Python 标准输出/错误；
原生库或外部子进程绕过 Python 流的输出不保证被捕获。达到单次字节上限会标记“日志已截断”。

在“服务 → 设置 → Endpoint 调用日志保留”设置每个接口保留的近期已完成调用数：
默认 100，可设 1–10000；正在执行的记录不因保留策略被清理。
这是**调用数量**，不是每次调用的日志行数。减少保留数量会清理旧的已完成记录及日志，操作前确认无需留存。

## 9. 常见问题

| 问题 | 如何处理 |
| --- | --- |
| 无法创建环境、Worker 类型为空 | 联系部署人员检查 Ray 连接和在线 Worker；不能在页面自行创建容器类型 |
| 依赖不兼容 | 对照项目 `requires-python` / `dependencies` 和环境解析版本，创建兼容的新环境 |
| `module` 配置错误 | 根目录 `main.py` 填 `module = "main"`；不要填文件路径或 `.py` 后缀 |
| 发布后仍是旧代码 | 检查目标分支、commit 是否推送，以及新 READY Revision 是否已激活 |
| 接口返回 404 / UNIMPLEMENTED | 服务是否启动、Endpoint 是否开放该协议、gRPC Stub 是否匹配当前 Proto |
| 401 / UNAUTHENTICATED | 检查 Bearer / metadata 和 Token 是否已删除；管理员登录不能替代 Token |
| HTTP 成功但业务没成功 | 检查 REST `status` 或 gRPC `execution.status`，以及 `error` 和调用记录 |
| 首次调用明显较慢 | 新环境或新 Worker 可能安装依赖、下载代码；正式接入前先做真实调用验证 |
| 日志没有立即出现 | 当前日志在调用结束后返回，点击刷新后查看 |
| Webhook 不更新 | 检查跟踪分支、仓库通知地址/事件，联系运维查看后台发布错误 |

排障时提供服务名、Endpoint、Request ID、Revision 和报错即可，不要发送密码或完整 Token。
