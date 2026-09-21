该项目是一个基于ray为分布式调度底座实现的无状态python脚本服务托管平台

# 项目架构
## 用户界面
用户界面是一个前后端项目, 用户可以在这里注册, 启用, 停用服务, 并且查看服务的运行状态和运行日志
功能:
1. 注册服务(git仓库), 设置代码跟踪方式和检查更新时间间隔
2. manager检测Git变化后先校验pyproject.toml引用的环境版本及依赖兼容性，成功后自动生成revision、artifact和digest
3. 启动服务(通知api server服务可以被使用)
4. 停止服务(通知api server服务停止)
5. 查看使用统计和请求记录

## manager
manager是一个常驻服务,  
其职能
1. 负责定时自动获取最新的脚本数据   
2. 把"脚本解析"为ray的task或者actor服务   
3. 把接口改动传递给api server

manager是项目的核心组件, 其需要对接脚本层, 服务层以及ray调度层  
需要把从git仓库拉取到的脚本, 解析为ray可以接受的任务, 以及restful api或者 grpc api.

manager读取git仓库中的pyproject.toml接口定义、逻辑环境标签，以及标准
`[project].requires-python`和`[project].dependencies`约束。
服务注册时只配置Git仓库和跟踪策略；运行环境属于Git revision的一部分。

服务运行环境设置主要是
1. python版本
2. 项目依赖
3. 容器标签(决定代码在那类节点中执行)

实际环境内容由版本化runtime label定义；pyproject.toml默认只引用逻辑标签并声明
脚本需要满足的Python和外部依赖约束。manager解析标签的当前活动版本并验证兼容后
才允许构建revision；标签版本更新时先重新校验所有跟随者，再热替换运行环境。

接口信息
1. 有哪些接口入口
2. 每个接口的定义和返回值定义
3. 是io还是compute任务

```toml
[tool.pyscript]
spec_version = 1

[tool.pyscript.runtime]
label = "polars-etl"

[[tool.pyscript.endpoints]]
id = "data_wash"
task_type = "compute"
entrypoint = "src.script:wash"

[tool.pyscript.endpoints.start_date]
type = "Date"

[tool.pyscript.endpoints.end_date]
type = "Date"

[tool.pyscript.endpoints.drop_duplicate]
type = "Bool"

[tool.pyscript.endpoints.response_schema]
type = "String"

```

## api服务
api server从manager那里获取到最新的接口数据, 并且启动接口.  接口调用会直接被转到ray执行层, 不经过manager.
同时api server需要记录接口调用的记录, 并且把调用记录存入数据库.

### 接口数据类型定义
详见`./data_design.md`

### gRPC动态接口
1. 用户只在endpoint的`io_type`声明`grpc`; 平台根据展平参数与`response_schema`自动生成proto和内部descriptor.
2. 客户端下载proto后按自己的语言工具链生成标准强类型Stub, 不直接调用`Invoke(bytes)`信封接口.
3. api server启动时只注册一个固定的`GenericRpcHandler`, 根据原生gRPC method path查询不可变RouteRegistry快照.
4. Gateway不解析业务消息, 将原始protobuf bytes透传到已经固定revision的IO Actor或Compute Task.
5. 每个revision携带`FileDescriptorSet`, 执行器使用revision独立的DescriptorPool完成请求解码、展平参数调用与响应编码, 禁止注册到全局DescriptorPool.
6. 路由切换后新请求使用新revision; 已匹配的请求继续持有旧路由, 直到inflight归零后释放旧代码和descriptor.
7. 字段编号继承上一份descriptor; 删除或改类型的字段号和名称写入`reserved`. 兼容变化自动提升minor, 破坏性变化自动提升major并切换protobuf package版本.
8. 当前落地范围为unary-unary; streaming需要分别实现对应的RpcMethodHandler并保持调用基数不变.
9. 发布阶段自动把生成的proto和descriptor加入最终Artifact; 手写`grpc_contract`只作为旧服务兼容模式.
10. schema digest变化时自动生成确定性的proto源码包，并把proto包与内部descriptor按digest持久化到对象存储；code revision变化但schema不变时复用已有契约.
11. 新契约及其对象存储文件未处于READY状态时禁止激活revision；服务重启不得重新生成已发布proto.

## ray执行层
k8s提供的ray执行集群

### IO Actor 与 Compute Task
执行层按任务类型拆分，但共享不可变 runtime profile 和 revision 执行快照。

1. IO 任务发送到按 runtime profile 创建的异步 Actor；同一 Actor 可在 `max_io` 范围内共享事件循环。
2. Compute 任务包装为无状态 Ray Task，由 Ray 直接按照 CPU/GPU 资源调度，不进入 Actor。
3. Actor 只接受 IO lease；Compute 请求误入 Actor 时必须明确失败，不能回退到旧的独占 Actor 路径。
4. Compute 使用固定的通用执行 Task，每次只传 revision、artifact digest、入口、上下文和参数，不捕获业务代码闭包。
5. Artifact 按 SHA-256 缓存在 Worker 节点；首次下载后复用本地只读文件，不随每次请求重新传输。
6. Compute 每次执行结束后卸载业务模块，禁止依赖跨请求可变全局状态；已验证和解压的代码文件可以继续缓存。
7. runtime profile 更新后，新请求使用新 environment digest；已派发 Task 继续持有旧执行快照直至完成。
8. Invocation 必须记录 execution kind、artifact digest、runtime profile version 和 environment digest；旧环境只有在 Actor lease 与 Compute invocation 都归零后才能释放。
9. Compute Task 的本地待提交数量必须有上限；请求取消或超时时同时取消 Ray ObjectRef。

### 两层运行标签

1. 容器层使用 `worker_pool`，由 K8s/KubeRay 在 Ray Worker 节点上提供；控制面只发现和引用，不允许用户创建、修改或删除。
2. Python 层使用可版本化的 runtime label，由用户创建；具体版本通过 Ray `runtime_env` 安装、校验并以 environment digest 标识。
3. runtime profile 只能引用当前集群已发现的 worker pool；环境校验、IO Actor 和 Compute Task 都必须携带该容器层硬约束。
4. 相同 environment digest 的 Compute Task 优先使用已预热节点，亲和失败时只能回退到相同 worker pool，不能跨容器类型执行。

### 业务 Artifact 分发

1. 发布阶段必须将业务 ZIP 按 SHA-256 内容寻址上传到 S3 兼容对象存储，数据库只保存稳定的 `s3://bucket/key` 与 digest。
2. 调度阶段由控制面生成短期预签名下载 URL；对象存储凭据不得传递给 Ray Worker。
3. Worker 仅在节点本地缓存未命中时下载，并必须在解压前重新校验 SHA-256。
4. 预签名 URL 不得持久化；Artifact 只有在没有 revision 引用且超过保留期后才能由垃圾回收任务删除。


## 数据库
使用postgresql作为后端数据库.
主要储存服务元信息, 例如服务注册信息, 当前服务checkpoint, apiserver会监控这部分数据调整api
还有每个服务的调用记录, 例如调用参数, 中间的日志捕获(脚本中的print), 调用返回结果

第一版日志捕获在业务执行期间通过 ContextVar 路由 Python stdout/stderr，执行结束后随
ExecutionOutcome 一次性返回并分块写入 invocation_logs；异步 IO 请求之间必须隔离，
Compute Task 使用同一结果协议。日志有单次调用字节上限并记录 truncated 状态，当前不提供实时流。
