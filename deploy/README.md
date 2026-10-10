# 项目部署指南

推荐部署方式：**控制面使用 Docker Compose，Ray 执行集群使用 Kubernetes/K3s**。
PostgreSQL 和 S3 兼容对象存储使用已有服务。以下命令从项目根目录执行；
Kubernetes 命令在配置好 `kubectl` 和 Helm 的机器上执行，Compose 命令在控制面主机执行。

## 1. 准备基础设施

开始前确认：

- Kubernetes 节点 Ready，已安装 `kubectl`、Helm；控制面主机安装 Docker 和 Compose V2。
- PostgreSQL 已有数据库 `pyscripts`，应用账号可以建表、修改表和读写数据。
- S3 已有私有 Bucket `pyscripts`，准备可以上传、读取对象的 Access Key/Secret Key。
- 控制面能访问 PostgreSQL、S3、HTTP/HTTPS Git 仓库和 Ray；Ray Worker 能访问 S3 和依赖包源。

没有数据库时，由管理员在 `psql` 中执行；已有库跳过：

```sql
CREATE ROLE pyscripts_adm LOGIN;
\password pyscripts_adm
CREATE DATABASE pyscripts OWNER pyscripts_adm;
\connect pyscripts
GRANT USAGE, CREATE ON SCHEMA public TO pyscripts_adm;
```

项目只初始化表，不创建数据库或 S3 Bucket。对象存储使用 **S3 API 端口**，不是管理页面端口。

## 2. 部署 Ray 集群

当前基线为 **KubeRay 1.7.0、Ray 2.58.0、Python 3.12**，不要单独升级某一侧的 Ray 版本。

安装 Operator；已经安装则跳过：

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm upgrade --install kuberay-operator kuberay/kuberay-operator \
  --version 1.7.0 \
  --namespace kuberay-system --create-namespace \
  --wait --timeout 10m
```

部署仓库提供的 [raycluster.yaml](./k8s/raycluster.yaml)：

```bash
kubectl apply -f deploy/k8s/raycluster.yaml
kubectl -n pyscripts get raycluster,pods,svc
kubectl -n pyscripts get pods -l ray.io/cluster=pyscripts-ray -w
```

等待 Head 为 `2/2 Running`、Worker 为 `1/1 Running`，RayCluster 为 `ready`。
`-w` 时只观察一种资源，按 Ctrl+C 结束观察。

默认只提供 `default` Worker Pool，使用 Ray 官方镜像：

| 组件 | 默认资源 |
| --- | --- |
| Head | CPU request 1 / limit 2，内存 request 2Gi / limit 4Gi |
| 单个 Worker | 2 CPU、4Gi 内存 |
| Worker 数量 | 初始 1，最少 1，最多 10 |

另留 Autoscaler 和 Kubernetes 的开销；起步 requests 约 3.25 CPU、6.25Gi 内存。
扩容上限不会增加物理节点容量。资源不足时，先修改 YAML 中的资源和 `maxReplicas`。

控制面在集群外使用 Compose，需要开放 Ray Client 的受控 LAN 入口：

```bash
kubectl apply -f deploy/k8s/ray-client-nodeport.yaml
kubectl -n pyscripts get svc pyscripts-ray-client-lan
```

此入口为 **Ray 节点 IP:31001**，转发到 Head 的 `10001`；不开放 Dashboard。
**应用前用防火墙限制来源为控制面主机，禁止公网开放。**
默认集群 YAML 未启用认证，仅用于受信任网络。

长期部署建议在首次创建前给 RayCluster 的 `spec` 增加 `authOptions: {mode: token}`，
按 [KubeRay Token 配置](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/kuberay-auth.html)
启用认证，再将对应的 `RAY_AUTH_MODE=token` 和 `RAY_AUTH_TOKEN` 写入控制面的 `.env`。
这不是 Web UI 创建的业务 API Token，认证也不替代网络隔离或加密。

## 3. 配置控制面

在控制面主机获取完整项目源码。尚无 `.env` 时复制模板，已有文件不要覆盖：

```bash
cp -n .env.example .env
chmod 600 .env
openssl rand -hex 32
```

最后一条生成 Session Secret。编辑 `.env`，重点修改以下字段，其余先保留模板默认值。
下面 IP 和凭据均为示例，必须按实际环境替换：

```dotenv
PYSCRIPTS_DATABASE_HOST=192.168.0.11
PYSCRIPTS_DATABASE_PORT=5432
PYSCRIPTS_DATABASE_NAME=pyscripts
PYSCRIPTS_DATABASE_USER=pyscripts_adm
PYSCRIPTS_DATABASE_PASSWORD='replace-with-database-password'

# Kubernetes 节点的 LAN IP，不是 Head Pod IP
PYSCRIPTS_RAY_ADDRESS=ray://192.168.0.21:31001
PYSCRIPTS_RAY_NAMESPACE=pyscripts
PYSCRIPTS_RAY_USE_LABEL_SELECTOR=true
PYSCRIPTS_DEFAULT_WORKER_POOL=default

# 地址必须同时被控制面容器和 Ray Worker 访问
PYSCRIPTS_OBJECT_STORE_ENABLED=true
PYSCRIPTS_OBJECT_STORE_ENDPOINT_URL=http://192.168.0.11:19000
PYSCRIPTS_OBJECT_STORE_BUCKET=pyscripts
PYSCRIPTS_OBJECT_STORE_ACCESS_KEY_ID='replace-with-access-key'
PYSCRIPTS_OBJECT_STORE_SECRET_ACCESS_KEY='replace-with-secret-key'

PYSCRIPTS_ADMIN_PASSWORD='replace-with-strong-password'
PYSCRIPTS_ADMIN_SESSION_SECRET='replace-with-generated-session-secret'
PYSCRIPTS_ADMIN_COOKIE_SECURE=false

PYSCRIPTS_GRPC_ENABLED=true
```

注意四点：

- 容器中的 `localhost` 是容器自己；集群外不能直接使用 `*.svc.cluster.local` 地址。
- 如果设置过 `PYSCRIPTS_DATABASE_URL`，它会覆盖分项数据库配置；不用时移除旧值。
- 管理员密码只在首次启动时创建；之后通过 Web UI 修改，改 `.env` 不会重置已有密码。
- `.env` 不提交 Git。接入 HTTPS 后设置 `PYSCRIPTS_ADMIN_COOKIE_SECURE=true`；
  使用 Webhook 时再设置 `PYSCRIPTS_PUBLIC_BASE_URL=https://实际访问域名`。

## 4. 构建、初始化并启动

```bash
docker compose config --quiet
docker compose build pyscripts
docker compose run --rm --no-deps pyscripts pyscripts-db --init
docker compose up -d --no-build pyscripts
docker compose ps
docker compose logs --tail=100 pyscripts
```

构建会自动编译 Web UI，不需要另外启动 Bun 服务。构建主机需要能下载镜像及 npm/Python 包。
`PYSCRIPTS_CONTRACT_CACHE_ROOT` 只配置本地 Proto/Descriptor 构建缓存；原生默认
`/tmp/pyscripts-contracts`，Compose 覆盖为 `/var/lib/pyscripts/contracts`，已发布契约持久化到 S3。
当前 `pyproject.toml` 含开发用内网 PyPI 地址，换网络部署前检查其可达性及锁文件配置；
Web UI 的运行环境包源不影响控制面镜像构建。

默认入口：

- 管理页面：`http://127.0.0.1:8000`
- 公开调用文档：`http://127.0.0.1:8000/api-docs`
- gRPC：`127.0.0.1:50051`

需要受信任 LAN 访问时，在 `.env` 增加下面配置，限制主机防火墙来源后重新创建容器：

```dotenv
PYSCRIPTS_HTTP_BIND=0.0.0.0
PYSCRIPTS_GRPC_BIND=0.0.0.0
```

随后浏览器访问控制面主机的 LAN IP，而不是 `127.0.0.1`。
生产入口使用 HTTPS/gRPC TLS 代理，不要把明文管理和 Token 调用直接暴露公网。
控制面保持 **一个实例、一个 Uvicorn worker**。

## 5. 创建环境并发布服务

在 Web UI 中按以下顺序操作：

1. 使用固定账号 `admin` 登录，只需输入 `.env` 中的初始密码。
2. 在环境管理创建 Python 标签，如 `default`，选择 `default` Worker Pool、Python `3.12`。
3. 配置业务依赖和 PyPI 地址，等待环境校验成功。默认官方源，也可填可达的私有 Simple Index。
4. 创建服务，填写 **HTTP/HTTPS Git 地址**、跟踪分支和更新策略。
5. 仓库按 [Hook 接入规范](../pyproject_interface.md) 定义接口，并在 `pyproject.toml`
   引用该运行环境；点击发布，Revision、Artifact 和 digest 自动生成。
6. 确认服务已启动，再创建业务 API Token，用于 REST/gRPC 调用。

环境依赖由 `runtime_env` 安装，普通 Python 依赖无需另做 Worker 镜像。
Hook 包由平台分发，不要求重复加入运行环境；系统库/GPU 等特殊依赖仍需专用 Worker 镜像。
集群外控制面只发现在线节点的 Pool，保留当前 YAML 的最少 1 个 Worker。

自动更新选择 Webhook，在服务设置生成链接，配置到 Gitea/GitLab 的 Push Webhook。
当前 `poll` 尚未接入周期调度，先使用手动发布或 Webhook。

## 6. 验证部署

先检查控制面和数据库：

```bash
curl --fail http://127.0.0.1:8000/health/ready
docker compose exec pyscripts pyscripts-db
```

然后在公开文档中找到实际服务/接口，使用 Token 调用，例如：

```bash
read -r -s -p 'API Token: ' PYSCRIPTS_TEST_TOKEN
curl --fail-with-body 'http://127.0.0.1:8000/v1/services/example/add' \
  -H "Authorization: Bearer ${PYSCRIPTS_TEST_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"a":1,"b":2}'
unset PYSCRIPTS_TEST_TOKEN
```

这里的 `example/add` 和参数需替换成已发布接口。确认返回成功、结果正确，
并在调用记录查看执行状态、调用方式和日志。**健康检查成功不等于 Ray/S3 链路正常，必须实际调用一次。**

gRPC 从服务详情下载 Proto，生成对应语言的客户端，以
`metadata=(("authorization", "Bearer pst_..."),)` 附加 Token。
平台只提供 Proto，不提供 SDK；默认仅支持 unary 调用。

## 7. 日常操作和快速排障

修改 `.env` 后重新创建容器；仅 `restart` 不会加载新的容器环境配置：

```bash
docker compose up -d --no-build --force-recreate pyscripts
```

更新控制面/UI 代码后重建；升级前备份 PostgreSQL 和 S3，先排空在途请求：

```bash
docker compose build pyscripts
docker compose run --rm --no-deps pyscripts pyscripts-db --init
docker compose up -d --no-build --force-recreate pyscripts
```

业务脚本更新只需发布新 Revision，不需要重建控制面。常用检查：

```bash
docker compose logs --tail=200 pyscripts
kubectl -n pyscripts get raycluster,pods,svc
```

| 问题 | 优先检查 |
| --- | --- |
| 页面报 500 | 控制面日志、数据库连通和表初始化 |
| Ray Client 超时 | 节点 IP、31001 入口、防火墙、Ray Token 和版本匹配 |
| 环境校验慢/失败 | Worker 能否访问包源、依赖是否齐全、节点资源是否够 |
| 代码下载失败 | Worker 能否访问 S3 API 地址/端口、Bucket 权限和签名 |
| 401 / UNAUTHENTICATED | 业务 API Token 是否有效，不是管理员密码 |
| 404 / UNIMPLEMENTED | 服务是否启动、接口协议是否开放、Proto 是否匹配 |

详细配置查看 [.env.example](../.env.example)；Ray 资源配置修改
[raycluster.yaml](./k8s/raycluster.yaml)。不要删除正在被数据库引用的 S3 Artifact/Proto。
