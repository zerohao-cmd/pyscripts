# 项目部署指南

正式部署统一使用 **Kubernetes/K3s**，网关同时提供 REST、原生 gRPC 和 Web UI。
Dockerfile 只负责构建镜像；测试直接运行 Python 和 Ray 进程。
PostgreSQL 与 S3 使用已有服务。本指南从空的 `pyscripts` namespace 开始，
不要求手动复制 Ray token，也不对外开放 Ray Client、Dashboard 或 GCS。

`deploy/k8s` 中的文件作为配置示例，部署前按实际环境审阅并修改。
镜像构建在开发机完成，管理机只需部署文件；目标机没有 Git 时，
可通过 SCP/SFTP 传输 `deploy` 目录并保留内部结构。
下面的命令均相对于包含 `deploy` 目录的工作目录执行。

## 1. 准备基础设施

需要：

- 可用 Kubernetes/K3s，管理主机安装 kubectl 与 Helm。
- 已有 PostgreSQL 数据库 `pyscripts`，应用账号可建表及读写；已有私有 S3 Bucket `pyscripts`。
- 节点能拉取网关与 Ray 镜像；网关能访问数据库、S3、HTTP/HTTPS Git，Worker 能访问 S3 和 PyPI。
- 首次启动建议至少给本系统预留 8 CPU、19Gi 内存，以及 Worker 运行环境缓存所需磁盘。
  其他 Pod、Kubernetes 和操作系统的开销另外预留。

应用初始化表，不创建数据库或 Bucket。没有数据库时，由数据库管理员执行：

```sql
CREATE ROLE pyscripts_adm LOGIN;
\password pyscripts_adm
CREATE DATABASE pyscripts OWNER pyscripts_adm;
\connect pyscripts
GRANT USAGE, CREATE ON SCHEMA public TO pyscripts_adm;
```

## 2. 构建并准备镜像

基线固定为 **KubeRay 1.7.0、Ray 2.58.0、Python 3.12**。
网关依赖和 Ray 镜像必须同时更新版本，不单独升级。

使用 Podman，在项目根目录构建并推送到内网 HTTPS 镜像仓库。
镜像名为 `pyscripts/gateway`，下例以 x86_64 节点为目标；ARM64 节点改用 `linux/arm64`：

```bash
# 仓库需要认证时登录；匿名仓库跳过。
podman login 192.168.0.11:15000
podman build --layers --platform linux/amd64 -f Dockerfile \
  -t 192.168.0.11:15000/pyscripts/gateway:0.1.1 .
podman push 192.168.0.11:15000/pyscripts/gateway:0.1.1
```

登录、构建和推送使用同一个用户。Dockerfile 自动编译 Web UI，不需要另起前端容器。
构建使用锁定的 Python 依赖，发布 Hook 的内网包源不会成为默认构建包源。

仓库使用自签名证书时，将签发 CA（直接自签时为服务器证书）安装到
`/etc/containers/certs.d/192.168.0.11:15000/ca.crt`，然后重新登录。
临时上传可在 `podman login` 和 `podman push` 命令中添加 `--tls-verify=false`；
连接仍使用 HTTPS，但不校验证书。构建命令不需要此参数。

若拉取 Docker Hub 基础镜像超时，先确认构建主机能访问代理。WSL 使用 Windows 代理时，
Windows 仅监听 `127.0.0.1` 的端口可能无法从 WSL 访问；开启代理软件的 Allow LAN，
并使用从 WSL 可访问的 Windows IP。可通过 `ip route show default` 查找当前 NAT 网关。
验证代理时使用 `curl --noproxy '' --proxy http://代理IP:7890 -i https://registry-1.docker.io/v2/`，
返回 401 表示镜像仓库可访问；不要使用 `--noproxy '*'`，它会绕过代理。

将下例中的 `代理IP` 替换为已验证可达的地址（写法兼容 Bash/Fish）：

```bash
env HTTP_PROXY=http://代理IP:7890 HTTPS_PROXY=http://代理IP:7890 \
  http_proxy=http://代理IP:7890 https_proxy=http://代理IP:7890 \
  ALL_PROXY= all_proxy= \
  NO_PROXY=localhost,127.0.0.1,192.168.0.11 \
  no_proxy=localhost,127.0.0.1,192.168.0.11 \
  podman build --layers --platform linux/amd64 -f Dockerfile \
    -t 192.168.0.11:15000/pyscripts/gateway:0.1.1 .
```

Podman 默认将代理变量传入构建容器。如果使用构建主机环回地址上的代理，
在构建命令中加 `--network=host`，让构建阶段也能访问该端口。
代理变量无需写入 Dockerfile。详见 [Podman 构建代理配置](https://docs.podman.io/en/v4.9.3/markdown/podman-build.1.html#http-proxy)。

[kustomization.yaml](./k8s/base/kustomization.yaml) 已配置同一镜像地址：

```yaml
images:
  - name: pyscripts-gateway
    newName: 192.168.0.11:15000/pyscripts/gateway
    newTag: "0.1.1"
```

每次更新使用新 tag，避免同 tag 的旧缓存。需要认证的私有仓库可按下节在每个 K3s 节点
配置仓库认证；也可在 `pyscripts` namespace 创建拉取凭据，
并在 `deploy/k8s/base/control-plane.yaml` 的 Pod `spec.imagePullSecrets` 引用；
Ray 镜像若也使用私有仓库，在对应 Head/Worker Pod 模板中同样配置。
单节点 K3s 可用 `podman save --format docker-archive -o gateway.tar 镜像名` 导出镜像，
再用 `sudo k3s ctr images import gateway.tar` 导入；
多节点需每个节点导入，通常使用镜像仓库更方便。

Podman 的证书信任不会自动传递给 K3s。在每个拉取镜像的节点，将 CA 文件复制到
`/etc/rancher/k3s/registry-ca.crt`，并向 `/etc/rancher/k3s/registries.yaml` 合并：

```yaml
configs:
  "192.168.0.11:15000":
    auth:
      username: admin
      password: "实际仓库密码"
    tls:
      ca_file: /etc/rancher/k3s/registry-ca.crt
```

匿名拉取时省略 `auth`。配置文件含凭据，应执行
`sudo chmod 600 /etc/rancher/k3s/registries.yaml`。
若暂时没有 CA 文件，可将 `tls` 下的 `ca_file` 替换为 `insecure_skip_verify: true`，
对应本次 Podman 上传使用的跳过证书校验方式，连接仍为 HTTPS。
然后按节点角色重启 `k3s` 或 `k3s-agent`。HTTPS 是默认协议，不配置 HTTP endpoint。
证书还需覆盖访问 IP；若证书只覆盖域名，应统一使用证书中的仓库域名。
详见 [K3s 私有仓库配置](https://docs.k3s.io/installation/private-registry)。

除了网关镜像，节点还需能拉取 `docker.io/rayproject/ray:2.58.0-py312`
和 `quay.io/kuberay/operator:v1.7.0`。无法访问公共仓库时，在可访问公共仓库的构建机
执行以下命令（Podman 代理需先配置可达）：

```bash
podman pull docker.io/rayproject/ray:2.58.0-py312
podman tag docker.io/rayproject/ray:2.58.0-py312 \
  192.168.0.11:15000/pyscripts/ray:2.58.0-py312
podman push --tls-verify=false 192.168.0.11:15000/pyscripts/ray:2.58.0-py312
podman pull quay.io/kuberay/operator:v1.7.0
podman tag quay.io/kuberay/operator:v1.7.0 \
  192.168.0.11:15000/pyscripts/kuberay-operator:v1.7.0
podman push --tls-verify=false 192.168.0.11:15000/pyscripts/kuberay-operator:v1.7.0
```

然后将 `base/raycluster.yaml` 中 Head/Worker 的两处 `image` 改为
`192.168.0.11:15000/pyscripts/ray:2.58.0-py312`；下一节的 Helm 安装命令增加
`--set image.repository=192.168.0.11:15000/pyscripts/kuberay-operator --set image.tag=v1.7.0`。
以上两种辅助镜像尚未搬运；当前仓库已完成上传的是网关镜像。

## 3. 填写统一 .env 配置并安装 KubeRay

```bash
cp -n deploy/k8s/base/.env.example deploy/k8s/base/.env
chmod 600 deploy/k8s/base/.env
openssl rand -hex 32
```

将最后一条生成的随机值写入 `PYSCRIPTS_ADMIN_SESSION_SECRET`，保持它稳定。
编辑 `deploy/k8s/base/.env`，填写运行参数并替换数据库、S3、初始管理员密码等所有占位值。
这是与原生运行相同的 **dotenv 文件**：Kustomize 按文件保存到 Secret，
在应用和启动检查容器中只读挂载为 `/app/.env`，由应用的同一配置模型读取。
密码引号、注释及 JSON 数组写法保持一致，例如：

```dotenv
PYSCRIPTS_DATABASE_PASSWORD='password with spaces # and $characters'
PYSCRIPTS_PRIVATE_PIP_EXTRA_INDEX_URLS='["https://packages.example.com/simple"]'
```

原生从启动目录的 `.env` 读取，K8s 从容器工作目录 `/app/.env` 读取。
环境变量优先于文件值；Ray 认证模式和自动生成的 token 由部署配置注入环境，
无需在应用 `.env` 维护。不要 `source` 或提交此文件，也不要分享渲染后的 Secret 内容。
两种模板使用相同配置键，Ray 地址、节点标签策略及容器缓存/UI 路径按运行环境填写。
网关 Pod 设置 `spec.template.spec.enableServiceLinks: false`，使用 DNS 发现 Service。
否则 Kubernetes 为 `pyscripts-grpc` Service 自动注入的
`PYSCRIPTS_GRPC_PORT=tcp://...:50051` 会覆盖 `.env` 中的整数端口，导致
启动检查和主容器的配置校验失败。此设置同时适用于 initContainer 与主容器。

只使用分项数据库配置时不要额外设置 `PYSCRIPTS_DATABASE_URL`，它会覆盖分项。
数据库和 S3 地址必须从 Pod 可访问，不能填管理主机的 `localhost`。
S3 使用 API 端口，不是管理页面端口；下载 URL 中的地址必须同时被网关与 Worker 访问。

安装 Operator；已有 1.7.0 时跳过：

```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm upgrade --install kuberay-operator kuberay/kuberay-operator \
  --version 1.7.0 \
  --namespace kuberay-system --create-namespace \
  --wait --timeout 10m
kubectl wait --for=condition=Established crd/rayclusters.ray.io --timeout=120s
```

## 4. 手动启动服务

先按第 2 节配置节点仓库访问，按第 3 节安装 KubeRay 并填写凭据。
审阅 `deploy/k8s/base` 和选定的 overlay 后，手动执行下列命令。

### 内网直接访问

完成镜像和凭据配置后，执行：

```bash
kubectl kustomize deploy/k8s/overlays/lan >/dev/null
kubectl apply -k deploy/k8s/overlays/lan
kubectl -n pyscripts rollout status deployment/pyscripts --timeout=900s
kubectl -n pyscripts get raycluster,pods,svc
```

入口：

| 服务 | 内网入口 |
| --- | --- |
| Web UI + REST + API 文档 | `http://节点IP:30080`，文档路径 `/api-docs` |
| 原生 gRPC | `节点IP:30551` |
| Ray Client / Dashboard / GCS | 仅集群内，没有外部 NodePort |

LAN 模式使用明文 HTTP/gRPC，只适用于受控内网；公网和跨不可信网络使用下一节的 TLS 入口。
Web UI 与 REST 共用端口，不需要独立前端服务。

KubeRay 创建 token Secret `pyscripts-ray`，网关和启动检查通过 `secretKeyRef`
引用其 `auth_token`；认证模式为 token，不需要把 token 写入应用凭据文件。
网关启动前实际验证 Ray 认证连接，并等待 `default` Worker 可用。
数据库建表在应用启动时执行；凭据错误、依赖不可达会让启动/就绪检查失败，见排障章节。
认证机制见 [KubeRay token 配置](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/kuberay-auth.html)。

### K3s/Traefik 的 HTTPS 和 gRPC TLS

K3s 默认 Traefik 可以让两个域名共用外部 `443`。
编辑 `deploy/k8s/overlays/tls/ingress.yaml` 的两个真实域名和相应 `tls.hosts`，
将 DNS 指向入口地址，并准备覆盖两个域名的证书。
在 `deploy/k8s/base/.env` 设置 `PYSCRIPTS_PUBLIC_BASE_URL=https://实际Web域名`
及 `PYSCRIPTS_ADMIN_COOKIE_SECURE=true`。

```bash
kubectl create namespace pyscripts --dry-run=client -o yaml | kubectl apply -f -
kubectl -n pyscripts create secret tls pyscripts-tls \
  --cert=/实际路径/fullchain.pem --key=/实际路径/privkey.pem
kubectl apply -k deploy/k8s/overlays/tls
kubectl -n pyscripts rollout status deployment/pyscripts --timeout=900s
```

此模式两个应用 Service 都为 ClusterIP，仅通过 TLS Ingress 对外；
Web UI/REST 使用 Web 域名，gRPC 使用 gRPC 域名，均为 `443`。
gRPC Service 配置 `h2c` 上游，代理负责 TLS 终止；管理员 Cookie 的 Secure 属性由 `.env` 配置。
其他入口控制器需按其文档配置 gRPC 上游协议，不直接套用 Traefik 注解。
上游协议说明见 [Traefik Kubernetes Ingress 文档](https://doc.traefik.io/traefik/routing/providers/kubernetes-ingress/)。

只需集群内部 Service 时使用 `kubectl apply -k deploy/k8s/base`。

## 5. 资源、持久化和运行边界

| 容器 | CPU request / limit | 内存 request / limit | 说明 |
| --- | --- | --- | --- |
| 网关 | 500m / 2 | 512Mi / 2Gi | 单实例、单 Uvicorn worker，HTTP/gRPC/UI 共用进程 |
| 网关 Ray 启动检查 | 100m / 1 | 256Mi / 1Gi | initContainer，执行后退出，不与网关并行 |
| Ray Head | 1 / 2 | 4Gi / 4Gi | 业务逻辑 CPU 为 0；对象存储 256Mi |
| Autoscaler | 250m / 500m | 256Mi / 512Mi | 扩缩容管理 |
| 单个 Worker | 6 / 不设置 | 12Gi / 12Gi | 逻辑 CPU 6，对象存储 3Gi，任务内存调度预算 7Gi |

初始 1 个 Worker，最少 1 个，最多 10 个。起步总 requests 约 **7.75 CPU、16.75Gi**，
内存 limits 合计 **18.5Gi**；10 个 Worker 时 requests 约 **61.75 CPU、124.75Gi**，
内存 limits 合计 **126.5Gi**。Worker 未设置 CPU limit，因此不存在固定的总 CPU limit。
以上不含 Operator 和其他基础设施。
`maxReplicas=10` 是模板上限，应按实际节点容量降低或提高，不代表有这么多物理资源。

Worker 以 **6 CPU / 12Gi** 作为 IO Actor → Compute 链路的压测起点。
CPU request 供 Kubernetes 分配节点容量；省略 CPU limit 可利用节点的空闲 CPU，
避免 CPU 配额节流，但共享节点上的 CPU 竞争仍会影响运行速度。
显式设置 `num-cpus=6`，Ray 仍按 6 个逻辑 CPU 调度，不会因为能使用更多物理 CPU 而增加调度容量。
此做法符合 [Ray 官方资源配置建议](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html)。
更多 CPU 槽位有助于 Actor 与 Compute 共用一个 Worker，但不能保证同节点执行；
Actor 终身占用声明的 CPU，等待子任务时也不释放，需给 Compute 留出逻辑 CPU 容量。

Head/Worker 的 `/dev/shm` 上限为 384Mi/3584Mi，实际使用计入容器内存 limit。
Worker 的 12Gi 预算初步分为对象存储 3Gi、任务/Actor 堆内存 7Gi，以及约 2Gi 的 Ray 进程等开销；
`/dev/shm` 上限包含对象存储空间，不额外增加 3.5Gi 内存预算，后续按 Arrow 数据量和峰值内存调整。
Worker 的任务 `memory` 是调度预算，只有任务声明内存需求才参与该资源调度，
当前业务任务未声明该需求，因此 7Gi 不限制其并发或实际用量；总内存仍受 12Gi 容器限制。
改变 Worker CPU 时同步改 `num-cpus` 和 CPU request；如设置 CPU limit，也应一并核对。

网关临时目录和契约编译缓存使用 emptyDir，分别限制 2Gi；容器临时存储
request 1Gi / limit 6Gi。已发布 Artifact、Proto 和 Descriptor 存于 S3，
数据库保存业务状态，故网关不需要本地持久卷。Worker 的运行环境缓存上限 20Gi，
部署前保证节点磁盘空间，并观察临时存储压力。

```bash
kubectl get nodes -o custom-columns=NAME:.metadata.name,CPU:.status.allocatable.cpu,MEMORY:.status.allocatable.memory
kubectl describe nodes
kubectl -n pyscripts top pods --containers
```

`top` 需要 Metrics Server。按每个节点扣除其他 Pod、Head/网关等预算后的剩余容量，
估算 Worker 数 `min(floor(剩余 CPU/6), floor(剩余内存/12Gi))`，再汇总各节点。
预留满负载内存和必要的 CPU 余量，不只看空闲时使用量。

网关使用 **Recreate**：更新会先停止旧实例，因此有短暂不可用；
当前发布、Actor 管理和后台任务尚不支持多副本协调，不修改 replicas 或启用 HPA。
网关的 liveness 只检查进程，readiness 检查数据库；Ray/S3 仍需业务验收。
退出宽限期 90 秒，HTTP 优雅退出最多 60 秒，gRPC 宽限期 30 秒。

Ray 入站 NetworkPolicy 仅允许同集群 Ray Pod 和本 namespace 网关，
保留出站访问供 DNS、S3、PyPI 与 Autoscaler 使用。需要 CNI 支持 NetworkPolicy；
默认 K3s 支持，禁用网络策略控制器时应自行补充隔离。
认证、业务 API Token 和管理员登录分别保留。

## 6. 发布与验收

### 新增非 default 的底层 Worker 类型

在 `deploy/k8s/base/raycluster.yaml` 的 `spec.workerGroupSpecs` 中复制完整
`default-workers` 条目，新增一个 Worker Group。例如 `analytics` 类型，
在副本中修改以下字段，其余 `rayStartParams`、容器和卷配置按原条目保留：

```yaml
- groupName: analytics-workers
  replicas: 0
  minReplicas: 0
  maxReplicas: 3
  labels:
    pyscripts.worker-pool: analytics
  # rayStartParams、template 等字段从 default-workers 完整复制。
```

这是字段修改示例，不作为独立 RayCluster 文件应用。
`groupName` 是 KubeRay 的分组名称，`labels.pyscripts.worker-pool` 的值才是应用中的
Worker 类型名；每个分组的类型名应唯一。标签应放在 Worker Group 顶层的 `labels`，
KubeRay 会同步到 Ray 节点和 Pod，不要只添加 Pod metadata 标签。

如果类型需要不同系统依赖，修改该 Group 的容器镜像，但保持 Ray 2.58.0、Python 3.12
与 Head 一致。调整 CPU 时同时修改容器 CPU request 和 `rayStartParams.num-cpus`，
若设置了 CPU limit 也需一并核对；
调整内存时一并核对对象存储、任务调度内存和 `/dev/shm` 预算。
需要指定物理节点时，可给节点打标签，并在该 Group 的 `template.spec.nodeSelector` 配置匹配值。

保留 `.env` 中 `PYSCRIPTS_RAY_USE_LABEL_SELECTOR=true`，按选定入口重新手动
`kubectl apply -k deploy/k8s/overlays/lan`（TLS 模式使用相应目录）。
刷新 Web UI，在创建运行环境版本时将 Worker 类型选择为 `analytics`；
API 请求则显式传入 `worker_pool: analytics`。
环境验证、IO Actor 和 Compute Task 都按此类型的 Ray 标签调度。

当前 K8s 模式从 RayCluster 声明发现 Worker 类型，因此初始 0 个 Pod 时仍可选择；
运行环境验证和任务需求会由 Autoscaler v2 触发扩容，节点须有足够资源。
新增类型并保留 default 时不用改 `PYSCRIPTS_DEFAULT_WORKER_POOL`。
若移除 default、只使用新类型，将 `.env` 的 `PYSCRIPTS_DEFAULT_WORKER_POOL`
改为 `analytics`，并让该类型的 `replicas`/`minReplicas` 至少为 1，
使网关启动检查能找到可用 Worker；API 请求也需显式指定新类型。

标签调度说明见 [Ray label selector](https://docs.ray.io/en/latest/ray-core/scheduling/labels.html)。

### 服务发布与业务调用

```bash
curl --fail http://节点IP:30080/health/ready
kubectl -n pyscripts exec deployment/pyscripts -- pyscripts-db
kubectl -n pyscripts logs deployment/pyscripts -c wait-for-ray
```

Web UI 使用固定账号 `admin` 与配置的初始密码登录；初始密码只创建一次，
后续在 Web UI 修改。创建运行环境（Python 3.12、default Pool），验证后创建服务，
填写 HTTP/HTTPS Git 地址、跟踪分支并发布。
脚本按 [Hook 接入规范](../pyproject_interface.md) 声明接口和 runtime。

创建业务 API Token，再进行一次实际 REST 调用，例如：

```bash
read -r -s -p 'API Token: ' PYSCRIPTS_TEST_TOKEN
curl --fail-with-body 'http://节点IP:30080/v1/services/example/add' \
  -H "Authorization: Bearer ${PYSCRIPTS_TEST_TOKEN}" \
  -H 'Content-Type: application/json' --data '{"a":1,"b":2}'
unset PYSCRIPTS_TEST_TOKEN
```

替换 `example/add` 和参数为实际接口。TLS 部署使用 HTTPS 地址。
gRPC 从服务详情下载 Proto、生成对应语言的 Stub，附加
`metadata=(("authorization", "Bearer pst_..."),)`；TLS 客户端连接 gRPC 域名的 443。
在调用记录检查返回、执行状态和日志。**探针成功不等于完整 Ray/S3 链路通过。**

## 7. 更新和排障

修改 `deploy/k8s/base/.env` 或镜像 tag 后重新对相同目录执行 `kubectl apply -k`。
Kustomize 为保存完整 `.env` 的 Secret 生成带内容 hash 的名字，
Pod 的 Secret 卷引用变化会触发 Deployment 更新；挂载文件由新 Pod 重新读取。
旧的 hash Secret 不会自动删除；确认没有旧 Pod 引用后再清理，别删除 `pyscripts-ray`。
从旧配置迁移后，旧的 `pyscripts-runtime-*` ConfigMap 已不再使用，可确认无引用后清理。
不要切换 profile 时同时保留另一套入口；从 LAN 改 TLS 时同名 Service 会恢复 ClusterIP。

```bash
kubectl -n pyscripts get raycluster,pods,svc,ingress
kubectl -n pyscripts describe deployment pyscripts
kubectl -n pyscripts logs deployment/pyscripts -c wait-for-ray
kubectl -n pyscripts logs deployment/pyscripts -c pyscripts --tail=200
kubectl -n pyscripts get events --sort-by=.metadata.creationTimestamp
```

| 现象 | 检查 |
| --- | --- |
| ImagePullBackOff | 镜像/tag、仓库认证、节点下载网络 |
| Init 日志出现 grpc_port int_parsing，值为 tcp://... | 网关 Pod 模板须设置 enableServiceLinks: false，避免 Service 环境变量覆盖 .env |
| Init 卡住 | Ray Head/Worker Ready、认证 Secret、Ray 版本、资源不足 |
| 网关启动或 readiness 失败 | PostgreSQL 地址、账号权限、数据库已存在 |
| Ray Worker Pending | 节点可分配资源、Head 所在节点剩余预算、扩容上限 |
| 环境验证失败 | Worker 到 PyPI 的连接、依赖与 Python 版本 |
| Artifact 下载失败 | Worker 到 S3 API 的连接、Bucket、签名和证书 |
| REST/gRPC 401 | 业务 API Token 是否有效 |
| TLS gRPC 失败 | 域名/证书、客户端 TLS、Traefik 上游 h2c 配置 |

升级 Ray、认证或 Head 配置需维护窗口：排空业务，停网关，暂停 RayCluster，
待 Ray Pod 全部退出后应用配置，再恢复 Ray 和网关。Head 重启会丢失内存中的 Actor/任务状态。
首次从旧部署迁移时删除遗留 Ray 外部入口
`kubectl -n pyscripts delete service pyscripts-ray-client-lan --ignore-not-found`；
新部署没有这个 Service。

## 8. 原生测试和开发

```bash
uv sync --locked --dev
uv run --no-sync pytest
bun install --cwd ui --frozen-lockfile
bun run --cwd ui build
```

测试直接运行 Python，包含真正的本地 Ray Actor/Compute，以及独立 Uvicorn 进程的
HTTP、Web UI、管理员登录和 gRPC 监听验收。另有 token 认证的原生 Ray 服务测试，
确认缺少/错误 token 被拒绝，正确 token 与自动认证请求通过；不使用容器运行测试。

手工启动时复制项目根 `.env.example` 为 `.env`，配置测试数据库和 S3，
保持 `PYSCRIPTS_RAY_ADDRESS=local`、`PYSCRIPTS_RAY_USE_LABEL_SELECTOR=false`：

```bash
uv run pyscripts-db --init
uv run uvicorn pyscripts.api:app --host 127.0.0.1 --port 8000 --workers 1
```

原生远程调试需要时临时用 `kubectl port-forward` 暴露 Ray Client 到本机环回地址，
并在 Python 启动前导出 `RAY_AUTH_MODE=token` 和从 Secret 取得的 token。
日常测试无需依赖远程集群。

## 本轮交付验证（2026-10-10）

- 后端原生测试 138 项通过，包含独立网关进程和带认证的本地 Ray。
- Ray 认证检查覆盖缺失/错误 token 拒绝、正确 token 放行，以及网关实际
  `connect_ray()` 的包上传和远程任务执行（返回 42）。
- Web UI 类型检查及生产构建通过；Python 依赖锁一致性检查通过。
- internal、lan、tls 三套配置均实际执行 Kustomize 渲染，通过 Kubernetes 1.34
  和 KubeRay 1.7.0 CRD 结构校验，并检查镜像、Secret/ConfigMap 引用和对外端口。
- 验证运行配置和凭据变更会改变 Pod 模板，触发网关更新。
- 已使用 Podman 完成完整 Dockerfile 构建，镜像为 linux/amd64、非 root 用户，
  镜像配置中未保存构建代理变量。
- 已推送 `192.168.0.11:15000/pyscripts/gateway:0.1.0`，并从仓库按以下摘要成功拉取确认：
  `sha256:461e796770b40b8e54ac4a84c3cdb0ab6425c7f9fd06f0b3ee7b349c5df871ab`。

本轮按要求没有部署到真实集群；上线时仍需配置节点仓库证书及拉取凭据、
实际 PostgreSQL/S3 凭据，并按第 6 节完成业务验收。

## 统一 .env 配置验证（2026-10-14）

- 原生与 K8s 模板包含同一组 67 个配置键，仅环境相关默认值不同。
- 后端原生测试 138 项通过。
- base、lan、tls 分别渲染 10、10、12 个资源，通过 Kubernetes 1.34 和 KubeRay CRD 结构校验。
- 验证完整 `.env` 经 Secret 挂载后与原生读取结果一致，包含带空格/特殊字符的引号密码和 JSON 数组。
- 验证应用与 initContainer 共用 `.env`，修改文件会改变 Secret 卷引用并触发 Pod 更新。
- 新镜像 `192.168.0.11:15000/pyscripts/gateway:0.1.1` 已构建、推送并按摘要拉取确认：
  `sha256:d6fe7ca2d982539150f5d48dab9b0f49f320365f54f1885ca20dce21d1acf272`。

新镜像移除了会覆盖 `.env` 的 UI/缓存路径环境变量。K8s 配置已使用 0.1.1，
按前述手动步骤填写 `.env` 并应用 YAML；本轮未部署到真实集群。
