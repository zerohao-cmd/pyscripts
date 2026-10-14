# Kubernetes 配置示例

完整手动操作顺序见 [部署指南](../README.md)：节点镜像仓库配置、KubeRay 安装、
凭据填写、资源配置、Service/Ingress 选择和业务验收。
本目录提供可按实际环境修改的 YAML 与配置模板。

目标管理机无需 Git 或项目源码。可通过 SCP/SFTP 传输 `deploy` 目录，
保留 `base` 与 `overlays` 的相对路径；然后按指南逐项执行 kubectl/Helm 命令。
仅有 K3s 自带 CLI 时，将 kubectl 命令替换为 `sudo k3s kubectl`。

| 文件 | 配置内容 |
| --- | --- |
| `base/kustomization.yaml` | 应用资源引用、完整 `.env` 文件的 Secret 生成及网关镜像地址 |
| `base/.env.example` | 统一 dotenv 模板，包含运行参数和凭据；复制为 `base/.env` 后填写 |
| `base/control-plane.yaml` | 单实例 Gateway、`.env` 挂载、Ray 启动检查、资源限制和健康探针 |
| `base/raycluster.yaml` | Ray Head/Worker、Autoscaler、token 认证和只读 RBAC |
| `base/services.yaml` | REST/Web UI 与 gRPC Service |
| `base/network-policy.yaml` | Ray 入站网络规则 |
| `overlays/lan/kustomization.yaml` | 内网 NodePort 示例：30080、30551 |
| `overlays/tls/ingress.yaml` | K3s/Traefik TLS 入口示例：两个域名共享 443 |
| `overlays/tls/kustomization.yaml` | TLS 模式基础资源与 Ingress 引用 |

应用配置统一使用 `.env`：原生进程读取启动目录的 `.env`，K8s 将同一格式的文件
通过 Secret 只读挂载到 `/app/.env`，应用和 Ray 启动检查使用同一配置模型读取。
支持原生 dotenv 的引号、注释和 JSON 数组写法。
Ray token 仍由 KubeRay Secret 注入进程环境，无需写入 `.env`。
TLS 模式需在 `.env` 设置 `PYSCRIPTS_ADMIN_COOKIE_SECURE=true`。

网关镜像为 `192.168.0.11:15000/pyscripts/gateway:0.1.1`。
修改镜像地址、资源预算、Worker 上限、外部依赖和入口域名后再应用。
Ray Client、Dashboard、GCS 仅在集群内部访问。
