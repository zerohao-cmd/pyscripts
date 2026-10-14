# pyscripts

`pyscripts` is a Ray-backed platform for hosting stateless Python service
revisions. Revisions sharing an immutable runtime profile can be hot-loaded
into the same actor while retaining isolated Python module namespaces.

日常使用见 [用户使用指南](./USER_GUIDE.md)，部署步骤见
[项目部署指南](./deploy/README.md)，脚本编写规则见
[Hook 接入规范](./pyproject_interface.md)。

## Current vertical slice

- Register a Git-backed service.
- Register an immutable revision and its endpoint manifest.
- Atomically activate a revision in PostgreSQL and drain the old revision.
- Invoke a stable HTTP route and dispatch it to a runtime-profile actor.
- Invoke native, strongly typed unary gRPC methods through one dynamic server handler.
- Verify and hot-load ZIP artifacts by SHA-256 digest.
- Track invocation state in PostgreSQL.
- Create versioned dependency labels, validate them in temporary Ray Runtime
  Environments, and migrate services that follow an active label without
  restarting the API.
- Operate services, revisions, Proto contract downloads, and invocation logs from a
  lightweight SolidJS console.

## Configuration

项目部署步骤见 [deploy/README.md](./deploy/README.md)：准备数据库和对象存储、
构建 Dockerfile 镜像、在 Kubernetes/K3s 启动网关和 Ray 集群、发布服务及验证调用。

Copy `.env.example` to `.env` and provide the database password locally. Do
not commit `.env`.

The configured PostgreSQL database must already exist. For the current test
environment the assumed database name is `pyscripts`; override it with
`PYSCRIPTS_DATABASE_NAME` if the server uses a different name.

For local Ray development without labeled KubeRay workers, set:

```dotenv
PYSCRIPTS_RAY_ADDRESS=local
PYSCRIPTS_RAY_USE_LABEL_SELECTOR=false
```

Install and run:

```bash
uv sync --dev
uv run pyscripts-db --init
uv run uvicorn pyscripts.api:app --reload
```

### Kubernetes deployment

正式部署统一使用 Kubernetes/K3s。Dockerfile 构建网关镜像（包含 Web UI），
Kustomize 部署网关、RayCluster、认证、Service 和健康探针。
PostgreSQL 和 S3 使用已有服务；具体步骤见 [部署指南](./deploy/README.md)。
应用配置统一使用 dotenv `.env`：原生从启动目录读取，K8s 通过 Secret
只读挂载到 `/app/.env`，两种方式使用相同语法和配置模型。

```bash
# Configure credentials and the image name/tag first; install KubeRay as documented.
cp -n deploy/k8s/base/.env.example deploy/k8s/base/.env
chmod 600 deploy/k8s/base/.env
kubectl apply -k deploy/k8s/overlays/lan
kubectl -n pyscripts rollout status deployment/pyscripts --timeout=900s
```

内网启动只开放 Web UI/REST 的 `30080` 和 gRPC 的 `30551`，Ray 端口不对外公开。
K3s/Traefik 的 TLS 入口使用 `kubectl apply -k deploy/k8s/overlays/tls`，HTTP 与 gRPC 通过各自域名共享 `443`。
部署交付为文档和 YAML 配置示例。无 Git 的管理机可通过文件传输获得 `deploy` 目录，
保留目录结构、修改配置后，按 [部署指南](./deploy/README.md) 逐项执行命令。
网关通过 Secret 引用 Ray token；Web UI 管理会话和业务 API Token 继续分别鉴权。
网关保持单实例、单 Uvicorn worker，使用 Recreate 更新。

### Native testing

测试直接运行 Python、HTTP/gRPC 和本地 Ray 进程：

```bash
uv sync --locked --dev
uv run --no-sync pytest
```

原生进程启动测试使用临时 SQLite 和独立端口，不改现有 PostgreSQL 或运行中的服务；
Ray 集成测试实际执行 Actor 和 Compute Task。
原生手工调试使用项目根目录 `.env.example`，本地 Ray 设置
`PYSCRIPTS_RAY_ADDRESS=local`、`PYSCRIPTS_RAY_USE_LABEL_SELECTOR=false`。
正式 K8s 凭据模板为 `deploy/k8s/base/.env.example`，不要混用两种环境文件。

Service registration and repository URL updates accept only HTTP or HTTPS Git
addresses, such as `https://git.example.com/team/orders.git`. SSH, `git://`,
`file://`, and local filesystem paths are rejected by the management API.

### Web console

The console uses Bun, SolidJS, and local CSS only. For frontend development,
run the API on port `8000`, then start Vite's proxy-enabled development server:

```bash
cd ui
bun install
bun run dev
```

Open `http://localhost:5173`. To serve the production console from FastAPI,
build it before starting the API:

```bash
cd ui
bun run build
cd ..
uv run uvicorn pyscripts.api:app
```

FastAPI mounts `ui/dist` at `/` after all API routes. Set
`PYSCRIPTS_SERVE_UI=false` to run API-only, or change
`PYSCRIPTS_UI_DIST_PATH` when the static bundle lives elsewhere.

HTTP and gRPC run in the same process on ports `8000` and `50051` by default.
Run one application worker per pod because each process owns its gRPC listener and
immutable route snapshot. Start with one control-plane replica; multi-replica HA
requires additional coordination for publication, pool scaling, and local limits.

Run `uv run pyscripts-db` without `--init` for a read-only connectivity and
table check.

### Administrator account

The management console has exactly one fixed account named `admin`; the login
form only asks for its password. `PYSCRIPTS_ADMIN_PASSWORD` bootstraps the
credential only when the database does not contain an administrator record.
After initialization, the Web console's password-change action stores a salted
scrypt hash in PostgreSQL, and the environment password no longer overrides it.
Changing the password increments the credential session version and immediately
invalidates every existing administrator cookie. Keep
`PYSCRIPTS_ADMIN_SESSION_SECRET` stable and provide at least 32 random characters.

### API access tokens

Business REST and gRPC invocations require a platform API Token. Administrators
create and revoke tokens from the Web console. A token grants access to every
active service and endpoint; there are no per-service permissions. Only the
token prefix and SHA-256 digest are stored, and the complete token is shown
once when it is created.

REST clients send the token as a Bearer credential:

```bash
curl -X POST 'http://localhost:8000/v1/services/example/add' \
  -H 'Authorization: Bearer pst_...' \
  -H 'Content-Type: application/json' \
  --data '{"x": 1, "y": 2}'
```

gRPC clients use the same value as call metadata:

```python
response = stub.Add(
    AddRequest(x=1, y=2),
    metadata=(("authorization", "Bearer pst_..."),),
)
```

Health checks, the public API reference, catalog, and Proto downloads remain
public. Management APIs continue to require the separate administrator session.
Always transport API Tokens over HTTPS/TLS outside a trusted development network.

The application creates its initial schema during startup. This is intended
only for the first implementation slice; versioned migrations should replace
`create_all` before production.

## Artifact contract

业务脚本从目录组织、打包、注册、发布、激活到调用的完整接入方式，以及如何在
Hook 装饰器中声明 HTTP、Compute/IO、内部调用和 gRPC 接口，见
[`pyproject_interface.md`](./pyproject_interface.md)。

An artifact is a ZIP file containing importable Python modules. `[tool.pyscript]`
selects one `module` or a list of `modules`; decorated functions become
`package.module:function` entrypoints. Relative imports are preferred, and artifact-local
absolute imports such as `from src.helpers import value` are translated into
the revision's private module namespace so concurrent revisions remain isolated.

HTTP request object keys are passed as flat keyword arguments:

```python
@hook(io_types=[IOType.Rest])
def add(x: Int64, y: Int64) -> Int64:
    return x + y
```

`pyproject.toml` defines the Hook module list, a logical Runtime Label reference,
and the project's Python/dependency constraints. Hook modules define the
interface through decorators and type annotations. Service registration stores
the Git repository, an optional tracked branch, and the tracking policy. An
unset branch follows the repository's default branch; a configured branch is
cloned explicitly with single-branch history. For every new commit, the manager
checks that the referenced label satisfies `requires-python` and
`dependencies`; only then does it derive the Git-SHA revision, build the ZIP,
compute its digest, and upload the content-addressed artifact.

The Web console uses `GET /admin/services/{service_id}` for one consolidated
service view, including the active Revision and its endpoints. Git source,
tracked branch, tracking mode, and polling interval are updated with
`PATCH /admin/services/{service_id}`; the service name remains immutable because
it is part of public HTTP paths and generated Proto identity. Stopping a service
preserves its active Revision and rejects new requests; starting it again with
`POST /admin/services/{service_id}/start` restores that same Revision without a
Git pull, build, or Artifact upload. Publishing while stopped creates a READY
Revision but does not implicitly start the service.

## Business artifact object storage

Production publication stores every business ZIP in an S3-compatible object
store under a content-addressed key:

```text
s3://<bucket>/<prefix>/sha256/<first-two-hex>/<sha256>.zip
```

The database stores this stable `s3://` reference, never an expiring URL. For
each invocation the control plane generates a short-lived presigned HTTPS URL.
The Ray worker downloads it only on a node-local cache miss and verifies the
SHA-256 before loading code. Object-store credentials therefore stay in the
control-plane Pod and are not distributed to Ray workers.

Generated Proto source bundles and the internal canonical descriptor are also
stored by digest under `contracts/proto/sha256/...` and
`contracts/descriptors/sha256/...`. The contract build directory is only a
scratch workspace; a process restart does not regenerate published contracts.

Enable the store with:

```text
PYSCRIPTS_OBJECT_STORE_ENABLED=true
PYSCRIPTS_OBJECT_STORE_ENDPOINT_URL=http://object-store.example.internal:9000
PYSCRIPTS_OBJECT_STORE_REGION=us-east-1
PYSCRIPTS_OBJECT_STORE_BUCKET=pyscripts
PYSCRIPTS_OBJECT_STORE_ARTIFACT_PREFIX=pyscripts/artifacts
PYSCRIPTS_OBJECT_STORE_ACCESS_KEY_ID=...
PYSCRIPTS_OBJECT_STORE_SECRET_ACCESS_KEY=...
PYSCRIPTS_OBJECT_STORE_ADDRESSING_STYLE=path
PYSCRIPTS_OBJECT_STORE_VERIFY_SSL=true
PYSCRIPTS_OBJECT_STORE_PRESIGN_TTL_SECONDS=3600
```

The endpoint embedded in a presigned URL must be resolvable and reachable from
Ray Worker Pods. For an in-cluster MinIO-compatible service, use its Kubernetes
service DNS name rather than `localhost` or a control-plane-only address.

## Versioned runtime profiles

The logical label (for example `data-default`) is stable, while every dependency
change creates an immutable reference such as `data-default@v2`. Dependencies
must be exactly pinned (`package==1.2.3`); URL dependencies also require a
SHA-256 fragment.

Creating a version runs a temporary Ray task with the proposed `runtime_env`,
executes `pip check`, verifies configured imports, and records the full resolved
environment plus a content digest. The first valid version is activated
automatically. Later versions remain `READY` until explicitly activated.

Business projects normally reference only the logical label, such as
`runtime = "data-default"` in `[tool.pyscript]`; `data-default@latest` is an
equivalent explicit spelling. The manager resolves its current active version
and stores that concrete `@vN` reference for scheduling and audit. Activating a
new label version first revalidates every tracking Revision. If all remain
compatible, new requests switch to the new environment while old IO Actors and
already submitted Compute Tasks drain on the previous execution snapshot.
Explicit `data-default@v2` references remain available for exceptional pinned
rollbacks and do not follow later activations.

Each runtime-label version also selects a package source. In the Web console,
choose **Default / official** or **Custom PyPI server** and enter a URL reachable
from every Ray worker, for example `http://192.168.0.11:8080/simple/`. The URL
is stored on the immutable environment version, and the control plane applies
it during temporary validation, interface discovery, and execution. For plain
HTTP URLs, the host is added to pip's trusted hosts. Index URLs entered in the
Web console cannot contain credentials.

The existing `private` choice uses the platform-managed private index. Configure
it on the control plane (or through a Kubernetes Secret):

```dotenv
PYSCRIPTS_PRIVATE_PIP_INDEX_URL=https://user:token@pypi.example.com/simple
PYSCRIPTS_PRIVATE_PIP_EXTRA_INDEX_URLS=["https://mirror.example.com/simple"]
PYSCRIPTS_PRIVATE_PIP_TRUSTED_HOSTS=[]
```

For `private`, the database stores only the choice and environment digest;
credentials are injected into Ray Runtime Env only when validating or starting
work. Custom index URLs are stored and displayed in the Web console. Prefer TLS
outside controlled development networks.

## Git webhooks

For a service whose tracking mode is `webhook`, open its Web console settings
and choose **Generate URL**. The returned URL contains a high-entropy token and
is shown only once; the database stores only its SHA-256 hash. Configure that
URL as a repository webhook with JSON payloads and Push events enabled:

- Gitea: repository **Settings → Webhooks → Add Webhook → Gitea**.
- GitLab: project **Settings → Webhooks**, paste the URL and select **Push events**.

Set `PYSCRIPTS_PUBLIC_BASE_URL` when the API is behind an ingress or reverse
proxy, otherwise generated URLs use the incoming admin request's origin. A
token can be rotated or disabled from the same settings panel. Push delivery
returns `202` immediately and synchronizes the repository in the background;
when a branch is configured, Push events for every other branch are ignored.
The currently active revision is unchanged if fetching, metadata validation,
runtime compatibility, contract generation, or artifact publication fails.

Revision, Artifact URI, and digest are generated by the manager and are not
publication inputs. Interface metadata and runtime compatibility are read from
the tracked Git commit before any Artifact is uploaded.

The KubeRay worker can use the standard `rayproject/ray:2.58.0-py312` image.
The control plane uploads the small `pyscripts` executor module with Ray and
installs only the selected Python-profile dependencies through Runtime Env.

Scheduling has two distinct layers:

- K8s/KubeRay owns immutable container-level worker types. Workers advertise
  them through the Ray label configured by
  `PYSCRIPTS_RAY_WORKER_POOL_LABEL_KEY` (default `pyscripts.worker-pool`). The
  Web/API can discover and select these values but cannot create or edit them.
- Users own versioned Python runtime labels. Each version references one
  discovered `worker_pool` and carries its own Ray `runtime_env` plus
  `environment_digest`.

IO actors are pooled by Python environment digest. Compute tasks prefer nodes
already warmed for the same digest, with a fallback that still enforces the
container worker-pool label.

Set `PYSCRIPTS_KUBERAY_CLUSTER_NAME` when the control plane runs in Kubernetes.
It will read `spec.workerGroupSpecs[].labels` from that RayCluster, so worker
types remain visible while scaled to zero. The service account needs `get` on
`rayclusters.ray.io` in `PYSCRIPTS_KUBERNETES_NAMESPACE`. Without that setting,
the catalog falls back to labels observed on currently live Ray nodes.

Declare each container worker type on its KubeRay worker group (KubeRay 1.5+):

```yaml
spec:
  workerGroupSpecs:
    - groupName: default-workers
      minReplicas: 0
      maxReplicas: 10
      labels:
        pyscripts.worker-pool: default
      template:
        spec:
          containers:
            - name: ray-worker
              image: rayproject/ray:2.58.0-py312
```

The control-plane service account only needs read access:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pyscripts-worker-pool-reader
rules:
  - apiGroups: ["ray.io"]
    resources: ["rayclusters"]
    verbs: ["get"]
```

Management endpoints:

```text
GET  /admin/runtime-labels
GET  /admin/worker-pools
GET  /admin/actor-pools
POST /admin/runtime-labels
POST /admin/runtime-labels/{label_id}/versions
POST /admin/runtime-profile-versions/{version_id}/activate
POST /admin/runtime-profile-versions/{version_id}/retire
```

`GET /admin/actor-pools` reports each Python environment pool as Cold, Warm,
or Hot and includes desired/ready/creating Actor counts, IO lease totals,
recent request rate, capacity, and per-Actor state. Reading a Cold pool does
not start an Actor.

## Generated gRPC contract

The script declares which transports an endpoint supports. Proto files,
descriptors, and contract versions are generated by the platform:

```toml
[tool.pyscript]
spec_version = 2
runtime = "default"
module = "service"
```

The same flattened Python function handles both transports:

```python
from sc_hook import Int64, IOType, hook

@hook(io_types=[IOType.Rest, IOType.Grpc])
def add(x: Int64, y: Int64) -> Int64:
    return x + y
```

The decorator accepts `IOType.Rest`, `IOType.Grpc`, and internal-only
`IOType.Inner` transports.
REST wraps the flattened parameters in a JSON object, while generated gRPC maps
the request message fields directly back to keyword arguments. Clients use the
downloaded Proto to generate the ordinary service Stub and protobuf messages
for their language and toolchain.

At publication time the platform:

- generates `service.proto` and its `FileDescriptorSet` from endpoint metadata;
- preserves field numbers from the previous descriptor and emits `reserved`
  names/numbers for removed or type-changed fields;
- automatically bumps the contract major version for breaking changes and the
  minor version for compatible schema additions;
- builds a deterministic Proto source ZIP and persists it in object storage.

An unchanged interface produces the same descriptor and reuses the existing
contract and Proto bundle. The initial implementation supports unary-unary RPCs. Closed
gRPC `Struct` schemas must set `additionalProperties = false`; nested `List` of
`List` is not supported.

## Published Proto contract

When a revision declares `grpc` in `io_type`, registration:

1. derives and compiles Proto sources from the interface metadata;
2. computes a canonical `schema_digest`;
3. builds a deterministic Proto source bundle;
4. stores the Proto bundle and internal canonical descriptor by digest in the
   configured object store;
5. records the contract before allowing revision activation.

Code-only revisions with the same schema digest reuse the existing contract and
Proto bundle. Contract versions are assigned automatically and never overwritten.

After activation, discover and download the contract with:

```text
GET /v1/services/{service_name}/grpc-contract
GET /v1/contracts/{contract_id}/proto.zip
```

The platform intentionally does not publish a language-specific SDK. Generate
client code from the downloaded Proto with `protoc`, `grpc_tools.protoc`, Buf,
or the standard generator for the target language.

## IO Actor and Compute Task scheduling

Execution is split by endpoint type:

- IO endpoints are packed into asynchronous profile Actors up to
  `ACTOR_MAX_IO`. Actor leases are atomic and unconsumed reservations expire.
- IO Actor scaling is entirely system-managed; services cannot select a
  scheduling mode. A rolling request-rate window, observed execution duration,
  and current concurrency determine demand. Busy Actors are Hot, while one
  ready idle Actor is kept as a Warm transition buffer for Hot profiles.
- Scale-up is immediate: once a Hot Actor reaches
  `ACTOR_TARGET_IO_CONCURRENCY`, the Warm Actor is promoted and another Warm
  spare is prepared. Scale-down is delayed: excess Actors use
  `ACTOR_HOT_IDLE_TIMEOUT_SECONDS`; the final Warm Actor uses the longer
  `ACTOR_WARM_IDLE_TIMEOUT_SECONDS` and then the profile scales to zero.
- Compute endpoints run as stateless Ray Tasks. Ray schedules their CPU/GPU
  resources directly; `COMPUTE_MAX_PENDING_PER_PROFILE` bounds the local
  submission queue.
- Both execution paths use the same immutable runtime-profile version and Ray
  node label selector.
- A stable generic Compute Task receives only the execution snapshot and
  parameters. Service source is not captured in the remote-function closure.
- Artifact ZIPs are downloaded once per worker node into a SHA-256-addressed
  cache with a cross-process file lock. Worker processes reuse the verified
  archive and extracted files.
- Compute service modules are unloaded after every invocation so module globals
  cannot carry state into the next request. Dependencies remain in the immutable
  Runtime Env.
- Cancelling or timing out a Compute invocation cancels its Ray ObjectRef.
- Hook IO endpoints may `await target.remote_call(...)`. Calls remain pinned to
  the current Revision: Compute targets become nested Ray Tasks, while IO
  targets stay asynchronous inside the current Actor. Outside Ray the same
  code runs locally for normal script development.

Every invocation records its execution kind, artifact digest, runtime-profile
version, and environment digest. A retiring environment waits for both Actor
leases and Compute Task invocation snapshots to finish.

Python `sys.stdout` and `sys.stderr` output is isolated per invocation and
stored when execution finishes. The first implementation is intentionally
non-streaming: the HTTP/gRPC result and captured log chunks return from Ray as
one outcome, then the control plane commits the invocation status and logs in
the same transaction. REST returns status and logs in its invocation envelope;
generated gRPC responses expose the same data through the strongly typed
`execution` field. This response envelope is mandatory for generated contracts;
upgrading a legacy generated response without `execution` is a breaking contract
change and requires publishing a new revision. Every chunk carries the UTC timestamp captured on the
Worker when that output was emitted; the Web console displays it in the
browser's local time with millisecond precision. Read logs with
`GET /admin/invocations/{invocation_id}/logs` or by clicking an invocation in
the Web console. Each service endpoint has an independently configurable
recent-invocation retention count; reducing it prunes the oldest completed
invocations without touching running work. `PYSCRIPTS_INVOCATION_LOG_MAX_BYTES`
bounds output captured for one invocation,
`PYSCRIPTS_INVOCATION_LOG_CHUNK_BYTES` controls database chunk size, and
`PYSCRIPTS_CAPTURE_STDERR` enables stderr capture. Native file-descriptor
writes and child-process output are outside this first version.

Compute endpoints may override the default task resources in their decorator:

```python
@hook(io_types=[IOType.Inner], num_cpus=2, num_gpus=0)
def forecast(data: list[Float]) -> Float:
    ...
```
