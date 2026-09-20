# pyscripts

`pyscripts` is a Ray-backed platform for hosting stateless Python service
revisions. Revisions sharing an immutable runtime profile can be hot-loaded
into the same actor while retaining isolated Python module namespaces.

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
- Operate services, revisions, SDK downloads, and invocation logs from a
  lightweight SolidJS console.

## Configuration

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
immutable route snapshot. Horizontal scaling should happen at the pod level.

Run `uv run pyscripts-db` without `--init` for a read-only connectivity and
table check.

The application creates its initial schema during startup. This is intended
only for the first implementation slice; versioned migrations should replace
`create_all` before production.

## Artifact contract

业务脚本从目录组织、打包、注册、发布、激活到调用的完整接入方式，以及如何在
`pyproject.toml` 中声明 HTTP、Compute/IO 和 gRPC 接口，见
[`pyproject_interface.md`](./pyproject_interface.md)。

An artifact is a ZIP file containing importable Python modules. Entrypoints use
`package.module:function`. Local project imports must be relative so that each
revision stays inside its generated module namespace.

HTTP request object keys are passed as flat keyword arguments:

```python
def add(x: int, y: int) -> int:
    return x + y
```

For backward compatibility, a handler that explicitly names its first argument
`context` receives the platform invocation context before its business
arguments.

`pyproject.toml` defines the interface, a logical Runtime Label reference,
and the project's Python/dependency constraints. Service registration stores
only the Git repository and tracking policy. For every new commit, the manager
checks that the referenced label satisfies `requires-python` and
`dependencies`; only then does it derive the Git-SHA revision, build the ZIP,
compute its digest, and upload the content-addressed artifact.

The Web console uses `GET /admin/services/{service_id}` for one consolidated
service view, including the active Revision and its endpoints. Git source,
tracking mode, and polling interval are updated with
`PATCH /admin/services/{service_id}`; the service name remains immutable because
it is part of public HTTP paths and generated SDK identity. Stopping a service
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
`data-default`, in `[tool.pyscript.runtime]`; `data-default@latest` is an
equivalent explicit spelling. The manager resolves its current active version
and stores that concrete `@vN` reference for scheduling and audit. Activating a
new label version first revalidates every tracking Revision. If all remain
compatible, new requests switch to the new environment while old IO Actors and
already submitted Compute Tasks drain on the previous execution snapshot.
Explicit `data-default@v2` references remain available for exceptional pinned
rollbacks and do not follow later activations.

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
POST /admin/runtime-labels
POST /admin/runtime-labels/{label_id}/versions
POST /admin/runtime-profile-versions/{version_id}/activate
POST /admin/runtime-profile-versions/{version_id}/retire
```

## Generated gRPC contract

The script declares which transports an endpoint supports. Proto files,
descriptors, contract versions, and the Python SDK are generated by the
platform:

```toml
[tool.pyscript]
spec_version = 1

[[tool.pyscript.endpoints]]
id = "add"
task_type = "compute"
entrypoint = "service:add"
io_type = ["rest", "grpc"]

[tool.pyscript.endpoints.x]
type = "Int64"

[tool.pyscript.endpoints.y]
type = "Int64"

[tool.pyscript.endpoints.response_schema]
type = "Int64"
```

The same flattened Python function handles both transports:

```python
def add(x: int, y: int) -> int:
    return x + y
```

`io_type` accepts `rest` and `grpc`; omitting it defaults to `["rest"]`.
REST wraps the flattened parameters in a JSON object, while generated gRPC maps
the request message fields directly back to keyword arguments. Clients use the
ordinary generated service Stub and protobuf messages.

At publication time the platform:

- generates `service.proto` and its `FileDescriptorSet` from endpoint metadata;
- preserves field numbers from the previous descriptor and emits `reserved`
  names/numbers for removed or type-changed fields;
- automatically bumps the contract major version for breaking changes and the
  minor version for compatible schema additions;
- builds an immutable Python wheel containing messages, type stubs, and the
  standard gRPC client Stub.

An unchanged interface produces the same descriptor and reuses the existing
contract and wheel. The initial implementation supports unary-unary RPCs. Closed
gRPC `Struct` schemas must set `additionalProperties = false`; nested `List` of
`List` is not supported.

## Generated Python SDK

When a revision declares `grpc` in `io_type`, registration:

1. derives and compiles Proto sources from the interface metadata;
2. computes a canonical `schema_digest`;
3. generates Python messages, type stubs, and gRPC client stubs;
4. builds an immutable pure-Python wheel;
5. records the contract and SDK before allowing revision activation.

Code-only revisions with the same schema digest reuse the existing contract and
wheel. Contract versions and package versions are assigned automatically and
never overwritten.

After activation, discover and download the contract with:

```text
GET /v1/services/{service_name}/grpc-contract
GET /v1/contracts/{contract_id}/descriptor.pb
GET /v1/contracts/{contract_id}/proto.zip
GET /v1/contracts/{contract_id}/python-sdk/{generated-wheel-name}.whl
```

## IO Actor and Compute Task scheduling

Execution is split by endpoint type:

- IO endpoints are packed into asynchronous profile Actors up to
  `ACTOR_MAX_IO`. Actor leases are atomic and unconsumed reservations expire.
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

Every invocation records its execution kind, artifact digest, runtime-profile
version, and environment digest. A retiring environment waits for both Actor
leases and Compute Task invocation snapshots to finish.

Python `sys.stdout` and `sys.stderr` output is isolated per invocation and
stored when execution finishes. The first implementation is intentionally
non-streaming: the HTTP/gRPC result and captured log chunks return from Ray as
one outcome, then the control plane commits the invocation status and logs in
the same transaction. Every chunk carries the UTC timestamp captured on the
Worker when that output was emitted; the Web console displays it in the
browser's local time with millisecond precision. Read logs with
`GET /admin/invocations/{invocation_id}/logs` or by clicking an invocation in
the Web console. `PYSCRIPTS_INVOCATION_LOG_MAX_BYTES` bounds retained output,
`PYSCRIPTS_INVOCATION_LOG_CHUNK_BYTES` controls database chunk size, and
`PYSCRIPTS_CAPTURE_STDERR` enables stderr capture. Native file-descriptor
writes and child-process output are outside this first version.

Compute endpoints may override the default task resources in their manifest:

```json
{
  "id": "forecast",
  "task_type": "compute",
  "entrypoint": "service:forecast",
  "num_cpus": 2,
  "num_gpus": 0
}
```
