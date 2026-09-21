import {
  For,
  Show,
  createEffect,
  createMemo,
  createSignal,
  onCleanup,
  onMount,
  type Component,
  type JSX,
} from "solid-js";

import { ApiError, api } from "./api";
import { Icon, type IconName } from "./icons";
import type {
  Contract,
  CreateServiceInput,
  Invocation,
  InvocationLog,
  Revision,
  RuntimeLabel,
  RuntimeProfileInput,
  RuntimeProfileVersion,
  Service,
  ServiceDetail,
  UpdateServiceInput,
  WebhookConfig,
  WorkerPool,
} from "./types";

type View = "overview" | "services" | "profiles" | "activity";
type Toast = { message: string; tone: "success" | "danger" };

const viewLabels: Record<View, { title: string; kicker: string }> = {
  overview: { title: "运行概览", kicker: "CONTROL PLANE" },
  services: { title: "服务与版本", kicker: "SERVICE REGISTRY" },
  profiles: { title: "运行环境", kicker: "RUNTIME PROFILES" },
  activity: { title: "调用记录", kicker: "INVOCATION LOG" },
};

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return "发生未知错误";
}

function shortId(value: string | null | undefined, length = 10): string {
  if (!value) return "—";
  return value.length > length ? `${value.slice(0, length)}…` : value;
}

function formatDate(value: string | null | undefined): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function duration(invocation: Invocation): string {
  if (!invocation.started_at) return "—";
  const end = invocation.finished_at
    ? new Date(invocation.finished_at).getTime()
    : Date.now();
  const elapsed = Math.max(0, end - new Date(invocation.started_at).getTime());
  if (elapsed < 1000) return `${elapsed} ms`;
  return `${(elapsed / 1000).toFixed(elapsed < 10_000 ? 2 : 1)} s`;
}

function formatLogTime(value: string): string {
  const date = new Date(value);
  return `${date.toLocaleTimeString("zh-CN", { hour12: false })}.${String(date.getMilliseconds()).padStart(3, "0")}`;
}

function logContent(value: string): string {
  return value.replace(/\r?\n$/, "");
}

const endpointMetadataFields = new Set([
  "id",
  "task_type",
  "entrypoint",
  "io_type",
  "response_schema",
  "grpc",
  "num_cpus",
  "num_gpus",
]);

function endpointParameters(endpoint: Revision["endpoints"][number]): string[] {
  return Object.keys(endpoint).filter((key) => !endpointMetadataFields.has(key));
}

function grpcMethodName(service: string, method: string): string {
  const serviceName = service.split(".").filter(Boolean).at(-1) ?? service;
  return `${serviceName}/${method}`;
}

const StatusBadge: Component<{ value: string }> = (props) => {
  const tone = () => {
    if (["ACTIVE", "SUCCEEDED", "READY"].includes(props.value)) return "good";
    if (["FAILED", "TIMED_OUT"].includes(props.value)) return "bad";
    if (["RUNNING", "BUILDING", "DRAINING", "VALIDATING", "RETIRING"].includes(props.value)) return "warn";
    return "neutral";
  };
  return <span class={`status status--${tone()}`}>{props.value}</span>;
};

const EmptyState: Component<{
  icon: IconName;
  title: string;
  detail: string;
  action?: JSX.Element;
}> = (props) => (
  <div class="empty-state">
    <span class="empty-state__icon"><Icon name={props.icon} size={22} /></span>
    <strong>{props.title}</strong>
    <p>{props.detail}</p>
    {props.action}
  </div>
);

const Modal: Component<{
  title: string;
  eyebrow: string;
  onClose: () => void;
  children: JSX.Element;
}> = (props) => {
  const onKey = (event: KeyboardEvent) => {
    if (event.key === "Escape") props.onClose();
  };
  onMount(() => window.addEventListener("keydown", onKey));
  onCleanup(() => window.removeEventListener("keydown", onKey));
  return (
    <div
      class="modal-backdrop"
      onMouseDown={(event) => {
        if (event.currentTarget === event.target) props.onClose();
      }}
    >
      <section class="modal" role="dialog" aria-modal="true" aria-label={props.title}>
        <header class="modal__header">
          <div>
            <span class="eyebrow">{props.eyebrow}</span>
            <h2>{props.title}</h2>
          </div>
          <button class="icon-button" type="button" onClick={props.onClose} aria-label="关闭">
            <Icon name="x" />
          </button>
        </header>
        {props.children}
      </section>
    </div>
  );
};

const CreateServiceModal: Component<{
  onClose: () => void;
  onCreated: (service: Service) => void;
}> = (props) => {
  const [name, setName] = createSignal("");
  const [gitUrl, setGitUrl] = createSignal("");
  const [tracking, setTracking] = createSignal<CreateServiceInput["tracking_mode"]>("manual");
  const [interval, setInterval] = createSignal("60");
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal("");
  const submit: JSX.EventHandler<HTMLFormElement, SubmitEvent> = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const body: CreateServiceInput = {
        name: name().trim(),
        git_url: gitUrl().trim(),
        tracking_mode: tracking(),
      };
      if (tracking() === "poll") body.check_interval_seconds = Number(interval());
      props.onCreated(await api.createService(body));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal eyebrow="NEW SERVICE" title="注册脚本服务" onClose={props.onClose}>
      <form class="form-stack" onSubmit={submit}>
        <label class="field">
          <span>服务名称</span>
          <input autofocus required pattern="[a-z][a-z0-9_-]{1,63}" value={name()} onInput={(event) => setName(event.currentTarget.value)} placeholder="order-service" />
          <small>使用小写字母、数字、下划线或连字符</small>
        </label>
        <label class="field">
          <span>Git 仓库</span>
          <input required type="url" value={gitUrl()} onInput={(event) => setGitUrl(event.currentTarget.value)} placeholder="https://git.example.com/team/orders.git" />
        </label>
        <div class="field-grid">
          <label class="field">
            <span>跟踪方式</span>
            <select value={tracking()} onChange={(event) => setTracking(event.currentTarget.value as CreateServiceInput["tracking_mode"])}>
              <option value="manual">手动发布</option>
              <option value="poll">定时检查</option>
              <option value="webhook">Webhook</option>
            </select>
          </label>
          <Show when={tracking() === "poll"}>
            <label class="field">
              <span>检查间隔（秒）</span>
              <input type="number" min="10" value={interval()} onInput={(event) => setInterval(event.currentTarget.value)} />
            </label>
          </Show>
        </div>
        <div class="banner">
          <strong>环境随代码版本管理</strong>
          <span>发布时从 pyproject.toml 读取逻辑环境标签，解析活动版本并校验 Python 与第三方依赖。</span>
        </div>
        <Show when={error()}><div class="form-error">{error()}</div></Show>
        <footer class="modal__footer">
          <button class="button button--ghost" type="button" onClick={props.onClose}>取消</button>
          <button class="button button--primary" type="submit" disabled={busy()}>
            <Show when={!busy()} fallback="正在注册…"><Icon name="plus" />注册服务</Show>
          </button>
        </footer>
      </form>
    </Modal>
  );
};

const ServiceSettingsModal: Component<{
  service: Service;
  onClose: () => void;
  onUpdated: (service: ServiceDetail) => void;
}> = (props) => {
  const [gitUrl, setGitUrl] = createSignal(props.service.git_url);
  const [tracking, setTracking] = createSignal<Service["tracking_mode"]>(
    props.service.tracking_mode,
  );
  const [interval, setInterval] = createSignal(
    String(props.service.check_interval_seconds ?? 60),
  );
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal("");
  const [webhook, setWebhook] = createSignal<WebhookConfig | null>(null);
  const [webhookBusy, setWebhookBusy] = createSignal(false);

  onMount(() => {
    void api.serviceWebhook(props.service.id)
      .then(setWebhook)
      .catch((caught) => setError(errorMessage(caught)));
  });

  const rotateWebhook = async () => {
    setWebhookBusy(true);
    setError("");
    try {
      setWebhook(await api.rotateServiceWebhook(props.service.id));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setWebhookBusy(false);
    }
  };

  const disableWebhook = async () => {
    setWebhookBusy(true);
    setError("");
    try {
      setWebhook(await api.disableServiceWebhook(props.service.id));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setWebhookBusy(false);
    }
  };

  const copyWebhook = async () => {
    const url = webhook()?.url;
    if (url) await navigator.clipboard.writeText(url);
  };

  const submit: JSX.EventHandler<HTMLFormElement, SubmitEvent> = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      const body: UpdateServiceInput = {
        git_url: gitUrl().trim(),
        tracking_mode: tracking(),
        check_interval_seconds:
          tracking() === "poll" ? Number(interval()) : null,
      };
      props.onUpdated(await api.updateService(props.service.id, body));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal eyebrow={props.service.name} title="服务设置" onClose={props.onClose}>
      <form class="form-stack" onSubmit={submit}>
        <label class="field">
          <span>服务名称</span>
          <input value={props.service.name} disabled />
          <small>服务名关联固定调用路径与 Proto 契约，创建后不可修改。</small>
        </label>
        <label class="field">
          <span>Git 仓库</span>
          <input required type="url" value={gitUrl()} onInput={(event) => setGitUrl(event.currentTarget.value)} />
        </label>
        <div class="field-grid">
          <label class="field">
            <span>更新策略</span>
            <select value={tracking()} onChange={(event) => setTracking(event.currentTarget.value as Service["tracking_mode"])}>
              <option value="manual">手动同步</option>
              <option value="poll">定时检查</option>
              <option value="webhook">Webhook</option>
            </select>
          </label>
          <Show when={tracking() === "poll"}>
            <label class="field">
              <span>检查间隔（秒）</span>
              <input required type="number" min="10" value={interval()} onInput={(event) => setInterval(event.currentTarget.value)} />
            </label>
          </Show>
        </div>
        <div class="banner">
          <strong>设置只影响后续更新</strong>
          <span>修改 Git 地址或跟踪策略不会改变当前活动 Revision；下一次同步成功后才切换代码。</span>
        </div>
        <div class="webhook-config">
          <strong>Git Webhook</strong>
          <span>保存 Webhook 跟踪策略后，将下方地址配置为 Gitea 或 GitLab 仓库的 Push Webhook。</span>
          <Show
            when={webhook()?.url}
            fallback={<span>{webhook()?.enabled ? "令牌已经配置；如地址遗失，请轮换令牌生成新地址。" : "尚未生成 Webhook 地址。"}</span>}
          >
            <input class="mono" readonly value={webhook()!.url!} onFocus={(event) => event.currentTarget.select()} />
          </Show>
          <div class="contract-actions">
            <button class="button button--outline button--small" type="button" disabled={webhookBusy()} onClick={() => void rotateWebhook()}>
              <Icon name="refresh" size={15} />{webhook()?.enabled ? "轮换令牌" : "生成地址"}
            </button>
            <Show when={webhook()?.url}>
              <button class="button button--outline button--small" type="button" onClick={() => void copyWebhook()}><Icon name="copy" size={15} />复制地址</button>
            </Show>
            <Show when={webhook()?.enabled}>
              <button class="button button--danger button--small" type="button" disabled={webhookBusy()} onClick={() => void disableWebhook()}>停用</button>
            </Show>
          </div>
          <small>令牌只在生成时返回；平台数据库仅保存 SHA-256 哈希。</small>
        </div>
        <Show when={error()}><div class="form-error">{error()}</div></Show>
        <footer class="modal__footer">
          <button class="button button--ghost" type="button" onClick={props.onClose}>取消</button>
          <button class="button button--primary" type="submit" disabled={busy()}>
            <Show when={!busy()} fallback="正在保存…"><Icon name="check" />保存设置</Show>
          </button>
        </footer>
      </form>
    </Modal>
  );
};

const RuntimeProfileModal: Component<{
  label?: RuntimeLabel | null;
  workerPools: WorkerPool[];
  onClose: () => void;
  onCreated: (profile: RuntimeProfileVersion) => void;
}> = (props) => {
  const [name, setName] = createSignal("");
  const [pythonVersion, setPythonVersion] = createSignal("3.12");
  const currentPool = () => props.label?.versions.find(
    (version) => version.id === props.label?.active_version_id,
  )?.worker_pool;
  const [workerPool, setWorkerPool] = createSignal(
    currentPool() ?? props.workerPools[0]?.name ?? "",
  );
  const [dependencies, setDependencies] = createSignal("");
  const [imports, setImports] = createSignal("");
  const activeProfile = () => props.label?.versions.find(
    (version) => version.id === props.label?.active_version_id,
  );
  const [pipSource, setPipSource] = createSignal<RuntimeProfileInput["pip_source"]>(
    activeProfile()?.pip_source ?? "default",
  );
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal("");

  const lines = (value: string) =>
    value.split("\n").map((item) => item.trim()).filter(Boolean);

  const submit: JSX.EventHandler<HTMLFormElement, SubmitEvent> = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    const body: RuntimeProfileInput = {
      python_version: pythonVersion().trim(),
      worker_pool: workerPool(),
      dependencies: lines(dependencies()),
      import_checks: lines(imports()),
      pip_source: pipSource(),
    };
    try {
      const profile = props.label
        ? await api.createRuntimeVersion(props.label.id, body)
        : await api.createRuntimeLabel({ name: name().trim(), ...body });
      props.onCreated(profile);
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      eyebrow={props.label ? props.label.name : "NEW RUNTIME LABEL"}
      title={props.label ? "创建环境版本" : "创建运行环境"}
      onClose={props.onClose}
    >
      <form class="form-stack" onSubmit={submit}>
        <Show when={!props.label}>
          <label class="field">
            <span>标签名称</span>
            <input autofocus required pattern="[a-z][a-z0-9_-]{1,63}" value={name()} onInput={(event) => setName(event.currentTarget.value)} placeholder="data-default" />
            <small>服务引用稳定标签名；每次修改依赖会生成不可变的新版本。</small>
          </label>
        </Show>
        <div class="field-grid">
          <label class="field">
            <span>Python</span>
            <input required pattern="\d+\.\d+" value={pythonVersion()} onInput={(event) => setPythonVersion(event.currentTarget.value)} />
          </label>
          <label class="field">
            <span>Worker 类型</span>
            <select required value={workerPool()} onChange={(event) => setWorkerPool(event.currentTarget.value)}>
              <option value="" disabled>选择 K8s 提供的 Worker</option>
              <For each={props.workerPools}>{(pool) => (
                <option value={pool.name}>{pool.name} · {pool.node_count} nodes</option>
              )}</For>
            </select>
            <small>由 K8s/KubeRay 定义，只能选择，不能在这里修改。</small>
          </label>
        </div>
        <label class="field">
          <span>依赖锁定（每行一个）</span>
          <textarea class="code-editor compact-editor" rows="7" value={dependencies()} onInput={(event) => setDependencies(event.currentTarget.value)} placeholder={"httpx==0.28.1\npydantic==2.11.7"} spellcheck={false} />
          <small>PyPI 依赖必须使用精确版本；URL 依赖必须包含 #sha256。</small>
        </label>
        <label class="field">
          <span>PyPI 来源</span>
          <select value={pipSource()} onChange={(event) => setPipSource(event.currentTarget.value as RuntimeProfileInput["pip_source"])}>
            <option value="default">默认 / 官方源</option>
            <option value="private">平台私有源</option>
          </select>
          <small>私有源地址和凭据由控制面环境变量配置，不会写入项目仓库或接口响应。</small>
        </label>
        <label class="field">
          <span>导入校验（每行一个）</span>
          <textarea class="code-editor compact-editor" rows="4" value={imports()} onInput={(event) => setImports(event.currentTarget.value)} placeholder={"httpx\npydantic"} spellcheck={false} />
        </label>
        <Show when={error()}><div class="form-error">{error()}</div></Show>
        <footer class="modal__footer">
          <button class="button button--ghost" type="button" onClick={props.onClose}>取消</button>
          <button class="button button--primary" type="submit" disabled={busy() || props.workerPools.length === 0}>
            <Show when={!busy()} fallback="正在创建临时 Ray 环境并校验…"><Icon name="plus" />创建并校验</Show>
          </button>
        </footer>
      </form>
    </Modal>
  );
};

const PublishRevisionModal: Component<{
  service: Service;
  onClose: () => void;
  onPublished: () => void;
}> = (props) => {
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal("");

  const submit: JSX.EventHandler<HTMLFormElement, SubmitEvent> = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      await api.createRevision(props.service.id);
      props.onPublished();
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal eyebrow={props.service.name} title="同步 Git 并发布" onClose={props.onClose}>
      <form class="form-stack" onSubmit={submit}>
        <div class="banner">
          <strong>自动生成不可变版本</strong>
          <span>系统拉取仓库 HEAD，以 commit SHA 作为 revision，打包源码、计算 SHA-256 并上传对象存储。</span>
        </div>
        <div class="banner">
          <strong>上线前校验</strong>
          <span>自动读取 pyproject.toml 并完成兼容性校验；运行中的服务自动切换，已停止服务只生成 READY revision。</span>
        </div>
        <Show when={error()}><div class="form-error">{error()}</div></Show>
        <footer class="modal__footer">
          <button class="button button--ghost" type="button" onClick={props.onClose}>取消</button>
          <button class="button button--primary" type="submit" disabled={busy()}>
            <Show when={!busy()} fallback="正在同步 Git、校验环境并构建…"><Icon name="rocket" />同步并发布</Show>
          </button>
        </footer>
      </form>
    </Modal>
  );
};

const InvocationTable: Component<{
  invocations: Invocation[];
  compact?: boolean;
  onSelect: (invocation: Invocation) => void;
}> = (props) => (
  <Show
    when={props.invocations.length > 0}
    fallback={<EmptyState icon="activity" title="还没有调用记录" detail="服务被调用后，状态和耗时会显示在这里。" />}
  >
    <div class="table-scroll">
      <table class="data-table">
        <thead><tr><th>状态</th><th>服务 / Endpoint</th><th>Revision</th><th>耗时</th><th>时间</th></tr></thead>
        <tbody>
          <For each={props.compact ? props.invocations.slice(0, 7) : props.invocations}>
            {(item) => (
              <tr class="is-clickable" onClick={() => props.onSelect(item)}>
                <td><StatusBadge value={item.status} /></td>
                <td><strong>{item.service}</strong><span class="cell-sub mono">{item.endpoint_id} · {item.execution_kind ?? "LEGACY"}</span></td>
                <td class="mono muted">{shortId(item.revision, 14)}</td>
                <td class="mono">{duration(item)}</td>
                <td class="muted">{formatDate(item.created_at)}<Show when={item.has_logs}><span class="cell-sub log-available">{item.log_bytes} B logs</span></Show></td>
              </tr>
            )}
          </For>
        </tbody>
      </table>
    </div>
  </Show>
);

const InvocationLogModal: Component<{
  invocation: Invocation;
  logs: InvocationLog[];
  loading: boolean;
  error: string;
  onClose: () => void;
}> = (props) => (
  <Modal eyebrow={`${props.invocation.service} / ${props.invocation.endpoint_id}`} title="调用输出" onClose={props.onClose}>
    <div class="invocation-log-detail">
      <div class="invocation-log-meta">
        <div><span>状态</span><StatusBadge value={props.invocation.status} /></div>
        <div><span>Request ID</span><code>{props.invocation.id}</code></div>
        <div><span>耗时</span><strong class="mono">{duration(props.invocation)}</strong></div>
        <div><span>输出大小</span><strong class="mono">{props.invocation.log_bytes} B</strong></div>
      </div>
      <Show when={props.invocation.error}><div class="form-error">{props.invocation.error}</div></Show>
      <Show when={props.invocation.logs_truncated}><div class="banner banner--danger"><strong>日志已截断</strong><span>本次输出超过服务端单次调用上限，只保存了前 {props.invocation.log_bytes} 字节。</span></div></Show>
      <Show when={!props.loading} fallback={<div class="log-console log-console--empty">正在读取日志…</div>}>
        <Show when={!props.error} fallback={<div class="form-error">{props.error}</div>}>
          <Show when={props.logs.length > 0} fallback={<div class="log-console log-console--empty">本次调用没有捕获到 Python 标准输出。</div>}>
            <pre class="log-console"><For each={props.logs}>{(log) => <span classList={{ "log-line": true, "log-line--stderr": log.stream === "STDERR" }} data-stream={log.stream}><time>{formatLogTime(log.emitted_at)}</time><span>{logContent(log.content)}</span></span>}</For></pre>
          </Show>
        </Show>
      </Show>
    </div>
  </Modal>
);

const App: Component = () => {
  const [view, setView] = createSignal<View>("overview");
  const [services, setServices] = createSignal<Service[]>([]);
  const [runtimeLabels, setRuntimeLabels] = createSignal<RuntimeLabel[]>([]);
  const [workerPools, setWorkerPools] = createSignal<WorkerPool[]>([]);
  const [selectedId, setSelectedId] = createSignal<string>("");
  const [serviceDetail, setServiceDetail] = createSignal<ServiceDetail | null>(null);
  const [revisions, setRevisions] = createSignal<Revision[]>([]);
  const [invocations, setInvocations] = createSignal<Invocation[]>([]);
  const [serviceInvocations, setServiceInvocations] = createSignal<Invocation[]>([]);
  const [contract, setContract] = createSignal<Contract | null>(null);
  const [healthy, setHealthy] = createSignal(false);
  const [loading, setLoading] = createSignal(true);
  const [refreshing, setRefreshing] = createSignal(false);
  const [invocationsRefreshing, setInvocationsRefreshing] = createSignal(false);
  const [serviceInvocationsRefreshing, setServiceInvocationsRefreshing] = createSignal(false);
  const [pageError, setPageError] = createSignal("");
  const [workerPoolError, setWorkerPoolError] = createSignal("");
  const [showCreateService, setShowCreateService] = createSignal(false);
  const [showPublish, setShowPublish] = createSignal(false);
  const [showServiceSettings, setShowServiceSettings] = createSignal(false);
  const [showRuntimeModal, setShowRuntimeModal] = createSignal(false);
  const [runtimeModalLabel, setRuntimeModalLabel] = createSignal<RuntimeLabel | null>(null);
  const [toast, setToast] = createSignal<Toast | null>(null);
  const [selectedInvocation, setSelectedInvocation] = createSignal<Invocation | null>(null);
  const [invocationLogs, setInvocationLogs] = createSignal<InvocationLog[]>([]);
  const [invocationLogsLoading, setInvocationLogsLoading] = createSignal(false);
  const [invocationLogsError, setInvocationLogsError] = createSignal("");

  const selectedService = createMemo(() =>
    services().find((service) => service.id === selectedId()),
  );
  const activeCount = createMemo(() =>
    services().filter((service) => service.status === "ACTIVE").length,
  );
  const failureCount = createMemo(() =>
    invocations().filter((item) => ["FAILED", "TIMED_OUT"].includes(item.status)).length,
  );

  let toastTimer: ReturnType<typeof setTimeout> | undefined;
  const notify = (message: string, tone: Toast["tone"] = "success") => {
    setToast({ message, tone });
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => setToast(null), 3600);
  };
  onCleanup(() => toastTimer && clearTimeout(toastTimer));

  const loadDetails = async (service: Service) => {
    const [nextDetail, nextRevisions, nextInvocations] = await Promise.all([
      api.service(service.id),
      api.revisions(service.id),
      api.serviceInvocations(service.id, 50),
    ]);
    setServiceDetail(nextDetail);
    setRevisions(nextRevisions);
    setServiceInvocations(nextInvocations);
    if (service.status === "ACTIVE") {
      try {
        setContract(await api.contract(service.name));
      } catch (error) {
        if (error instanceof ApiError && error.status === 404) setContract(null);
        else throw error;
      }
    } else {
      setContract(null);
    }
  };

  const load = async (quiet = false) => {
    if (quiet) setRefreshing(true);
    else setLoading(true);
    setPageError("");
    setWorkerPoolError("");
    void api.workerPools()
      .then(setWorkerPools)
      .catch((error) => {
        setWorkerPools([]);
        setWorkerPoolError(errorMessage(error));
      });
    try {
      const [nextServices, nextInvocations, nextRuntimeLabels, health] = await Promise.all([
        api.services(),
        api.invocations(100),
        api.runtimeLabels(),
        api.health(),
      ]);
      setServices(nextServices);
      setInvocations(nextInvocations);
      setRuntimeLabels(nextRuntimeLabels);
      setHealthy(health.status === "ready");
      const current = nextServices.find((item) => item.id === selectedId());
      const nextSelected = current ?? nextServices[0];
      if (nextSelected) {
        setSelectedId(nextSelected.id);
      } else {
        setSelectedId("");
        setServiceDetail(null);
        setRevisions([]);
        setServiceInvocations([]);
        setContract(null);
      }
    } catch (error) {
      setHealthy(false);
      setPageError(errorMessage(error));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  createEffect(() => {
    const service = selectedService();
    if (service && !loading()) {
      setServiceDetail(null);
      void loadDetails(service).catch((error) => setPageError(errorMessage(error)));
    }
  });
  onMount(() => void load());

  const selectService = (service: Service) => {
    setSelectedId(service.id);
    setView("services");
  };

  const openInvocation = async (invocation: Invocation) => {
    setSelectedInvocation(invocation);
    setInvocationLogs([]);
    setInvocationLogsError("");
    setInvocationLogsLoading(true);
    try {
      setInvocationLogs(await api.invocationLogs(invocation.id));
    } catch (error) {
      setInvocationLogsError(errorMessage(error));
    } finally {
      setInvocationLogsLoading(false);
    }
  };

  const refreshInvocations = async () => {
    setInvocationsRefreshing(true);
    try {
      setInvocations(await api.invocations(100));
    } catch (error) {
      notify(errorMessage(error), "danger");
    } finally {
      setInvocationsRefreshing(false);
    }
  };

  const refreshServiceInvocations = async () => {
    const service = selectedService();
    if (!service) return;
    setServiceInvocationsRefreshing(true);
    try {
      setServiceInvocations(await api.serviceInvocations(service.id, 50));
    } catch (error) {
      notify(errorMessage(error), "danger");
    } finally {
      setServiceInvocationsRefreshing(false);
    }
  };

  const activate = async (revision: Revision) => {
    const service = selectedService();
    if (!service || !window.confirm(`激活 revision ${revision.revision}？新请求将立即切换。`)) return;
    try {
      await api.activateRevision(service.id, revision.id);
      notify(`已激活 ${revision.revision}`);
      await load(true);
    } catch (error) {
      notify(errorMessage(error), "danger");
    }
  };

  const stop = async () => {
    const service = selectedService();
    if (!service || !window.confirm(`停止 ${service.name}？新的调用将被拒绝。`)) return;
    try {
      await api.stopService(service.id);
      notify(`${service.name} 已停止`);
      await load(true);
    } catch (error) {
      notify(errorMessage(error), "danger");
    }
  };

  const start = async () => {
    const service = selectedService();
    if (!service || !window.confirm(`启动 ${service.name}？将恢复当前 active revision。`)) return;
    try {
      await api.startService(service.id);
      notify(`${service.name} 已启动`);
      await load(true);
    } catch (error) {
      notify(errorMessage(error), "danger");
    }
  };

  const openRuntimeModal = (label: RuntimeLabel | null = null) => {
    setRuntimeModalLabel(label);
    setShowRuntimeModal(true);
  };

  const activateRuntime = async (profile: RuntimeProfileVersion) => {
    if (!window.confirm(`激活 ${profile.profile_ref}？所有自动跟随的服务将先做依赖校验。`)) return;
    try {
      await api.activateRuntimeVersion(profile.id);
      notify(`${profile.profile_ref} 已激活`);
      await load(true);
    } catch (error) {
      notify(errorMessage(error), "danger");
    }
  };

  const retireRuntime = async (profile: RuntimeProfileVersion) => {
    if (!window.confirm(`停用 ${profile.profile_ref}？Actor 会等待现有请求结束后释放。`)) return;
    try {
      await api.retireRuntimeVersion(profile.id);
      notify(`${profile.profile_ref} 正在排空`);
      await load(true);
    } catch (error) {
      notify(errorMessage(error), "danger");
    }
  };

  const navItems: { id: View; label: string; icon: IconName }[] = [
    { id: "overview", label: "概览", icon: "grid" },
    { id: "services", label: "服务", icon: "server" },
    { id: "profiles", label: "运行环境", icon: "box" },
    { id: "activity", label: "调用记录", icon: "activity" },
  ];

  return (
    <div class="app-shell">
      <aside class="sidebar">
        <div class="brand"><span class="brand__mark">PS</span><div><strong>pyscripts</strong><span>runtime console</span></div></div>
        <nav class="side-nav" aria-label="主导航">
          <For each={navItems}>{(item) => (
            <button classList={{ "side-nav__item": true, "is-active": view() === item.id }} onClick={() => setView(item.id)}>
              <Icon name={item.icon} /><span>{item.label}</span>
            </button>
          )}</For>
        </nav>
        <div class="sidebar__foot">
          <div class="environment"><span classList={{ "health-dot": true, "is-online": healthy() }} /><div><strong>{healthy() ? "控制面正常" : "连接异常"}</strong><span>HTTP · gRPC</span></div></div>
          <span class="build-label">console / v0.1</span>
        </div>
      </aside>

      <main class="workspace">
        <header class="topbar">
          <div><span class="eyebrow">{viewLabels[view()].kicker}</span><h1>{viewLabels[view()].title}</h1></div>
          <div class="topbar__actions">
            <button class="icon-button" classList={{ "is-spinning": refreshing() }} onClick={() => void load(true)} aria-label="刷新数据" title="刷新数据"><Icon name="refresh" /></button>
            <Show
              when={view() === "profiles"}
              fallback={<button class="button button--primary" onClick={() => setShowCreateService(true)}><Icon name="plus" />注册服务</button>}
            >
              <button class="button button--primary" onClick={() => openRuntimeModal()}><Icon name="plus" />创建环境</button>
            </Show>
          </div>
        </header>

        <Show when={pageError()}><div class="banner banner--danger"><strong>无法加载控制面数据</strong><span>{pageError()}</span><button onClick={() => void load()}>重试</button></div></Show>
        <Show when={workerPoolError()}><div class="banner banner--danger"><strong>Worker 类型暂不可用</strong><span>{workerPoolError()}</span><button onClick={() => void load(true)}>重试</button></div></Show>
        <Show when={!loading()} fallback={<div class="loading-grid"><For each={[1, 2, 3, 4]}>{() => <div class="skeleton-card" />}</For></div>}>
          <Show when={view() === "overview"}>
            <section class="metrics-grid" aria-label="平台指标">
              <article class="metric"><span>服务总数</span><strong>{services().length}</strong><small>已注册脚本服务</small></article>
              <article class="metric metric--accent"><span>在线服务</span><strong>{activeCount()}</strong><small>{services().length ? `${Math.round((activeCount() / services().length) * 100)}% active` : "暂无服务"}</small></article>
              <article class="metric"><span>最近调用</span><strong>{invocations().length}</strong><small>当前加载窗口</small></article>
              <article classList={{ metric: true, "metric--danger": failureCount() > 0 }}><span>失败 / 超时</span><strong>{failureCount()}</strong><small>最近 100 条调用</small></article>
            </section>

            <section class="overview-grid">
              <article class="panel panel--services">
                <header class="panel__header"><div><span class="eyebrow">SERVICES</span><h2>服务状态</h2></div><button class="text-button" onClick={() => setView("services")}>查看全部 <Icon name="chevron" size={15} /></button></header>
                <Show when={services().length > 0} fallback={<EmptyState icon="server" title="还没有服务" detail="注册第一个 Git 脚本服务开始发布。" action={<button class="button button--primary button--small" onClick={() => setShowCreateService(true)}>注册服务</button>} />}>
                  <div class="service-list">
                    <For each={services().slice(0, 8)}>{(service) => (
                      <button class="service-row" onClick={() => selectService(service)}>
                        <span class="service-row__avatar">{service.name.slice(0, 2).toUpperCase()}</span>
                        <span class="service-row__main"><strong>{service.name}</strong><small><Icon name="git" size={13} />{service.git_url.replace(/^https?:\/\//, "")}</small></span>
                        <StatusBadge value={service.status} />
                        <span class="service-row__revision mono">{shortId(service.active_revision_id)}</span>
                        <Icon name="chevron" size={16} />
                      </button>
                    )}</For>
                  </div>
                </Show>
              </article>

              <article class="panel panel--activity">
                <header class="panel__header"><div><span class="eyebrow">LIVE LOG</span><h2>最近调用</h2></div><div class="contract-actions"><button class="icon-button icon-button--small" classList={{ "is-spinning": invocationsRefreshing() }} onClick={() => void refreshInvocations()} aria-label="刷新调用记录" title="刷新调用记录"><Icon name="refresh" size={15} /></button><button class="text-button" onClick={() => setView("activity")}>完整记录 <Icon name="chevron" size={15} /></button></div></header>
                <InvocationTable invocations={invocations()} compact onSelect={(item) => void openInvocation(item)} />
              </article>
            </section>
          </Show>

          <Show when={view() === "services"}>
            <section class="services-workbench">
              <aside class="service-rail">
                <div class="service-rail__header"><span>{services().length} SERVICES</span><button class="icon-button icon-button--small" onClick={() => setShowCreateService(true)} aria-label="注册服务"><Icon name="plus" size={16} /></button></div>
                <Show when={services().length > 0} fallback={<EmptyState icon="server" title="服务列表为空" detail="先注册一个服务。" />}>
                  <For each={services()}>{(service) => (
                    <button classList={{ "service-card": true, "is-selected": selectedId() === service.id }} onClick={() => setSelectedId(service.id)}>
                      <span class="service-card__top"><strong>{service.name}</strong><span classList={{ "health-dot": true, "is-online": service.status === "ACTIVE" }} /></span>
                      <span class="mono">{shortId(service.active_revision_id, 15)}</span>
                      <small>{service.tracking_mode}</small>
                    </button>
                  )}</For>
                </Show>
              </aside>

              <div class="service-detail">
                <Show when={selectedService()} fallback={<EmptyState icon="server" title="选择一个服务" detail="服务详情、版本和契约会显示在这里。" />} keyed>
                  {(service) => (
                    <>
                      <header class="service-hero">
                        <div><div class="service-hero__title"><h2>{service.name}</h2><StatusBadge value={service.status} /></div><p><Icon name="git" size={15} />{service.git_url}</p></div>
                        <div class="service-hero__actions">
                          <Show when={service.status === "ACTIVE"}><button class="button button--danger" onClick={() => void stop()}><Icon name="stop" />停止</button></Show>
                          <Show when={service.status === "STOPPED" && service.active_revision_id}><button class="button button--primary" onClick={() => void start()}><Icon name="start" />启动</button></Show>
                          <button class="button button--outline" onClick={() => setShowServiceSettings(true)}><Icon name="settings" />设置</button>
                          <button class="button button--primary" onClick={() => setShowPublish(true)}><Icon name="rocket" />同步并发布</button>
                        </div>
                      </header>

                      <div class="detail-strip">
                        <div><span>跟踪方式</span><strong>{service.tracking_mode}{service.tracking_mode === "poll" && service.check_interval_seconds ? ` · ${service.check_interval_seconds}s` : ""}</strong></div>
                        <div><span>环境来源</span><strong class="mono">pyproject.toml</strong></div>
                        <div><span>活动 revision</span><strong class="mono">{shortId(service.active_revision_id, 18)}</strong></div>
                        <div><span>创建时间</span><strong>{formatDate(service.created_at)}</strong></div>
                        <div><span>接口调用</span><strong>{serviceInvocations().length}</strong></div>
                      </div>

                      <section class="detail-section">
                        <header class="section-heading"><div><span class="eyebrow">ACTIVE API</span><h3>Endpoint</h3></div><span>{serviceDetail()?.endpoints.length ?? 0} 个接口</span></header>
                        <Show when={(serviceDetail()?.endpoints.length ?? 0) > 0} fallback={<EmptyState icon="activity" title="没有活动 Endpoint" detail="同步并激活包含 endpoint 的 Revision 后显示。" />}>
                          <div class="endpoint-grid">
                            <For each={serviceDetail()?.endpoints ?? []}>{(endpoint) => (
                              <article class="endpoint-card">
                                <header><div><strong>{endpoint.id}</strong><span class="mono">{endpoint.entrypoint}</span></div><StatusBadge value={endpoint.task_type.toUpperCase()} /></header>
                                <Show when={(endpoint.io_type ?? ["rest"]).includes("rest")}>
                                  <code>POST /v1/services/{service.name}/{endpoint.id}</code>
                                </Show>
                                <div class="endpoint-card__meta">
                                  <span>参数</span>
                                  <strong class="mono">{endpointParameters(endpoint).join(", ") || "无"}</strong>
                                </div>
                                <Show when={endpoint.grpc}><div class="endpoint-card__meta"><span>gRPC{endpoint.grpc!.generated ? " · AUTO" : ""}</span><strong class="mono" title={`${endpoint.grpc!.service}/${endpoint.grpc!.method}`}>{grpcMethodName(endpoint.grpc!.service, endpoint.grpc!.method)}</strong></div></Show>
                              </article>
                            )}</For>
                          </div>
                        </Show>
                      </section>

                      <section class="detail-section">
                        <header class="section-heading"><div><span class="eyebrow">REVISIONS</span><h3>发布历史</h3></div><span>{revisions().length} 个版本</span></header>
                        <Show when={revisions().length > 0} fallback={<EmptyState icon="box" title="还没有 revision" detail="同步 Git 并通过校验后会自动创建并上线。" />}>
                          <div class="revision-list">
                            <For each={revisions()}>{(item) => (
                              <article class="revision-row">
                                <div class="revision-row__timeline"><span classList={{ "timeline-dot": true, "is-active": item.status === "ACTIVE" }} /></div>
                                <div class="revision-row__identity"><strong class="mono">{item.revision}</strong><span>{formatDate(item.created_at)}</span></div>
                                <div class="revision-row__runtime"><span>runtime</span><strong class="mono">{item.runtime_profile}</strong></div>
                                <div class="revision-row__endpoints"><span>{item.endpoints.length}</span> endpoints</div>
                                <StatusBadge value={item.status} />
                                <Show when={item.status === "READY"}><button class="button button--small button--outline" onClick={() => void activate(item)}>激活</button></Show>
                              </article>
                            )}</For>
                          </div>
                        </Show>
                      </section>

                      <section class="contract-layout">
                        <article class="contract-card">
                          <header><div><span class="eyebrow">gRPC CONTRACT</span><h3>Proto 契约</h3></div><Icon name="box" /></header>
                          <Show when={contract()} fallback={<EmptyState icon="box" title="没有活动契约" detail="发布带 gRPC endpoint 的 revision 后生成。" />} keyed>
                            {(item) => (
                              <div class="contract-card__body">
                                <div class="package-line"><span class="package-line__icon">PB</span><div><strong>Proto 源码包</strong><span>contract v{item.contract_version}</span></div><StatusBadge value="READY" /></div>
                                <div class="contract-actions contract-download-actions">
                                  <a class="button button--contract-download" href={item.proto_bundle_url}><Icon name="download" size={13} />下载 Proto</a>
                                </div>
                                <dl class="contract-meta"><div><dt>Schema</dt><dd class="mono">{shortId(item.schema_digest.replace("sha256:", ""), 16)}</dd></div><div><dt>Methods</dt><dd>{item.methods.length}</dd></div><div><dt>Proto</dt><dd class="mono">{shortId(item.proto_bundle_digest.replace("sha256:", ""), 16)}</dd></div></dl>
                              </div>
                            )}
                          </Show>
                        </article>

                        <article class="panel service-calls">
                          <header class="panel__header"><div><span class="eyebrow">SERVICE LOG</span><h3>最近调用</h3></div><div class="contract-actions"><span>{serviceInvocations().length}</span><button class="icon-button icon-button--small" classList={{ "is-spinning": serviceInvocationsRefreshing() }} onClick={() => void refreshServiceInvocations()} aria-label="刷新服务调用记录" title="刷新服务调用记录"><Icon name="refresh" size={15} /></button></div></header>
                          <InvocationTable invocations={serviceInvocations()} compact onSelect={(item) => void openInvocation(item)} />
                        </article>
                      </section>
                    </>
                  )}
                </Show>
              </div>
            </section>
          </Show>

          <Show when={view() === "profiles"}>
            <section class="runtime-workspace">
              <header class="panel__header panel__header--large">
                <div><span class="eyebrow">VERSIONED ENVIRONMENTS</span><h2>Python 标签与版本</h2><p>Worker 类型由 K8s 提供；Python 标签通过 Ray Runtime Env 动态安装、校验并复用。</p></div>
                <div class="runtime-summary"><strong>{runtimeLabels().length}</strong><span>python labels</span></div>
              </header>
              <div class="worker-pool-strip">
                <span class="eyebrow">K8S WORKER TYPES · READ ONLY</span>
                <Show when={workerPools().length > 0} fallback={<span class="muted">当前没有可用 Worker 类型</span>}>
                  <For each={workerPools()}>{(pool) => (
                    <span class="worker-pool-chip"><strong class="mono">{pool.name}</strong><small>{pool.node_count} nodes · {pool.source}</small></span>
                  )}</For>
                </Show>
              </div>
              <Show
                when={runtimeLabels().length > 0}
                fallback={<EmptyState icon="box" title="还没有运行环境" detail="创建第一个标签；首个通过校验的版本会自动激活。" action={<button class="button button--primary button--small" onClick={() => openRuntimeModal()}>创建环境</button>} />}
              >
                <div class="runtime-labels">
                  <For each={runtimeLabels()}>{(label) => (
                    <article class="runtime-label-card">
                      <header class="runtime-label-card__header">
                        <div><span class="eyebrow">RUNTIME LABEL</span><h3>{label.name}</h3></div>
                        <button class="button button--outline button--small" onClick={() => openRuntimeModal(label)}><Icon name="plus" size={15} />新版本</button>
                      </header>
                      <div class="runtime-version-list">
                        <For each={label.versions}>{(profile) => (
                          <div classList={{ "runtime-version": true, "is-active": profile.id === label.active_version_id }}>
                            <div class="runtime-version__identity">
                              <strong class="mono">{profile.profile_ref}</strong>
                              <span>{formatDate(profile.created_at)} · Python {profile.python_version}</span>
                            </div>
                            <div class="runtime-version__pool">
                              <span>WORKER TYPE</span>
                              <strong class="mono">{profile.worker_pool} · {profile.pip_source === "private" ? "private PyPI" : "default PyPI"}</strong>
                            </div>
                            <div class="runtime-version__deps">
                              <span>{profile.requested_dependencies.length} locked deps</span>
                              <small class="mono">{profile.requested_dependencies.slice(0, 2).join(" · ") || "base image only"}</small>
                            </div>
                            <div class="runtime-version__refs"><strong>{profile.reference_count}</strong><span>服务引用</span></div>
                            <StatusBadge value={profile.status} />
                            <div class="runtime-version__actions">
                              <Show when={profile.status === "READY"}><button class="button button--primary button--small" onClick={() => void activateRuntime(profile)}>激活</button></Show>
                              <Show when={["READY", "FAILED", "RETIRING"].includes(profile.status) && profile.id !== label.active_version_id && profile.reference_count === 0}>
                                <button class="button button--danger button--small" disabled={profile.status === "RETIRING"} onClick={() => void retireRuntime(profile)}>停用</button>
                              </Show>
                            </div>
                            <Show when={profile.status === "FAILED" && profile.error}><div class="runtime-version__error">{profile.error}</div></Show>
                          </div>
                        )}</For>
                      </div>
                    </article>
                  )}</For>
                </div>
              </Show>
            </section>
          </Show>

          <Show when={view() === "activity"}>
            <section class="panel activity-panel">
              <header class="panel__header panel__header--large"><div><span class="eyebrow">LATEST 100</span><h2>全局调用流水</h2><p>请求状态来自 PostgreSQL invocation 记录。</p></div><div class="contract-actions"><div class="legend"><span><i class="legend__dot legend__dot--good" />成功</span><span><i class="legend__dot legend__dot--warn" />执行中</span><span><i class="legend__dot legend__dot--bad" />失败</span></div><button class="button button--outline button--small" classList={{ "is-spinning": invocationsRefreshing() }} onClick={() => void refreshInvocations()}><Icon name="refresh" size={15} />刷新</button></div></header>
              <InvocationTable invocations={invocations()} onSelect={(item) => void openInvocation(item)} />
            </section>
          </Show>
        </Show>
      </main>

      <Show when={showCreateService()}><CreateServiceModal onClose={() => setShowCreateService(false)} onCreated={(service) => { setShowCreateService(false); setSelectedId(service.id); setView("services"); notify(`${service.name} 已注册`); void load(true); }} /></Show>
      <Show when={showServiceSettings() && selectedService()} keyed>{(service) => <ServiceSettingsModal service={service} onClose={() => setShowServiceSettings(false)} onUpdated={(updated) => { setShowServiceSettings(false); setServiceDetail(updated); setServices((items) => items.map((item) => item.id === updated.id ? updated : item)); notify(`${updated.name} 设置已保存`); void load(true); }} />}</Show>
      <Show when={showPublish() && selectedService()} keyed>{(service) => <PublishRevisionModal service={service} onClose={() => setShowPublish(false)} onPublished={() => { setShowPublish(false); notify(service.status === "STOPPED" ? "Git revision 已发布，服务仍保持停止" : "Git revision 已构建并上线"); void load(true); }} />}</Show>
      <Show when={showRuntimeModal()}><RuntimeProfileModal label={runtimeModalLabel()} workerPools={workerPools()} onClose={() => setShowRuntimeModal(false)} onCreated={(profile) => { setShowRuntimeModal(false); notify(profile.status === "FAILED" ? `${profile.profile_ref} 校验失败` : `${profile.profile_ref} 已通过校验`, profile.status === "FAILED" ? "danger" : "success"); void load(true); }} /></Show>
      <Show when={selectedInvocation()} keyed>{(invocation) => <InvocationLogModal invocation={invocation} logs={invocationLogs()} loading={invocationLogsLoading()} error={invocationLogsError()} onClose={() => setSelectedInvocation(null)} />}</Show>
      <Show when={toast()} keyed>{(item) => <div class={`toast toast--${item.tone}`}><Icon name={item.tone === "success" ? "check" : "x"} /><span>{item.message}</span></div>}</Show>
    </div>
  );
};

export default App;
