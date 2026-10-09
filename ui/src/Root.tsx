import { For, Show, createSignal, onCleanup, onMount, type Component, type JSX } from "solid-js";

import AdminConsole from "./App";
import { ApiError, api } from "./api";
import { Icon } from "./icons";
import type { AdminSession, PublicService } from "./types";

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "发生未知错误";
}

function schemaLabel(schema: Record<string, unknown>): string {
  if (typeof schema.type === "string") return schema.type;
  return "Struct";
}

const LoginPage: Component<{
  notice?: string;
  onLogin: (session: AdminSession) => void;
}> = (props) => {
  const [password, setPassword] = createSignal("");
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal("");

  const submit: JSX.EventHandler<HTMLFormElement, SubmitEvent> = async (event) => {
    event.preventDefault();
    setBusy(true);
    setError("");
    try {
      props.onLogin(await api.login(password()));
    } catch (reason) {
      setError(
        reason instanceof ApiError && reason.status === 503
          ? "管理员认证尚未配置，请先设置服务端环境变量。"
          : "密码错误。",
      );
    } finally {
      setBusy(false);
    }
  };

  return (
    <main class="auth-page">
      <section class="auth-card">
        <a class="brand auth-brand" href="/api-docs"><span class="brand__mark">PS</span><div><strong>pyscripts</strong><span>runtime platform</span></div></a>
        <div class="auth-copy"><span class="eyebrow">ADMIN CONSOLE</span><h1>管理员登录</h1><p>管理服务、运行环境、版本和调用记录。</p></div>
        <form class="auth-form" onSubmit={submit}>
          <div class="auth-account"><span>管理员账户</span><strong>admin</strong></div>
          <label class="field"><span>密码</span><input type="password" autocomplete="current-password" value={password()} onInput={(event) => setPassword(event.currentTarget.value)} autofocus /></label>
          <Show when={props.notice}><div class="banner"><strong>{props.notice}</strong></div></Show>
          <Show when={error()}><div class="form-error">{error()}</div></Show>
          <button class="button button--primary" type="submit" disabled={busy()}>{busy() ? "正在验证…" : "进入管理控制台"}</button>
        </form>
        <a class="auth-public-link" href="/api-docs">无需登录，查看公开 API 文档 <Icon name="chevron" size={15} /></a>
      </section>
    </main>
  );
};

const PublicApiDocs: Component = () => {
  const [services, setServices] = createSignal<PublicService[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal("");
  const origin = window.location.origin;

  const load = async () => {
    setLoading(true);
    setError("");
    try {
      setServices(await api.catalog());
    } catch (reason) {
      setError(errorMessage(reason));
    } finally {
      setLoading(false);
    }
  };
  onMount(() => void load());

  return (
    <div class="docs-shell">
      <header class="docs-nav"><a class="brand" href="/api-docs"><span class="brand__mark">PS</span><div><strong>pyscripts</strong><span>API reference</span></div></a><a class="button button--outline button--small" href="/">管理员登录</a></header>
      <main class="docs-main">
        <section class="docs-hero"><span class="eyebrow">PUBLIC API REFERENCE</span><h1>调用已上线的脚本服务</h1><p>这里列出当前可调用的 REST 与 gRPC 接口。接口随服务的活动 revision 自动更新。</p><div class="docs-base"><span>HTTP Base URL</span><code>{origin}</code></div></section>
        <section class="docs-guide">
          <article><span>01</span><div><strong>选择服务和 Endpoint</strong><p>仅显示已启动并已激活 revision 的服务。</p></div></article>
          <article><span>02</span><div><strong>附带 API Token</strong><p>REST 使用 Authorization: Bearer；gRPC 使用同名 metadata。</p></div></article>
          <article><span>03</span><div><strong>调用或下载 Proto</strong><p>REST 直接发送 JSON；gRPC 使用 Proto 生成任意语言客户端。</p></div></article>
        </section>
        <Show when={error()}><div class="banner banner--danger"><strong>无法加载 API 目录</strong><span>{error()}</span><button onClick={() => void load()}>重试</button></div></Show>
        <Show when={!loading()} fallback={<div class="docs-loading">正在读取活动接口…</div>}>
          <Show when={services().length > 0} fallback={<div class="docs-empty"><strong>当前没有已上线的公开接口</strong><p>服务启动并激活 revision 后会自动出现在这里。</p></div>}>
            <div class="docs-services"><For each={services()}>{(service) => (
              <section class="docs-service">
                <header><div><span class="eyebrow">SERVICE</span><h2>{service.name}</h2></div><code>revision {service.revision}</code></header>
                <div class="docs-endpoints"><For each={service.endpoints}>{(endpoint) => {
                  return (
                    <article class="docs-endpoint">
                      <div class="docs-endpoint__title"><div><h3>{endpoint.id}</h3><span>{endpoint.task_type === "io" ? "异步 IO" : "计算任务"}</span></div><div class="transport-tags"><For each={endpoint.io_type}>{(transport) => <span>{transport.toUpperCase()}</span>}</For></div></div>
                      <Show when={endpoint.rest_path}><div class="docs-call"><span class="method method--post">POST</span><code>{endpoint.rest_path}</code></div></Show>
                      <Show when={endpoint.grpc_service && endpoint.grpc_method}><div class="docs-call"><span class="method method--grpc">RPC</span><code>{endpoint.grpc_service}/{endpoint.grpc_method}</code></div></Show>
                      <div class="docs-schema"><div><h4>参数</h4><Show when={Object.keys(endpoint.parameters).length > 0} fallback={<p class="muted">无参数</p>}><dl><For each={Object.entries(endpoint.parameters)}>{([name, schema]) => <div><dt>{name}</dt><dd>{schemaLabel(schema)}</dd></div>}</For></dl></Show></div><div><h4>返回</h4><code>{schemaLabel(endpoint.response_schema)}</code></div></div>
                      <p class="docs-auth-note">执行信息：状态、错误与 stdout/stderr 日志会随结果返回；REST 位于响应信封中，gRPC 位于强类型 <code>execution</code> 字段中。</p>
                      <Show when={endpoint.grpc_service && endpoint.grpc_method}><p class="docs-auth-note">gRPC metadata：<code>authorization: Bearer &lt;API_TOKEN&gt;</code></p></Show>
                    </article>
                  );
                }}</For></div>
                <Show when={service.grpc_contract_url}><footer><a class="button button--contract-download" href={service.grpc_contract_url!}>查看 gRPC 契约并下载 Proto <Icon name="download" size={13} /></a></footer></Show>
              </section>
            )}</For></div>
          </Show>
        </Show>
      </main>
    </div>
  );
};

const AdminGate: Component = () => {
  const [session, setSession] = createSignal<AdminSession | null>(null);
  const [checking, setChecking] = createSignal(true);
  const [notice, setNotice] = createSignal("");

  const unauthorize = () => setSession(null);
  onMount(() => {
    api.setUnauthorizedHandler(unauthorize);
    void api.session().then(setSession).catch(() => setSession(null)).finally(() => setChecking(false));
  });
  onCleanup(() => api.setUnauthorizedHandler());

  const logout = async () => {
    try {
      await api.logout();
    } finally {
      setSession(null);
    }
  };

  const passwordChanged = () => {
    setNotice("密码已修改，请使用新密码重新登录。");
    setSession(null);
  };

  return <Show when={!checking()} fallback={<div class="app-loading"><span class="brand__mark">PS</span><p>正在验证管理会话…</p></div>}><Show when={session()} fallback={<LoginPage notice={notice()} onLogin={(active) => { setNotice(""); setSession(active); }} />} keyed>{(active) => <AdminConsole username={active.username} onLogout={() => void logout()} onPasswordChanged={passwordChanged} />}</Show></Show>;
};

const Root: Component = () => window.location.pathname.startsWith("/api-docs") ? <PublicApiDocs /> : <AdminGate />;

export default Root;
