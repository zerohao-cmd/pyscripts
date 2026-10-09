import type {
  AdminSession,
  ApiToken,
  Contract,
  CreateServiceInput,
  CreatedApiToken,
  Invocation,
  InvocationLog,
  EndpointInvocationPolicy,
  PublicService,
  Revision,
  RuntimeLabel,
  RuntimeProfileInput,
  CreateRuntimeLabelInput,
  RuntimeProfileVersion,
  Service,
  ServiceDetail,
  UpdateServiceInput,
  WorkerPool,
  WebhookConfig,
} from "./types";

let unauthorizedHandler: (() => void) | undefined;

export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const body = (await response.json()) as { detail?: unknown };
      if (typeof body.detail === "string") message = body.detail;
      else if (body.detail) message = JSON.stringify(body.detail);
    } catch {
      // Keep the HTTP status fallback when the response is not JSON.
    }
    if (response.status === 401 && path.startsWith("/admin/")) {
      unauthorizedHandler?.();
    }
    throw new ApiError(response.status, message);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export const api = {
  setUnauthorizedHandler: (handler?: () => void) => {
    unauthorizedHandler = handler;
  },
  login: (password: string) =>
    request<AdminSession>("/auth/login", {
      method: "POST",
      body: JSON.stringify({ password }),
    }),
  logout: () => request<void>("/auth/logout", { method: "POST" }),
  changeAdminPassword: (currentPassword: string, newPassword: string) =>
    request<void>("/admin/password", {
      method: "POST",
      body: JSON.stringify({
        current_password: currentPassword,
        new_password: newPassword,
      }),
    }),
  session: () => request<AdminSession>("/auth/session"),
  catalog: () => request<PublicService[]>("/v1/catalog"),
  apiTokens: () => request<ApiToken[]>("/admin/api-tokens"),
  createApiToken: (name: string) =>
    request<CreatedApiToken>("/admin/api-tokens", {
      method: "POST",
      body: JSON.stringify({ name }),
    }),
  revokeApiToken: (tokenId: string) =>
    request<ApiToken>(`/admin/api-tokens/${tokenId}`, { method: "DELETE" }),
  health: () => request<{ status: string }>("/health/ready"),
  services: () => request<Service[]>("/admin/services"),
  runtimeLabels: () => request<RuntimeLabel[]>("/admin/runtime-labels"),
  workerPools: () => request<WorkerPool[]>("/admin/worker-pools"),
  createRuntimeLabel: (body: CreateRuntimeLabelInput) =>
    request<RuntimeProfileVersion>("/admin/runtime-labels", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  createRuntimeVersion: (labelId: string, body: RuntimeProfileInput) =>
    request<RuntimeProfileVersion>(`/admin/runtime-labels/${labelId}/versions`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  activateRuntimeVersion: (versionId: string) =>
    request<RuntimeProfileVersion>(
      `/admin/runtime-profile-versions/${versionId}/activate`,
      { method: "POST" },
    ),
  retireRuntimeVersion: (versionId: string) =>
    request<RuntimeProfileVersion>(
      `/admin/runtime-profile-versions/${versionId}/retire`,
      { method: "POST" },
    ),
  createService: (body: CreateServiceInput) =>
    request<Service>("/admin/services", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  service: (serviceId: string) =>
    request<ServiceDetail>(`/admin/services/${serviceId}`),
  updateService: (serviceId: string, body: UpdateServiceInput) =>
    request<ServiceDetail>(`/admin/services/${serviceId}`, {
      method: "PATCH",
      body: JSON.stringify(body),
    }),
  deleteService: (serviceId: string) =>
    request<void>(`/admin/services/${serviceId}`, { method: "DELETE" }),
  updateEndpointInvocationPolicy: (
    serviceId: string,
    endpointId: string,
    retentionCount: number,
  ) => request<EndpointInvocationPolicy>(
    `/admin/services/${serviceId}/endpoints/${encodeURIComponent(endpointId)}/invocation-policy`,
    {
      method: "PATCH",
      body: JSON.stringify({ retention_count: retentionCount }),
    },
  ),
  serviceWebhook: (serviceId: string) =>
    request<WebhookConfig>(`/admin/services/${serviceId}/webhook`),
  rotateServiceWebhook: (serviceId: string) =>
    request<WebhookConfig>(`/admin/services/${serviceId}/webhook/rotate`, {
      method: "POST",
    }),
  disableServiceWebhook: (serviceId: string) =>
    request<WebhookConfig>(`/admin/services/${serviceId}/webhook`, {
      method: "DELETE",
    }),
  revisions: (serviceId: string) =>
    request<Revision[]>(`/admin/services/${serviceId}/revisions`),
  createRevision: (serviceId: string) =>
    request<Revision>(`/admin/services/${serviceId}/revisions`, {
      method: "POST",
    }),
  activateRevision: (serviceId: string, revisionId: string) =>
    request<Revision>(
      `/admin/services/${serviceId}/revisions/${revisionId}/activate`,
      { method: "POST" },
    ),
  stopService: (serviceId: string) =>
    request<Service>(`/admin/services/${serviceId}/stop`, { method: "POST" }),
  startService: (serviceId: string) =>
    request<Service>(`/admin/services/${serviceId}/start`, { method: "POST" }),
  invocations: (limit = 100) =>
    request<Invocation[]>(`/admin/invocations?limit=${limit}`),
  serviceInvocations: (serviceId: string, limit = 100) =>
    request<Invocation[]>(
      `/admin/services/${serviceId}/invocations?limit=${limit}`,
    ),
  invocationLogs: (invocationId: string, afterSequence = -1) =>
    request<InvocationLog[]>(
      `/admin/invocations/${invocationId}/logs?after_sequence=${afterSequence}`,
    ),
  contract: (serviceName: string) =>
    request<Contract>(`/v1/services/${serviceName}/grpc-contract`),
};
