import type {
  Contract,
  CreateServiceInput,
  Invocation,
  InvocationLog,
  Revision,
  RuntimeLabel,
  RuntimeProfileInput,
  CreateRuntimeLabelInput,
  RuntimeProfileVersion,
  Service,
  ServiceDetail,
  UpdateServiceInput,
  WorkerPool,
} from "./types";

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
    throw new ApiError(response.status, message);
  }
  return response.json() as Promise<T>;
}

export const api = {
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
