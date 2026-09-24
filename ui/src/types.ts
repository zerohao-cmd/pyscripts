export type ServiceStatus = "REGISTERED" | "ACTIVE" | "STOPPED";
export type RevisionStatus =
  | "BUILDING"
  | "READY"
  | "ACTIVE"
  | "DRAINING"
  | "RETIRED"
  | "FAILED";
export type InvocationStatus =
  | "ACCEPTED"
  | "RUNNING"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED"
  | "TIMED_OUT";
export type RuntimeProfileStatus =
  | "VALIDATING"
  | "READY"
  | "ACTIVE"
  | "FAILED"
  | "RETIRING"
  | "RETIRED";

export interface Service {
  id: string;
  name: string;
  git_url: string;
  git_branch: string | null;
  tracking_mode: "manual" | "poll" | "webhook";
  check_interval_seconds: number | null;
  status: ServiceStatus;
  active_revision_id: string | null;
  webhook_enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface GrpcEndpoint {
  service: string;
  method: string;
  descriptor_path: string;
  generated?: boolean;
  response_wrapped?: boolean;
}

export interface Endpoint {
  id: string;
  task_type: "io" | "compute";
  entrypoint: string;
  io_type?: ("rest" | "grpc")[];
  response_schema?: Record<string, unknown>;
  grpc?: GrpcEndpoint | null;
  num_cpus?: number | null;
  num_gpus?: number | null;
  [parameter: string]: unknown;
}

export interface Revision {
  id: string;
  service_id: string;
  revision: string;
  artifact_uri: string;
  artifact_digest: string;
  runtime_profile: string;
  status: RevisionStatus;
  endpoints: Endpoint[];
  created_at: string;
  activated_at: string | null;
}

export interface ServiceDetail extends Service {
  revision_count: number;
  active_revision: Revision | null;
  endpoints: Endpoint[];
}

export interface Invocation {
  id: string;
  service_id: string;
  service: string;
  revision_id: string;
  revision: string;
  endpoint_id: string;
  transport: "REST" | "GRPC" | null;
  status: InvocationStatus;
  error: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  execution_kind: "IO_ACTOR" | "COMPUTE_TASK" | null;
  runtime_profile: string | null;
  environment_digest: string | null;
  has_logs: boolean;
  log_bytes: number;
  logs_truncated: boolean;
}

export interface InvocationLog {
  sequence: number;
  stream: "STDOUT" | "STDERR";
  content: string;
  emitted_at: string;
  created_at: string;
}

export interface Contract {
  id: string;
  service: string;
  revision: string | null;
  contract_version: string;
  schema_digest: string;
  source_digest: string;
  proto_bundle_digest: string;
  methods: string[];
  proto_bundle_url: string;
}

export interface RuntimeProfileVersion {
  id: string;
  label_id: string;
  label_name: string;
  version: number;
  profile_ref: string;
  python_version: string;
  worker_pool: string;
  pip_source: "default" | "private";
  requested_dependencies: string[];
  resolved_dependencies: Record<string, string>;
  import_checks: string[];
  environment_digest: string | null;
  status: RuntimeProfileStatus;
  error: string | null;
  validation_result: Record<string, unknown>;
  reference_count: number;
  created_at: string;
  validated_at: string | null;
  retired_at: string | null;
}

export interface RuntimeLabel {
  id: string;
  name: string;
  active_version_id: string | null;
  created_at: string;
  versions: RuntimeProfileVersion[];
}

export interface RuntimeProfileInput {
  python_version: string;
  worker_pool: string;
  dependencies: string[];
  import_checks: string[];
  pip_source: "default" | "private";
}

export interface WebhookConfig {
  enabled: boolean;
  url: string | null;
  configured_at: string | null;
}

export interface WorkerPool {
  name: string;
  label_key: string;
  node_count: number;
  source: "KUBERAY" | "RAY" | "CONFIG";
  mutable: false;
}

export interface CreateRuntimeLabelInput extends RuntimeProfileInput {
  name: string;
}

export interface CreateServiceInput {
  name: string;
  git_url: string;
  git_branch?: string;
  tracking_mode: "manual" | "poll" | "webhook";
  check_interval_seconds?: number;
}

export interface UpdateServiceInput {
  git_url?: string;
  git_branch?: string | null;
  tracking_mode?: "manual" | "poll" | "webhook";
  check_interval_seconds?: number | null;
}
