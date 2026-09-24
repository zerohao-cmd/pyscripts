from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class Settings(BaseSettings):
    """Process configuration.

    The password intentionally has no default. It must be supplied through
    ``PYSCRIPTS_DATABASE_PASSWORD`` or a complete
    ``PYSCRIPTS_DATABASE_URL``.
    """

    model_config = SettingsConfigDict(
        env_prefix="PYSCRIPTS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    database_url: SecretStr | None = None
    database_host: str = "192.168.0.11"
    database_port: int = 5432
    database_name: str = "pyscripts"
    database_user: str = "pyscripts_adm"
    database_password: SecretStr | None = None
    database_echo: bool = False
    auto_create_schema: bool = True

    ray_address: str = "auto"
    ray_namespace: str = "pyscripts"
    ray_use_label_selector: bool = True
    ray_worker_pool_label_key: str = Field(
        default="pyscripts.worker-pool",
        validation_alias=AliasChoices(
            "PYSCRIPTS_RAY_WORKER_POOL_LABEL_KEY",
            "PYSCRIPTS_RAY_NODE_LABEL_KEY",
        ),
    )
    ray_node_id_label_key: str = "ray.io/node-id"
    default_worker_pool: str = "default"
    kuberay_cluster_name: str | None = None
    kubernetes_namespace: str | None = None
    kubernetes_service_account_root: Path = Path(
        "/var/run/secrets/kubernetes.io/serviceaccount"
    )
    actor_max_per_profile: int = Field(default=8, ge=1)
    actor_max_io: int = Field(default=100, ge=1)
    actor_target_io_concurrency: int = Field(default=50, ge=1)
    actor_hot_request_rate: float = Field(default=0.5, gt=0)
    actor_request_rate_window_seconds: float = Field(default=10.0, gt=0)
    actor_warm_spares: int = Field(default=1, ge=0)
    actor_duration_ewma_alpha: float = Field(default=0.2, gt=0, le=1)
    actor_num_cpus: float = Field(default=1.0, gt=0)
    actor_lease_ttl_seconds: float = Field(default=15.0, gt=0)
    actor_queue_timeout_seconds: float = Field(default=30.0, gt=0)
    actor_scheduler_poll_seconds: float = Field(default=0.05, gt=0)
    actor_hot_idle_timeout_seconds: float = Field(default=300.0, gt=0)
    actor_warm_idle_timeout_seconds: float = Field(default=1800.0, gt=0)
    actor_reaper_interval_seconds: float = Field(default=15.0, gt=0)
    actor_cache_root: Path = Path("/tmp/pyscripts-runtime")
    invocation_timeout_seconds: float = Field(default=300.0, gt=0)
    invocation_log_max_bytes: int = Field(default=64 * 1024, ge=1024)
    invocation_log_chunk_bytes: int = Field(default=4 * 1024, ge=256)
    capture_stderr: bool = True
    compute_task_num_cpus: float = Field(default=1.0, gt=0)
    compute_task_num_gpus: float = Field(default=0.0, ge=0)
    compute_max_pending_per_profile: int = Field(default=64, ge=1)
    compute_profile_affinity: bool = True
    compute_warm_nodes_per_profile: int = Field(default=8, ge=1)
    runtime_env_setup_timeout_seconds: int = Field(default=900, ge=30)
    runtime_profile_retirement_timeout_seconds: float = Field(default=600, gt=0)
    require_registered_runtime_profiles: bool = True
    private_pip_index_url: SecretStr | None = None
    private_pip_extra_index_urls: list[SecretStr] = Field(default_factory=list)
    private_pip_trusted_hosts: list[str] = Field(default_factory=list)

    public_base_url: str | None = None
    webhook_max_body_bytes: int = Field(default=1024 * 1024, ge=1024)

    object_store_enabled: bool = False
    object_store_endpoint_url: str | None = None
    object_store_region: str = "us-east-1"
    object_store_bucket: str | None = None
    object_store_artifact_prefix: str = "pyscripts/artifacts"
    object_store_access_key_id: SecretStr | None = None
    object_store_secret_access_key: SecretStr | None = None
    object_store_session_token: SecretStr | None = None
    object_store_addressing_style: str = Field(
        default="path", pattern="^(path|virtual)$"
    )
    object_store_verify_ssl: bool = True
    object_store_presign_ttl_seconds: int = Field(default=3600, ge=60, le=604800)
    object_store_connect_timeout_seconds: int = Field(default=10, ge=1)
    object_store_read_timeout_seconds: int = Field(default=120, ge=1)

    grpc_enabled: bool = True
    grpc_host: str = "0.0.0.0"
    grpc_port: int = Field(default=50051, ge=0, le=65535)
    grpc_shutdown_grace_seconds: float = Field(default=10.0, ge=0)
    grpc_route_refresh_seconds: float = Field(default=5.0, gt=0)
    grpc_max_receive_message_bytes: int = Field(default=4 * 1024 * 1024, gt=0)
    grpc_max_send_message_bytes: int = Field(default=4 * 1024 * 1024, gt=0)

    contract_artifact_root: Path = Path("/tmp/pyscripts-contracts")

    serve_ui: bool = True
    ui_dist_path: Path = Path("ui/dist")

    @property
    def sqlalchemy_url(self) -> URL | str:
        if self.database_url is not None:
            return self.database_url.get_secret_value()
        if self.database_password is None:
            raise RuntimeError(
                "Set PYSCRIPTS_DATABASE_PASSWORD or PYSCRIPTS_DATABASE_URL"
            )
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.database_user,
            password=self.database_password.get_secret_value(),
            host=self.database_host,
            port=self.database_port,
            database=self.database_name,
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
