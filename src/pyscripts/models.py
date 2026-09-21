from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JSON_VALUE = JSON().with_variant(JSONB(), "postgresql")


class ServiceStatus(str, enum.Enum):
    REGISTERED = "REGISTERED"
    ACTIVE = "ACTIVE"
    STOPPED = "STOPPED"


class RevisionStatus(str, enum.Enum):
    BUILDING = "BUILDING"
    READY = "READY"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    RETIRED = "RETIRED"
    FAILED = "FAILED"


class InvocationStatus(str, enum.Enum):
    ACCEPTED = "ACCEPTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMED_OUT = "TIMED_OUT"


class ExecutionKind(str, enum.Enum):
    IO_ACTOR = "IO_ACTOR"
    COMPUTE_TASK = "COMPUTE_TASK"


class ContractStatus(str, enum.Enum):
    READY = "READY"
    FAILED = "FAILED"


class RuntimeProfileStatus(str, enum.Enum):
    VALIDATING = "VALIDATING"
    READY = "READY"
    ACTIVE = "ACTIVE"
    FAILED = "FAILED"
    RETIRING = "RETIRING"
    RETIRED = "RETIRED"


class RuntimeTrackingMode(str, enum.Enum):
    PINNED = "PINNED"
    TRACK_ACTIVE = "TRACK_ACTIVE"


class Base(DeclarativeBase):
    pass


class Service(Base):
    __tablename__ = "services"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    git_url: Mapped[str] = mapped_column(Text)
    tracking_mode: Mapped[str] = mapped_column(String(24), default="manual")
    check_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    webhook_token_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    webhook_configured_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    runtime_profile: Mapped[str | None] = mapped_column(
        String(256), nullable=True, index=True
    )
    runtime_tracking_mode: Mapped[RuntimeTrackingMode | None] = mapped_column(
        Enum(RuntimeTrackingMode, native_enum=False), nullable=True
    )
    status: Mapped[ServiceStatus] = mapped_column(
        Enum(ServiceStatus, native_enum=False), default=ServiceStatus.REGISTERED
    )
    active_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    @property
    def webhook_enabled(self) -> bool:
        return self.webhook_token_hash is not None


class Revision(Base):
    __tablename__ = "revisions"
    __table_args__ = (
        UniqueConstraint("service_id", "revision", name="uq_revision_service_name"),
        Index("ix_revisions_service_status", "service_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("services.id", ondelete="CASCADE"), index=True
    )
    revision: Mapped[str] = mapped_column(String(128))
    artifact_uri: Mapped[str] = mapped_column(Text)
    artifact_digest: Mapped[str] = mapped_column(String(128))
    runtime_profile: Mapped[str] = mapped_column(String(256), index=True)
    status: Mapped[RevisionStatus] = mapped_column(
        Enum(RevisionStatus, native_enum=False), default=RevisionStatus.READY
    )
    manifest: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RuntimeLabel(Base):
    __tablename__ = "runtime_labels"

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    active_version_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class RuntimeProfileVersion(Base):
    __tablename__ = "runtime_profile_versions"
    __table_args__ = (
        UniqueConstraint("label_id", "version", name="uq_runtime_profile_version"),
        UniqueConstraint("profile_ref", name="uq_runtime_profile_ref"),
        Index("ix_runtime_profile_label_status", "label_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    label_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runtime_labels.id", ondelete="RESTRICT"), index=True
    )
    version: Mapped[int] = mapped_column(Integer)
    profile_ref: Mapped[str] = mapped_column(String(96), unique=True, index=True)
    python_version: Mapped[str] = mapped_column(String(16))
    # Keep the existing physical column name so deployed databases do not need
    # a destructive rename; the domain/API name distinguishes this immutable
    # container-level target from the user-managed Python runtime label.
    worker_pool: Mapped[str] = mapped_column("node_pool", String(64))
    pip_source: Mapped[str] = mapped_column(String(24), default="default")
    requested_dependencies: Mapped[list[str]] = mapped_column(JSON_VALUE)
    resolved_dependencies: Mapped[dict[str, str]] = mapped_column(
        JSON_VALUE, default=dict
    )
    import_checks: Mapped[list[str]] = mapped_column(JSON_VALUE, default=list)
    environment_digest: Mapped[str | None] = mapped_column(
        String(128), unique=True, nullable=True, index=True
    )
    runtime_env: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, default=dict)
    validation_result: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, default=dict)
    status: Mapped[RuntimeProfileStatus] = mapped_column(
        Enum(RuntimeProfileStatus, native_enum=False),
        default=RuntimeProfileStatus.VALIDATING,
    )
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RevisionRuntimeProfile(Base):
    __tablename__ = "revision_runtime_profiles"

    revision_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("revisions.id", ondelete="CASCADE"), primary_key=True
    )
    runtime_label_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runtime_labels.id", ondelete="RESTRICT"), index=True
    )
    runtime_profile_version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("runtime_profile_versions.id", ondelete="RESTRICT"), index=True
    )
    tracking_mode: Mapped[RuntimeTrackingMode] = mapped_column(
        Enum(RuntimeTrackingMode, native_enum=False),
        default=RuntimeTrackingMode.TRACK_ACTIVE,
    )
    # Retain the deployed column name for an additive migration. The value now
    # identifies the immutable service interface artifact, not its dependencies.
    interface_digest: Mapped[str] = mapped_column("pyproject_digest", String(128))
    validation_result: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class Endpoint(Base):
    __tablename__ = "endpoints"
    __table_args__ = (
        UniqueConstraint("revision_id", "endpoint_id", name="uq_endpoint_revision_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    revision_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("revisions.id", ondelete="CASCADE"), index=True
    )
    endpoint_id: Mapped[str] = mapped_column(String(128))
    task_type: Mapped[str] = mapped_column(String(16))
    entrypoint: Mapped[str] = mapped_column(String(512))
    request_schema: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, default=dict)
    response_schema: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE, default=dict)


class ApiContract(Base):
    __tablename__ = "api_contracts"
    __table_args__ = (
        UniqueConstraint(
            "service_id", "schema_digest", name="uq_contract_service_digest"
        ),
        UniqueConstraint(
            "service_id", "contract_version", name="uq_contract_service_version"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("services.id", ondelete="CASCADE"), index=True
    )
    schema_digest: Mapped[str] = mapped_column(String(128))
    source_digest: Mapped[str] = mapped_column(String(128))
    contract_version: Mapped[str] = mapped_column(String(64))
    descriptor_uri: Mapped[str] = mapped_column(Text)
    proto_bundle_uri: Mapped[str] = mapped_column(Text)
    proto_bundle_digest: Mapped[str | None] = mapped_column(
        String(128), nullable=True
    )
    methods: Mapped[list[str]] = mapped_column(JSON_VALUE)
    status: Mapped[ContractStatus] = mapped_column(
        Enum(ContractStatus, native_enum=False), default=ContractStatus.READY
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class RevisionContract(Base):
    __tablename__ = "revision_contracts"

    revision_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("revisions.id", ondelete="CASCADE"), primary_key=True
    )
    contract_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("api_contracts.id", ondelete="RESTRICT"), index=True
    )


class Invocation(Base):
    __tablename__ = "invocations"
    __table_args__ = (
        Index("ix_invocations_service_created", "service_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    service_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("services.id", ondelete="RESTRICT")
    )
    revision_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("revisions.id", ondelete="RESTRICT")
    )
    endpoint_id: Mapped[str] = mapped_column(String(128))
    status: Mapped[InvocationStatus] = mapped_column(
        Enum(InvocationStatus, native_enum=False), default=InvocationStatus.ACCEPTED
    )
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class InvocationExecution(Base):
    __tablename__ = "invocation_executions"
    __table_args__ = (
        Index(
            "ix_invocation_execution_environment",
            "environment_digest",
            "execution_kind",
        ),
    )

    invocation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("invocations.id", ondelete="CASCADE"), primary_key=True
    )
    runtime_profile_version_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("runtime_profile_versions.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )
    runtime_profile_ref: Mapped[str] = mapped_column(String(256))
    environment_digest: Mapped[str | None] = mapped_column(
        String(128), nullable=True, index=True
    )
    artifact_digest: Mapped[str] = mapped_column(String(128))
    execution_kind: Mapped[ExecutionKind] = mapped_column(
        Enum(ExecutionKind, native_enum=False)
    )
    log_bytes: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    logs_truncated: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class InvocationLog(Base):
    __tablename__ = "invocation_logs"
    __table_args__ = (
        UniqueConstraint(
            "invocation_id", "sequence", name="uq_invocation_log_sequence"
        ),
        Index("ix_invocation_logs_invocation_sequence", "invocation_id", "sequence"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    invocation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("invocations.id", ondelete="CASCADE"), index=True
    )
    sequence: Mapped[int] = mapped_column(Integer)
    stream: Mapped[str] = mapped_column(String(16))
    content: Mapped[str] = mapped_column(Text)
    emitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_outbox_unpublished", "published_at", "created_at"),)

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    topic: Mapped[str] = mapped_column(String(128))
    aggregate_id: Mapped[str] = mapped_column(String(128))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
