from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Literal

from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.version import InvalidVersion, Version
from pydantic import AliasChoices, BaseModel, Field, field_validator, model_validator

SERVICE_NAME = re.compile(r"^[a-z][a-z0-9_-]{1,63}$")
PROTO_FULL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+$")
PROTO_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RUNTIME_LABEL_NAME = re.compile(r"^[a-z][a-z0-9_-]{1,63}$")
IMPORT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
RUNTIME_PROFILE_REF = re.compile(
    r"^[a-z][a-z0-9_-]{1,63}(?:@(?:latest|v[1-9][0-9]*))?$"
)


class CreateServiceRequest(BaseModel):
    name: str
    git_url: str
    tracking_mode: Literal["manual", "poll", "webhook"] = "manual"
    check_interval_seconds: int | None = Field(default=None, ge=10)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not SERVICE_NAME.fullmatch(value):
            raise ValueError("use 2-64 lowercase letters, digits, '_' or '-'")
        return value

    @model_validator(mode="after")
    def validate_tracking_interval(self) -> CreateServiceRequest:
        if self.tracking_mode == "poll" and self.check_interval_seconds is None:
            raise ValueError(
                "check_interval_seconds is required when tracking_mode is 'poll'"
            )
        if self.tracking_mode != "poll" and self.check_interval_seconds is not None:
            raise ValueError(
                "check_interval_seconds is only valid when tracking_mode is 'poll'"
            )
        return self


class UpdateServiceRequest(BaseModel):
    git_url: str | None = Field(default=None, min_length=1)
    tracking_mode: Literal["manual", "poll", "webhook"] | None = None
    check_interval_seconds: int | None = Field(default=None, ge=10)

    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def require_changes(self) -> UpdateServiceRequest:
        if not self.model_fields_set:
            raise ValueError("at least one service property must be provided")
        if self.git_url is not None and not self.git_url.strip():
            raise ValueError("git_url cannot be blank")
        return self


class GrpcEndpointSpec(BaseModel):
    """Native unary gRPC route metadata for one script endpoint."""

    service: str
    method: str
    descriptor_path: str = "descriptor.pb"
    generated: bool = False
    response_wrapped: bool = False

    model_config = {"extra": "forbid"}

    @field_validator("service")
    @classmethod
    def validate_service(cls, value: str) -> str:
        if not PROTO_FULL_NAME.fullmatch(value):
            raise ValueError("grpc service must be a fully-qualified protobuf name")
        return value

    @field_validator("method")
    @classmethod
    def validate_method(cls, value: str) -> str:
        if not PROTO_IDENTIFIER.fullmatch(value):
            raise ValueError("grpc method must be a protobuf identifier")
        return value

    @field_validator("descriptor_path")
    @classmethod
    def validate_descriptor_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        parts = normalized.split("/")
        if not normalized or normalized.startswith("/") or ".." in parts:
            raise ValueError("descriptor_path must be a safe artifact-relative path")
        return normalized

    @property
    def method_path(self) -> str:
        return f"/{self.service}/{self.method}"


class EndpointSpec(BaseModel):
    id: str
    task_type: Literal["io", "compute"]
    entrypoint: str
    io_type: list[Literal["rest", "grpc"]] = Field(
        default_factory=lambda: ["rest"], min_length=1
    )
    parameters: dict[str, dict[str, Any]] = Field(default_factory=dict)
    response_schema: dict[str, Any] = Field(default_factory=dict)
    grpc: GrpcEndpointSpec | None = None
    num_cpus: float | None = Field(default=None, gt=0)
    num_gpus: float | None = Field(default=None, ge=0)

    model_config = {"extra": "forbid"}

    @field_validator("entrypoint")
    @classmethod
    def validate_entrypoint(cls, value: str) -> str:
        module, separator, function = value.partition(":")
        if not separator or not module or not function:
            raise ValueError("entrypoint must use 'package.module:function'")
        return value

    @field_validator("io_type")
    @classmethod
    def unique_io_types(
        cls, values: list[Literal["rest", "grpc"]]
    ) -> list[Literal["rest", "grpc"]]:
        if len(values) != len(set(values)):
            raise ValueError("io_type values must be unique")
        return values

    @model_validator(mode="after")
    def compute_resources_only(self) -> EndpointSpec:
        if self.grpc is not None and "io_type" not in self.model_fields_set:
            self.io_type = ["grpc"]
        if self.task_type != "compute" and (
            self.num_cpus is not None or self.num_gpus is not None
        ):
            raise ValueError("num_cpus and num_gpus are only valid for compute tasks")
        return self

    @property
    def request_schema(self) -> dict[str, Any]:
        """Return the internal HTTP object schema for the flat parameters."""

        return {
            "type": "Struct",
            "required": list(self.parameters),
            "additionalProperties": False,
            "properties": self.parameters,
        }

    @classmethod
    def from_manifest(cls, value: dict[str, Any]) -> EndpointSpec:
        metadata_fields = {
            "id",
            "task_type",
            "entrypoint",
            "io_type",
            "response_schema",
            "grpc",
            "num_cpus",
            "num_gpus",
        }
        return cls.model_validate(
            {
                **{
                    name: item
                    for name, item in value.items()
                    if name in metadata_fields
                },
                "parameters": {
                    name: item
                    for name, item in value.items()
                    if name not in metadata_fields
                },
            }
        )

    def to_manifest(self) -> dict[str, Any]:
        """Serialize endpoint metadata with function parameters kept flat."""

        manifest = self.model_dump(
            mode="json",
            exclude={"parameters"},
            exclude_none=True,
        )
        manifest.update(self.parameters)
        return manifest


class GrpcContractSpec(BaseModel):
    version: str
    proto_root: str = "proto"
    # Accepted only so historical manifests remain readable. Proto publication
    # no longer builds or names a Python distribution.
    package_name: str | None = None

    model_config = {"extra": "forbid"}

    @field_validator("version")
    @classmethod
    def validate_version(cls, value: str) -> str:
        try:
            normalized = str(Version(value))
        except InvalidVersion as error:
            raise ValueError(
                "contract version must be a valid Python package version"
            ) from error
        return normalized

    @field_validator("proto_root")
    @classmethod
    def validate_proto_root(cls, value: str) -> str:
        normalized = value.replace("\\", "/").strip("/")
        if not normalized or ".." in normalized.split("/"):
            raise ValueError("proto_root must be a safe artifact-relative directory")
        return normalized

    @field_validator("package_name")
    @classmethod
    def validate_package_name(cls, value: str | None) -> str | None:
        if value is not None and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value):
            raise ValueError("package_name is not a valid Python distribution name")
        return value


class CreateRevisionRequest(BaseModel):
    revision: str = Field(min_length=1, max_length=128)
    artifact_uri: str
    artifact_digest: str = Field(min_length=16, max_length=128)

    model_config = {"extra": "forbid"}


class RevisionInterfaceSpec(BaseModel):
    endpoints: list[EndpointSpec] = Field(min_length=1)
    grpc_contract: GrpcContractSpec | None = None
    runtime_profile: str
    requires_python: str
    dependencies: list[str] = Field(default_factory=list)

    model_config = {"extra": "forbid"}

    @field_validator("runtime_profile")
    @classmethod
    def validate_runtime_profile(cls, value: str) -> str:
        if not RUNTIME_PROFILE_REF.fullmatch(value):
            raise ValueError(
                "runtime label must use '<label>', '<label>@latest', or "
                "'<label>@v<version>'"
            )
        return value

    @field_validator("requires_python")
    @classmethod
    def validate_requires_python(cls, value: str) -> str:
        try:
            SpecifierSet(value)
        except InvalidSpecifier as error:
            raise ValueError("project.requires-python is not valid") from error
        return value

    @field_validator("dependencies")
    @classmethod
    def validate_project_dependencies(cls, values: list[str]) -> list[str]:
        for value in values:
            try:
                Requirement(value)
            except InvalidRequirement as error:
                raise ValueError(f"invalid project dependency: {value}") from error
        return values

    @field_validator("endpoints")
    @classmethod
    def unique_endpoint_ids(cls, endpoints: list[EndpointSpec]) -> list[EndpointSpec]:
        ids = [endpoint.id for endpoint in endpoints]
        if len(ids) != len(set(ids)):
            raise ValueError("endpoint ids must be unique within a revision")
        method_paths = [
            endpoint.grpc.method_path for endpoint in endpoints if endpoint.grpc
        ]
        if len(method_paths) != len(set(method_paths)):
            raise ValueError("gRPC method paths must be unique within a revision")
        return endpoints

    @model_validator(mode="after")
    def require_contract_for_grpc(self) -> RevisionInterfaceSpec:
        legacy_grpc = [endpoint for endpoint in self.endpoints if endpoint.grpc]
        generated_grpc = [
            endpoint
            for endpoint in self.endpoints
            if "grpc" in endpoint.io_type and endpoint.grpc is None
        ]
        if legacy_grpc and generated_grpc:
            raise ValueError(
                "legacy grpc metadata cannot be mixed with generated grpc endpoints"
            )
        if legacy_grpc and self.grpc_contract is None:
            raise ValueError(
                "grpc_contract is required when gRPC endpoints are declared"
            )
        if generated_grpc and self.grpc_contract is not None:
            raise ValueError(
                "grpc_contract is generated automatically when io_type contains grpc"
            )
        missing_responses = [
            endpoint.id
            for endpoint in generated_grpc
            if not endpoint.response_schema
        ]
        if missing_responses:
            raise ValueError(
                "generated gRPC endpoints require response_schema: "
                + ", ".join(missing_responses)
            )
        descriptor_paths = {
            endpoint.grpc.descriptor_path
            for endpoint in legacy_grpc
            if endpoint.grpc is not None
        }
        if len(descriptor_paths) > 1:
            raise ValueError(
                "all gRPC endpoints in a revision must share one descriptor"
            )
        return self


class ResolvedRevisionRequest(CreateRevisionRequest):
    endpoints: list[EndpointSpec] = Field(min_length=1)
    grpc_contract: GrpcContractSpec | None = None
    runtime_profile: str
    requires_python: str
    dependencies: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_resolved_interface(self) -> ResolvedRevisionRequest:
        RevisionInterfaceSpec(
            endpoints=self.endpoints,
            grpc_contract=self.grpc_contract,
            runtime_profile=self.runtime_profile,
            requires_python=self.requires_python,
            dependencies=self.dependencies,
        )
        return self


class RuntimeProfileSpecRequest(BaseModel):
    python_version: str = Field(default="3.12", pattern=r"^\d+\.\d+$")
    worker_pool: str = Field(
        default="default",
        min_length=1,
        max_length=64,
        validation_alias=AliasChoices("worker_pool", "node_pool"),
    )
    dependencies: list[str] = Field(default_factory=list, max_length=256)
    import_checks: list[str] = Field(default_factory=list, max_length=128)
    pip_source: Literal["default", "private"] = "default"

    @field_validator("dependencies")
    @classmethod
    def validate_dependencies(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        names: set[str] = set()
        for value in values:
            try:
                requirement = Requirement(value.strip())
            except InvalidRequirement as error:
                raise ValueError(f"invalid dependency {value!r}") from error
            if requirement.name.lower() in names:
                raise ValueError(f"duplicate dependency {requirement.name!r}")
            names.add(requirement.name.lower())
            if requirement.url:
                if "#sha256=" not in requirement.url:
                    raise ValueError("URL dependencies must include a sha256 fragment")
            elif not any(
                item.operator in {"==", "==="} and "*" not in item.version
                for item in requirement.specifier
            ):
                raise ValueError(
                    f"dependency {requirement.name!r} must pin an exact version"
                )
            normalized.append(str(requirement))
        return sorted(normalized, key=str.lower)

    @field_validator("import_checks")
    @classmethod
    def validate_import_checks(cls, values: list[str]) -> list[str]:
        if any(not IMPORT_NAME.fullmatch(value) for value in values):
            raise ValueError("import checks must contain Python module names")
        return sorted(set(values))


class CreateRuntimeLabelRequest(RuntimeProfileSpecRequest):
    name: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not RUNTIME_LABEL_NAME.fullmatch(value):
            raise ValueError("use 2-64 lowercase letters, digits, '_' or '-'")
        return value


class CreateRuntimeProfileVersionRequest(RuntimeProfileSpecRequest):
    pass


class RuntimeProfileVersionResponse(BaseModel):
    id: uuid.UUID
    label_id: uuid.UUID
    label_name: str
    version: int
    profile_ref: str
    python_version: str
    worker_pool: str
    pip_source: str
    requested_dependencies: list[str]
    resolved_dependencies: dict[str, str]
    import_checks: list[str]
    environment_digest: str | None
    status: str
    error: str | None
    validation_result: dict[str, Any]
    reference_count: int = 0
    created_at: datetime
    validated_at: datetime | None
    retired_at: datetime | None


class RuntimeLabelResponse(BaseModel):
    id: uuid.UUID
    name: str
    active_version_id: uuid.UUID | None
    created_at: datetime
    versions: list[RuntimeProfileVersionResponse]


class WorkerPoolResponse(BaseModel):
    name: str
    label_key: str
    node_count: int
    source: Literal["KUBERAY", "RAY", "CONFIG"]
    mutable: Literal[False] = False


class ServiceResponse(BaseModel):
    id: uuid.UUID
    name: str
    git_url: str
    tracking_mode: str
    check_interval_seconds: int | None
    status: str
    active_revision_id: uuid.UUID | None
    webhook_enabled: bool = False
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class RevisionResponse(BaseModel):
    id: uuid.UUID
    service_id: uuid.UUID
    revision: str
    runtime_profile: str
    status: str

    model_config = {"from_attributes": True}


class RevisionDetailResponse(BaseModel):
    id: uuid.UUID
    service_id: uuid.UUID
    revision: str
    artifact_uri: str
    artifact_digest: str
    runtime_profile: str
    status: str
    endpoints: list[dict[str, Any]]
    created_at: datetime
    activated_at: datetime | None


class ServiceDetailResponse(ServiceResponse):
    revision_count: int
    active_revision: RevisionDetailResponse | None
    endpoints: list[dict[str, Any]]


class WebhookConfigResponse(BaseModel):
    enabled: bool
    url: str | None = None
    configured_at: datetime | None = None


class InvocationListItemResponse(BaseModel):
    id: uuid.UUID
    service_id: uuid.UUID
    service: str
    revision_id: uuid.UUID
    revision: str
    endpoint_id: str
    status: str
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    execution_kind: str | None = None
    runtime_profile: str | None = None
    environment_digest: str | None = None
    has_logs: bool = False
    log_bytes: int = 0
    logs_truncated: bool = False


class InvocationLogResponse(BaseModel):
    sequence: int
    stream: Literal["STDOUT", "STDERR"]
    content: str
    emitted_at: datetime
    created_at: datetime


class InvocationResponse(BaseModel):
    request_id: uuid.UUID
    service: str
    revision: str
    result: Any


class ContractResponse(BaseModel):
    id: uuid.UUID
    service: str
    revision: str | None = None
    contract_version: str
    schema_digest: str
    source_digest: str
    proto_bundle_digest: str
    methods: list[str]
    proto_bundle_url: str
