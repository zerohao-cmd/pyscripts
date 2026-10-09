from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from packaging.version import Version
from sqlalchemy import delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from pyscripts.models import (
    AdminCredential,
    ApiAccessToken,
    ApiContract,
    ContractStatus,
    Endpoint,
    EndpointInvocationPolicy,
    ExecutionKind,
    Invocation,
    InvocationExecution,
    InvocationLog,
    InvocationStatus,
    OutboxEvent,
    Revision,
    RevisionContract,
    RevisionRuntimeProfile,
    RevisionStatus,
    RuntimeLabel,
    RuntimeProfileStatus,
    RuntimeProfileVersion,
    RuntimeTrackingMode,
    Service,
    ServiceStatus,
)
from pyscripts.schemas import (
    CreateRuntimeLabelRequest,
    CreateRuntimeProfileVersionRequest,
    CreateServiceRequest,
    ResolvedRevisionRequest,
    UpdateServiceRequest,
    UpdateEndpointInvocationPolicyRequest,
)
from pyscripts.runtime.profiles import (
    RuntimeProfileCompatibilityError,
    validate_project_compatibility,
)
from pyscripts.runtime.output import CapturedLogChunk


class NotFoundError(LookupError):
    pass


class InvalidTransitionError(RuntimeError):
    pass


# Serializes publication of the process-wide gRPC route namespace. Service
# row locks alone cannot prevent two different services from concurrently
# validating and activating the same method path.
_GRPC_ROUTE_ACTIVATION_LOCK = 0x50595343525043
CURRENT_INTERFACE_SPEC_VERSION = 2


@dataclass(frozen=True, slots=True)
class ResolvedEndpoint:
    service_id: uuid.UUID
    service_name: str
    revision_id: uuid.UUID
    revision: str
    artifact_uri: str
    artifact_digest: str
    runtime_profile: str
    endpoint_manifest: list[dict[str, Any]]
    endpoint_id: str
    task_type: str
    entrypoint: str
    grpc: dict[str, str] | None = None
    environment_digest: str | None = None
    runtime_env: dict[str, Any] | None = None
    worker_pool: str | None = None
    runtime_profile_version_id: uuid.UUID | None = None
    num_cpus: float | None = None
    num_gpus: float | None = None
    io_type: tuple[str, ...] = ("rest",)
    pip_source: str = "default"
    pip_index_url: str | None = None


@dataclass(frozen=True, slots=True)
class ContractBundle:
    service: Service
    contract: ApiContract
    revision: Revision | None = None


@dataclass(frozen=True, slots=True)
class InvocationRecord:
    invocation: Invocation
    service_name: str
    revision: str
    execution: InvocationExecution | None = None


@dataclass(frozen=True, slots=True)
class RuntimeProfileRecord:
    label: RuntimeLabel
    version: RuntimeProfileVersion
    reference_count: int = 0


class PlatformRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def _supports_current_interface(revision: Revision) -> bool:
        manifest = revision.manifest
        return (
            isinstance(manifest, dict)
            and manifest.get("spec_version") == CURRENT_INTERFACE_SPEC_VERSION
        )

    @classmethod
    def _require_current_interface(cls, revision: Revision) -> None:
        if cls._supports_current_interface(revision):
            return
        raise InvalidTransitionError(
            "revision uses an unsupported interface specification; "
            "publish a Hook-based spec_version=2 revision"
        )

    async def stop_services_with_unsupported_interfaces(self) -> list[str]:
        """Disable legacy active revisions before rebuilding public routes."""

        rows = await self.session.execute(
            select(Service, Revision)
            .join(Revision, Revision.id == Service.active_revision_id)
            .where(
                Service.status == ServiceStatus.ACTIVE,
                Revision.status == RevisionStatus.ACTIVE,
            )
            .with_for_update()
        )
        stopped: list[str] = []
        now = datetime.now(UTC)
        for service, revision in rows:
            if self._supports_current_interface(revision):
                continue
            service.status = ServiceStatus.STOPPED
            service.updated_at = now
            stopped.append(service.name)
            self.session.add(
                OutboxEvent(
                    topic="service.stopped",
                    aggregate_id=str(service.id),
                    payload={
                        "service_id": str(service.id),
                        "reason": "unsupported_interface_specification",
                    },
                )
            )
        await self.session.flush()
        return stopped

    async def create_service(
        self,
        request: CreateServiceRequest,
    ) -> Service:
        service = Service(
            name=request.name,
            git_url=request.git_url,
            git_branch=request.git_branch,
            tracking_mode=request.tracking_mode,
            check_interval_seconds=request.check_interval_seconds,
            runtime_profile=None,
            runtime_tracking_mode=None,
        )
        self.session.add(service)
        await self.session.flush()
        return service

    async def get_admin_credential(
        self,
        username: str = "admin",
        *,
        for_update: bool = False,
    ) -> AdminCredential | None:
        statement = select(AdminCredential).where(
            AdminCredential.username == username
        )
        if for_update:
            statement = statement.with_for_update()
        return await self.session.scalar(statement)

    async def create_admin_credential(
        self,
        password_hash: str,
        username: str = "admin",
    ) -> AdminCredential:
        credential = AdminCredential(
            username=username,
            password_hash=password_hash,
            session_version=1,
        )
        self.session.add(credential)
        await self.session.flush()
        return credential

    async def update_admin_password(
        self,
        credential: AdminCredential,
        password_hash: str,
    ) -> AdminCredential:
        credential.password_hash = password_hash
        credential.session_version += 1
        credential.updated_at = datetime.now(UTC)
        await self.session.flush()
        return credential

    async def list_services(self) -> list[Service]:
        result = await self.session.scalars(select(Service).order_by(Service.name))
        return list(result)

    async def create_api_token(
        self,
        *,
        name: str,
        token_hash: str,
        token_prefix: str,
    ) -> ApiAccessToken:
        token = ApiAccessToken(
            name=name,
            token_hash=token_hash,
            token_prefix=token_prefix,
        )
        self.session.add(token)
        await self.session.flush()
        return token

    async def list_api_tokens(self) -> list[ApiAccessToken]:
        result = await self.session.scalars(
            select(ApiAccessToken)
            .where(ApiAccessToken.revoked_at.is_(None))
            .order_by(ApiAccessToken.created_at.desc())
        )
        return list(result)

    async def revoke_api_token(self, token_id: uuid.UUID) -> ApiAccessToken:
        token = await self.session.get(ApiAccessToken, token_id)
        if token is None:
            raise NotFoundError("API token does not exist")
        if token.revoked_at is None:
            token.revoked_at = datetime.now(UTC)
            await self.session.flush()
        return token

    async def validate_api_token(self, token_hash: str) -> bool:
        token_id = await self.session.scalar(
            select(ApiAccessToken.id).where(
                ApiAccessToken.token_hash == token_hash,
                ApiAccessToken.revoked_at.is_(None),
            )
        )
        return token_id is not None

    async def get_service(self, service_id: uuid.UUID) -> Service:
        service = await self.session.get(Service, service_id)
        if service is None:
            raise NotFoundError("service does not exist")
        return service

    async def delete_service(self, service_id: uuid.UUID) -> str:
        service = await self.session.scalar(
            select(Service).where(Service.id == service_id).with_for_update()
        )
        if service is None:
            raise NotFoundError("service does not exist")
        if service.status == ServiceStatus.ACTIVE:
            raise InvalidTransitionError(
                "active service cannot be deleted; stop it first"
            )

        active_invocations = int(
            await self.session.scalar(
                select(func.count())
                .select_from(Invocation)
                .where(
                    Invocation.service_id == service_id,
                    Invocation.status.in_(
                        [InvocationStatus.ACCEPTED, InvocationStatus.RUNNING]
                    ),
                )
            )
            or 0
        )
        if active_invocations:
            raise InvalidTransitionError(
                "service still has active invocations; wait for them to finish"
            )

        invocation_ids = select(Invocation.id).where(
            Invocation.service_id == service_id
        )
        revision_ids = select(Revision.id).where(Revision.service_id == service_id)
        await self.session.execute(
            delete(InvocationLog).where(
                InvocationLog.invocation_id.in_(invocation_ids)
            )
        )
        await self.session.execute(
            delete(InvocationExecution).where(
                InvocationExecution.invocation_id.in_(invocation_ids)
            )
        )
        await self.session.execute(
            delete(Invocation).where(Invocation.service_id == service_id)
        )
        await self.session.execute(
            delete(RevisionContract).where(
                RevisionContract.revision_id.in_(revision_ids)
            )
        )
        await self.session.execute(
            delete(Endpoint).where(Endpoint.revision_id.in_(revision_ids))
        )
        await self.session.execute(
            delete(RevisionRuntimeProfile).where(
                RevisionRuntimeProfile.revision_id.in_(revision_ids)
            )
        )
        await self.session.execute(
            delete(EndpointInvocationPolicy).where(
                EndpointInvocationPolicy.service_id == service_id
            )
        )
        await self.session.execute(
            delete(Revision).where(Revision.service_id == service_id)
        )
        await self.session.execute(
            delete(ApiContract).where(ApiContract.service_id == service_id)
        )
        await self.session.delete(service)
        await self.session.flush()
        return service.name

    async def update_service(
        self,
        service_id: uuid.UUID,
        request: UpdateServiceRequest,
    ) -> Service:
        service = await self.session.scalar(
            select(Service).where(Service.id == service_id).with_for_update()
        )
        if service is None:
            raise NotFoundError("service does not exist")

        if "git_url" in request.model_fields_set:
            assert request.git_url is not None
            service.git_url = request.git_url.strip()
        if "git_branch" in request.model_fields_set:
            service.git_branch = request.git_branch
        next_tracking_mode = request.tracking_mode or service.tracking_mode
        next_interval = (
            request.check_interval_seconds
            if "check_interval_seconds" in request.model_fields_set
            else service.check_interval_seconds
        )
        if next_tracking_mode == "poll" and next_interval is None:
            raise InvalidTransitionError(
                "check_interval_seconds is required when tracking_mode is 'poll'"
            )
        if (
            next_tracking_mode != "poll"
            and "check_interval_seconds" in request.model_fields_set
            and request.check_interval_seconds is not None
        ):
            raise InvalidTransitionError(
                "check_interval_seconds is only valid when tracking_mode is 'poll'"
            )
        if next_tracking_mode != "poll":
            next_interval = None
        service.tracking_mode = next_tracking_mode
        service.check_interval_seconds = next_interval
        service.updated_at = datetime.now(UTC)
        await self.session.flush()
        return service

    async def endpoint_invocation_retention_counts(
        self,
        service_id: uuid.UUID,
        endpoint_ids: list[str],
    ) -> dict[str, int]:
        if not endpoint_ids:
            return {}
        rows = await self.session.execute(
            select(
                EndpointInvocationPolicy.endpoint_id,
                EndpointInvocationPolicy.retention_count,
            ).where(
                EndpointInvocationPolicy.service_id == service_id,
                EndpointInvocationPolicy.endpoint_id.in_(endpoint_ids),
            )
        )
        configured = dict(rows.all())
        return {endpoint_id: configured.get(endpoint_id, 100) for endpoint_id in endpoint_ids}

    async def update_endpoint_invocation_policy(
        self,
        service_id: uuid.UUID,
        endpoint_id: str,
        request: UpdateEndpointInvocationPolicyRequest,
    ) -> EndpointInvocationPolicy:
        service = await self.get_service(service_id)
        if service.active_revision_id is None:
            raise NotFoundError("service has no active endpoints")
        exists = await self.session.scalar(
            select(Endpoint.id).where(
                Endpoint.revision_id == service.active_revision_id,
                Endpoint.endpoint_id == endpoint_id,
            )
        )
        if exists is None:
            raise NotFoundError("active service endpoint does not exist")
        policy = await self.session.get(
            EndpointInvocationPolicy,
            (service_id, endpoint_id),
        )
        if policy is None:
            policy = EndpointInvocationPolicy(
                service_id=service_id,
                endpoint_id=endpoint_id,
                retention_count=request.retention_count,
            )
            self.session.add(policy)
        else:
            policy.retention_count = request.retention_count
            policy.updated_at = datetime.now(UTC)
        await self.session.flush()
        await self._prune_endpoint_invocations(
            service_id,
            endpoint_id,
            request.retention_count,
        )
        return policy

    async def configure_service_webhook(
        self,
        service_id: uuid.UUID,
        token_hash: str | None,
    ) -> Service:
        service = await self.session.scalar(
            select(Service).where(Service.id == service_id).with_for_update()
        )
        if service is None:
            raise NotFoundError("service does not exist")
        service.webhook_token_hash = token_hash
        service.webhook_configured_at = (
            datetime.now(UTC) if token_hash is not None else None
        )
        service.updated_at = datetime.now(UTC)
        await self.session.flush()
        return service

    async def create_runtime_label(
        self,
        request: CreateRuntimeLabelRequest,
    ) -> RuntimeProfileRecord:
        if await self.session.scalar(
            select(RuntimeLabel.id).where(RuntimeLabel.name == request.name)
        ):
            raise InvalidTransitionError("runtime label already exists")
        label = RuntimeLabel(name=request.name)
        self.session.add(label)
        await self.session.flush()
        version = self._new_runtime_version(label, 1, request)
        self.session.add(version)
        await self.session.flush()
        return RuntimeProfileRecord(label, version)

    async def create_runtime_version(
        self,
        label_id: uuid.UUID,
        request: CreateRuntimeProfileVersionRequest,
    ) -> RuntimeProfileRecord:
        label = await self.session.scalar(
            select(RuntimeLabel).where(RuntimeLabel.id == label_id).with_for_update()
        )
        if label is None:
            raise NotFoundError("runtime label does not exist")
        latest = await self.session.scalar(
            select(func.max(RuntimeProfileVersion.version)).where(
                RuntimeProfileVersion.label_id == label_id
            )
        )
        version = self._new_runtime_version(label, int(latest or 0) + 1, request)
        self.session.add(version)
        await self.session.flush()
        return RuntimeProfileRecord(label, version)

    @staticmethod
    def _new_runtime_version(
        label: RuntimeLabel,
        version: int,
        request: CreateRuntimeLabelRequest | CreateRuntimeProfileVersionRequest,
    ) -> RuntimeProfileVersion:
        dependencies = list(request.dependencies)
        runtime_env: dict[str, Any] = {
            "pip": {"packages": dependencies, "pip_check": True}
        } if dependencies else {}
        return RuntimeProfileVersion(
            label_id=label.id,
            version=version,
            profile_ref=f"{label.name}@v{version}",
            python_version=request.python_version,
            worker_pool=request.worker_pool,
            pip_source=request.pip_source,
            pip_index_url=request.pip_index_url,
            requested_dependencies=dependencies,
            import_checks=list(request.import_checks),
            runtime_env=runtime_env,
            status=RuntimeProfileStatus.VALIDATING,
        )

    async def list_runtime_profiles(self) -> list[RuntimeProfileRecord]:
        reference_counts = (
            select(
                RevisionRuntimeProfile.runtime_profile_version_id.label("version_id"),
                func.count(func.distinct(Revision.service_id)).label(
                    "reference_count"
                ),
            )
            .join(Revision, Revision.id == RevisionRuntimeProfile.revision_id)
            .group_by(RevisionRuntimeProfile.runtime_profile_version_id)
            .subquery()
        )
        rows = await self.session.execute(
            select(
                RuntimeLabel,
                RuntimeProfileVersion,
                func.coalesce(reference_counts.c.reference_count, 0),
            )
            .join(
                RuntimeProfileVersion,
                RuntimeProfileVersion.label_id == RuntimeLabel.id,
            )
            .outerjoin(
                reference_counts,
                reference_counts.c.version_id == RuntimeProfileVersion.id,
            )
            .order_by(RuntimeLabel.name, RuntimeProfileVersion.version.desc())
        )
        return [
            RuntimeProfileRecord(label, version, int(reference_count))
            for label, version, reference_count in rows
        ]

    async def get_runtime_profile(
        self,
        value: str | uuid.UUID,
        *,
        require_usable: bool = False,
    ) -> RuntimeProfileRecord:
        if isinstance(value, uuid.UUID):
            condition = RuntimeProfileVersion.id == value
        elif "@v" in value:
            condition = RuntimeProfileVersion.profile_ref == value
        else:
            label_name = value.removesuffix("@latest")
            condition = RuntimeLabel.name == label_name
        statement = (
            select(RuntimeLabel, RuntimeProfileVersion)
            .join(
                RuntimeProfileVersion,
                RuntimeProfileVersion.label_id == RuntimeLabel.id,
            )
            .where(condition)
        )
        if not isinstance(value, uuid.UUID) and "@v" not in value:
            statement = statement.where(
                RuntimeProfileVersion.id == RuntimeLabel.active_version_id
            )
        row = (await self.session.execute(statement)).one_or_none()
        if row is None:
            raise NotFoundError("runtime profile does not exist or has no active version")
        label, version = row
        if require_usable and version.status not in {
            RuntimeProfileStatus.READY,
            RuntimeProfileStatus.ACTIVE,
        }:
            raise InvalidTransitionError(
                f"runtime profile is in {version.status.value} state"
            )
        count = await self.runtime_profile_reference_count(version.id)
        return RuntimeProfileRecord(label, version, count)

    async def complete_runtime_profile_validation(
        self,
        version_id: uuid.UUID,
        *,
        resolved_dependencies: dict[str, str],
        validation_result: dict[str, Any],
        runtime_env: dict[str, Any],
        environment_digest: str,
    ) -> RuntimeProfileRecord:
        version = await self.session.get(RuntimeProfileVersion, version_id)
        if version is None:
            raise NotFoundError("runtime profile version does not exist")
        if version.status != RuntimeProfileStatus.VALIDATING:
            raise InvalidTransitionError("runtime profile is not validating")
        version.resolved_dependencies = resolved_dependencies
        version.validation_result = validation_result
        version.runtime_env = runtime_env
        version.environment_digest = environment_digest
        version.status = RuntimeProfileStatus.READY
        version.validated_at = datetime.now(UTC)
        version.error = None
        label = await self.session.get(RuntimeLabel, version.label_id)
        assert label is not None
        await self.session.flush()
        return RuntimeProfileRecord(label, version)

    async def fail_runtime_profile_validation(
        self,
        version_id: uuid.UUID,
        error: str,
    ) -> None:
        version = await self.session.get(RuntimeProfileVersion, version_id)
        if version is None:
            raise NotFoundError("runtime profile version does not exist")
        version.status = RuntimeProfileStatus.FAILED
        version.error = error[:4000]
        version.validated_at = datetime.now(UTC)
        await self.session.flush()

    async def activate_runtime_profile(
        self,
        version_id: uuid.UUID,
    ) -> tuple[RuntimeProfileRecord, RuntimeProfileVersion | None, int]:
        version = await self.session.get(RuntimeProfileVersion, version_id)
        if version is None:
            raise NotFoundError("runtime profile version does not exist")
        if version.status not in {
            RuntimeProfileStatus.READY,
            RuntimeProfileStatus.ACTIVE,
        }:
            raise InvalidTransitionError(
                f"cannot activate profile in {version.status.value} state"
            )
        label = await self.session.scalar(
            select(RuntimeLabel)
            .where(RuntimeLabel.id == version.label_id)
            .with_for_update()
        )
        assert label is not None
        old = (
            await self.session.get(RuntimeProfileVersion, label.active_version_id)
            if label.active_version_id
            else None
        )
        bindings = list(
            await self.session.scalars(
                select(RevisionRuntimeProfile).where(
                    RevisionRuntimeProfile.runtime_label_id == label.id,
                    RevisionRuntimeProfile.tracking_mode
                    == RuntimeTrackingMode.TRACK_ACTIVE,
                )
            )
        )
        target = RuntimeProfileRecord(label, version)
        for binding in bindings:
            requirements = dict(binding.validation_result or {})
            requires_python = requirements.get("requires_python")
            dependencies = requirements.get("dependencies")
            if not isinstance(requires_python, str) or not isinstance(
                dependencies, list
            ):
                raise InvalidTransitionError(
                    "tracked revision is missing compatibility metadata; "
                    "republish it before activating this environment version"
                )
            try:
                validate_project_compatibility(
                    target,
                    requires_python=requires_python,
                    dependencies=[str(item) for item in dependencies],
                )
            except RuntimeProfileCompatibilityError as error:
                raise InvalidTransitionError(
                    f"cannot activate {version.profile_ref}: tracked revision "
                    f"{binding.revision_id} is incompatible: {error}"
                ) from error

        for binding in bindings:
            binding.runtime_profile_version_id = version.id
            binding.validation_result = {
                **dict(binding.validation_result or {}),
                "resolved_profile_ref": version.profile_ref,
            }
            revision = await self.session.get(Revision, binding.revision_id)
            if revision is not None:
                revision.runtime_profile = version.profile_ref
                manifest = dict(revision.manifest)
                manifest["runtime"] = {
                    **dict(manifest.get("runtime", {})),
                    "label": label.name,
                    "profile_ref": version.profile_ref,
                    "environment_digest": version.environment_digest,
                    "tracking_mode": RuntimeTrackingMode.TRACK_ACTIVE.value,
                }
                revision.manifest = manifest

        label.active_version_id = version.id
        version.status = RuntimeProfileStatus.ACTIVE
        self.session.add(
            OutboxEvent(
                topic="runtime-profile.activated",
                aggregate_id=str(label.id),
                payload={
                    "label": label.name,
                    "version_id": str(version.id),
                    "profile_ref": version.profile_ref,
                    "environment_digest": version.environment_digest,
                },
            )
        )
        await self.session.flush()
        old_references = (
            await self.runtime_profile_reference_count(old.id) if old else 0
        )
        if old is not None and old.id != version.id:
            old.status = (
                RuntimeProfileStatus.RETIRING
                if old_references == 0
                else RuntimeProfileStatus.READY
            )
        references = await self.runtime_profile_reference_count(version.id)
        return RuntimeProfileRecord(label, version, references), old, old_references

    async def runtime_profile_reference_count(self, version_id: uuid.UUID) -> int:
        return int(
            await self.session.scalar(
                select(func.count(func.distinct(Revision.service_id)))
                .select_from(RevisionRuntimeProfile)
                .join(Revision, Revision.id == RevisionRuntimeProfile.revision_id)
                .where(
                    RevisionRuntimeProfile.runtime_profile_version_id == version_id
                )
            )
            or 0
        )

    async def retire_runtime_profile(
        self,
        version_id: uuid.UUID,
        *,
        finalize: bool = False,
    ) -> RuntimeProfileRecord:
        version = await self.session.get(RuntimeProfileVersion, version_id)
        if version is None:
            raise NotFoundError("runtime profile version does not exist")
        label = await self.session.get(RuntimeLabel, version.label_id)
        assert label is not None
        if label.active_version_id == version.id:
            raise InvalidTransitionError("active runtime profile cannot be retired")
        references = await self.runtime_profile_reference_count(version.id)
        if references:
            raise InvalidTransitionError(
                f"runtime profile is referenced by {references} service(s)"
            )
        active_executions = (
            await self.active_runtime_execution_count(version.environment_digest)
            if version.environment_digest
            else 0
        )
        if active_executions:
            raise InvalidTransitionError(
                f"runtime profile has {active_executions} active execution(s)"
            )
        if finalize:
            if version.status != RuntimeProfileStatus.RETIRING:
                raise InvalidTransitionError("runtime profile is not retiring")
            version.status = RuntimeProfileStatus.RETIRED
            version.retired_at = datetime.now(UTC)
        else:
            if version.status == RuntimeProfileStatus.RETIRED:
                raise InvalidTransitionError("runtime profile is already retired")
            version.status = RuntimeProfileStatus.RETIRING
        await self.session.flush()
        return RuntimeProfileRecord(label, version, references)

    async def list_revisions(self, service_id: uuid.UUID) -> list[Revision]:
        if await self.session.get(Service, service_id) is None:
            raise NotFoundError("service does not exist")
        result = await self.session.scalars(
            select(Revision)
            .where(Revision.service_id == service_id)
            .order_by(Revision.created_at.desc())
        )
        return list(result)

    async def find_revision(
        self,
        service_id: uuid.UUID,
        revision: str,
    ) -> Revision | None:
        return await self.session.scalar(
            select(Revision).where(
                Revision.service_id == service_id,
                Revision.revision == revision,
            )
        )

    async def list_invocations(
        self,
        service_id: uuid.UUID | None = None,
        limit: int = 100,
    ) -> list[InvocationRecord]:
        statement = (
            select(Invocation, Service.name, Revision.revision, InvocationExecution)
            .join(Service, Service.id == Invocation.service_id)
            .join(Revision, Revision.id == Invocation.revision_id)
            .outerjoin(
                InvocationExecution,
                InvocationExecution.invocation_id == Invocation.id,
            )
            .order_by(Invocation.created_at.desc())
            .limit(limit)
        )
        if service_id is not None:
            if await self.session.get(Service, service_id) is None:
                raise NotFoundError("service does not exist")
            statement = statement.where(Invocation.service_id == service_id)
        rows = await self.session.execute(statement)
        return [
            InvocationRecord(invocation, service_name, revision, execution)
            for invocation, service_name, revision, execution in rows
        ]

    async def list_invocation_logs(
        self,
        invocation_id: uuid.UUID,
        *,
        after_sequence: int = -1,
        limit: int = 500,
    ) -> list[InvocationLog]:
        if await self.session.get(Invocation, invocation_id) is None:
            raise NotFoundError("invocation does not exist")
        result = await self.session.scalars(
            select(InvocationLog)
            .where(
                InvocationLog.invocation_id == invocation_id,
                InvocationLog.sequence > after_sequence,
            )
            .order_by(InvocationLog.sequence)
            .limit(limit)
        )
        return list(result)

    async def register_contract(
        self,
        service_id: uuid.UUID,
        published: Any,
    ) -> ApiContract:
        existing = await self.session.scalar(
            select(ApiContract).where(
                ApiContract.service_id == service_id,
                ApiContract.schema_digest == published.schema_digest,
            )
        )
        if existing is not None:
            if existing.contract_version != published.contract_version:
                raise InvalidTransitionError(
                    "the same schema digest is already published with contract version "
                    f"{existing.contract_version}"
                )
            existing.descriptor_uri = published.descriptor_uri
            existing.proto_bundle_uri = published.proto_bundle_uri
            existing.proto_bundle_digest = published.proto_bundle_digest
            existing.source_digest = published.source_digest
            await self.session.flush()
            return existing

        latest_contract = await self.session.scalar(
            select(ApiContract)
            .where(ApiContract.service_id == service_id)
            .order_by(ApiContract.created_at.desc())
            .limit(1)
        )
        if latest_contract is not None:
            if Version(published.contract_version) <= Version(
                latest_contract.contract_version
            ):
                raise InvalidTransitionError(
                    "a changed schema requires a contract version newer than "
                    f"{latest_contract.contract_version}"
                )

        contract = ApiContract(
            service_id=service_id,
            schema_digest=published.schema_digest,
            source_digest=published.source_digest,
            contract_version=published.contract_version,
            descriptor_uri=published.descriptor_uri,
            proto_bundle_uri=published.proto_bundle_uri,
            proto_bundle_digest=published.proto_bundle_digest,
            methods=published.methods,
            status=ContractStatus.READY,
        )
        self.session.add(contract)
        await self.session.flush()
        self.session.add(
            OutboxEvent(
                topic="contract.ready",
                aggregate_id=str(contract.id),
                payload={
                    "service_id": str(service_id),
                    "contract_id": str(contract.id),
                    "schema_digest": contract.schema_digest,
                    "proto_bundle_digest": contract.proto_bundle_digest,
                },
            )
        )
        return contract

    async def create_revision(
        self,
        service_id: uuid.UUID,
        request: ResolvedRevisionRequest,
        contract_id: uuid.UUID | None = None,
        runtime_profile: RuntimeProfileRecord | None = None,
    ) -> Revision:
        service = await self.session.get(Service, service_id)
        if service is None:
            raise NotFoundError("service does not exist")

        manifest = {
            "spec_version": 2,
            "endpoints": [endpoint.to_manifest() for endpoint in request.endpoints],
        }
        if request.grpc_contract is not None:
            manifest["grpc_contract"] = request.grpc_contract.model_dump(
                mode="json", exclude_none=True
            )
        tracking_mode = (
            RuntimeTrackingMode.PINNED
            if "@v" in request.runtime_profile
            else RuntimeTrackingMode.TRACK_ACTIVE
        )
        if runtime_profile is not None:
            manifest["runtime"] = {
                "label": runtime_profile.label.name,
                "profile_ref": runtime_profile.version.profile_ref,
                "environment_digest": runtime_profile.version.environment_digest,
                "tracking_mode": tracking_mode.value,
            }
        revision = Revision(
            service_id=service_id,
            revision=request.revision,
            artifact_uri=request.artifact_uri,
            artifact_digest=request.artifact_digest,
            runtime_profile=(
                runtime_profile.version.profile_ref
                if runtime_profile is not None
                else request.runtime_profile
            ),
            status=RevisionStatus.READY,
            manifest=manifest,
        )
        self.session.add(revision)
        await self.session.flush()

        if runtime_profile is not None:
            self.session.add(
                RevisionRuntimeProfile(
                    revision_id=revision.id,
                    runtime_label_id=runtime_profile.label.id,
                    runtime_profile_version_id=runtime_profile.version.id,
                    tracking_mode=tracking_mode,
                    interface_digest=request.artifact_digest,
                    validation_result={
                        "source": "pyscripts-hook",
                        "requested_runtime": request.runtime_profile,
                        "resolved_profile_ref": runtime_profile.version.profile_ref,
                        "requires_python": request.requires_python,
                        "dependencies": request.dependencies,
                    },
                )
            )

        has_grpc = any(endpoint.grpc is not None for endpoint in request.endpoints)
        if has_grpc and contract_id is None:
            raise InvalidTransitionError(
                "gRPC revision cannot be created before its Proto contract is ready"
            )
        if contract_id is not None:
            self.session.add(
                RevisionContract(revision_id=revision.id, contract_id=contract_id)
            )

        for spec in request.endpoints:
            self.session.add(
                Endpoint(
                    revision_id=revision.id,
                    endpoint_id=spec.id,
                    task_type=spec.task_type,
                    entrypoint=spec.entrypoint,
                    request_schema=spec.request_schema,
                    response_schema=spec.response_schema,
                )
            )
        self.session.add(
            OutboxEvent(
                topic="revision.ready",
                aggregate_id=str(revision.id),
                payload={
                    "service_id": str(service_id),
                    "revision_id": str(revision.id),
                },
            )
        )
        await self.session.flush()
        return revision

    async def activate_revision(
        self, service_id: uuid.UUID, revision_id: uuid.UUID
    ) -> Revision:
        service = await self.session.scalar(
            select(Service).where(Service.id == service_id).with_for_update()
        )
        if service is None:
            raise NotFoundError("service does not exist")

        revision = await self.session.scalar(
            select(Revision).where(
                Revision.id == revision_id,
                Revision.service_id == service_id,
            )
        )
        if revision is None:
            raise NotFoundError("revision does not exist")
        if revision.status not in {RevisionStatus.READY, RevisionStatus.ACTIVE}:
            raise InvalidTransitionError(
                f"cannot activate revision in {revision.status.value} state"
            )
        self._require_current_interface(revision)

        grpc_method_paths = self._grpc_method_paths(revision)
        if grpc_method_paths:
            contract_row = (
                await self.session.execute(
                    select(RevisionContract, ApiContract)
                    .join(ApiContract, ApiContract.id == RevisionContract.contract_id)
                    .where(RevisionContract.revision_id == revision.id)
                )
            ).one_or_none()
            if contract_row is None:
                raise InvalidTransitionError("revision contract is not ready")
            _, contract = contract_row
            if contract.status != ContractStatus.READY:
                raise InvalidTransitionError("revision contract is not ready")
            if service.status != ServiceStatus.STOPPED:
                await self._assert_grpc_routes_available(
                    service,
                    grpc_method_paths,
                    self._grpc_service_names(revision),
                )
        if service.active_revision_id and service.active_revision_id != revision.id:
            old_revision = await self.session.get(Revision, service.active_revision_id)
            if old_revision is not None:
                old_revision.status = RevisionStatus.DRAINING

        now = datetime.now(UTC)
        service_was_stopped = service.status == ServiceStatus.STOPPED
        revision.status = RevisionStatus.ACTIVE
        revision.activated_at = now
        service.active_revision_id = revision.id
        if not service_was_stopped:
            service.status = ServiceStatus.ACTIVE
        self.session.add(
            OutboxEvent(
                topic="revision.activated",
                aggregate_id=str(service.id),
                payload={
                    "service_id": str(service.id),
                    "revision_id": str(revision.id),
                    "revision": revision.revision,
                },
            )
        )
        await self.session.flush()
        return revision

    async def stop_service(self, service_id: uuid.UUID) -> Service:
        service = await self.session.scalar(
            select(Service).where(Service.id == service_id).with_for_update()
        )
        if service is None:
            raise NotFoundError("service does not exist")
        if service.status != ServiceStatus.ACTIVE:
            raise InvalidTransitionError("only an active service can be stopped")
        service.status = ServiceStatus.STOPPED
        service.updated_at = datetime.now(UTC)
        self.session.add(
            OutboxEvent(
                topic="service.stopped",
                aggregate_id=str(service.id),
                payload={"service_id": str(service.id)},
            )
        )
        await self.session.flush()
        await self.session.refresh(service)
        return service

    async def start_service(self, service_id: uuid.UUID) -> Service:
        service = await self.session.scalar(
            select(Service).where(Service.id == service_id).with_for_update()
        )
        if service is None:
            raise NotFoundError("service does not exist")
        if service.status != ServiceStatus.STOPPED:
            raise InvalidTransitionError("only a stopped service can be started")
        if service.active_revision_id is None:
            raise InvalidTransitionError(
                "service has no active revision; publish and activate one first"
            )
        revision = await self.session.get(Revision, service.active_revision_id)
        if revision is None or revision.status != RevisionStatus.ACTIVE:
            raise InvalidTransitionError(
                "service active revision is unavailable; activate a ready revision first"
            )
        self._require_current_interface(revision)
        grpc_method_paths = self._grpc_method_paths(revision)
        if grpc_method_paths:
            await self._assert_grpc_routes_available(
                service,
                grpc_method_paths,
                self._grpc_service_names(revision),
            )
        service.status = ServiceStatus.ACTIVE
        service.updated_at = datetime.now(UTC)
        self.session.add(
            OutboxEvent(
                topic="service.started",
                aggregate_id=str(service.id),
                payload={
                    "service_id": str(service.id),
                    "revision_id": str(revision.id),
                    "revision": revision.revision,
                },
            )
        )
        await self.session.flush()
        await self.session.refresh(service)
        return service

    @staticmethod
    def _grpc_method_paths(revision: Revision) -> set[str]:
        paths: set[str] = set()
        for endpoint in revision.manifest.get("endpoints", []):
            grpc = endpoint.get("grpc")
            if not isinstance(grpc, dict):
                continue
            service = grpc.get("service")
            method = grpc.get("method")
            if isinstance(service, str) and isinstance(method, str):
                paths.add(f"/{service}/{method}")
        return paths

    @staticmethod
    def _grpc_service_names(revision: Revision) -> set[str]:
        services: set[str] = set()
        for endpoint in revision.manifest.get("endpoints", []):
            grpc = endpoint.get("grpc")
            if not isinstance(grpc, dict):
                continue
            service = grpc.get("service")
            if isinstance(service, str):
                services.add(service)
        return services

    async def _assert_grpc_routes_available(
        self,
        candidate_service: Service,
        candidate_paths: set[str],
        candidate_grpc_services: set[str],
    ) -> None:
        if not candidate_paths:
            return
        bind = self.session.get_bind()
        if bind.dialect.name == "postgresql":
            await self.session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _GRPC_ROUTE_ACTIVATION_LOCK},
            )

        rows = await self.session.execute(
            select(Service.name, Revision)
            .join(Revision, Revision.id == Service.active_revision_id)
            .where(
                Service.id != candidate_service.id,
                Service.status == ServiceStatus.ACTIVE,
                Revision.status == RevisionStatus.ACTIVE,
            )
            .order_by(Service.name)
        )
        for service_name, revision in rows:
            service_conflicts = (
                candidate_grpc_services & self._grpc_service_names(revision)
            )
            if service_conflicts:
                grpc_service = min(service_conflicts)
                raise InvalidTransitionError(
                    f"gRPC service {grpc_service} is already published by "
                    f"service {service_name!r}"
                )
            conflicts = candidate_paths & self._grpc_method_paths(revision)
            if conflicts:
                method_path = min(conflicts)
                raise InvalidTransitionError(
                    f"gRPC method {method_path} is already published by "
                    f"service {service_name!r}"
                )

    async def get_active_contract(self, service_name: str) -> ContractBundle:
        row = (
            await self.session.execute(
                select(Service, Revision, ApiContract)
                .join(Revision, Revision.id == Service.active_revision_id)
                .join(RevisionContract, RevisionContract.revision_id == Revision.id)
                .join(ApiContract, ApiContract.id == RevisionContract.contract_id)
                .where(
                    Service.name == service_name,
                    Service.status == ServiceStatus.ACTIVE,
                    Revision.status == RevisionStatus.ACTIVE,
                    ApiContract.status == ContractStatus.READY,
                )
            )
        ).one_or_none()
        if row is None:
            raise NotFoundError("active gRPC contract does not exist")
        service, revision, contract = row
        if not self._supports_current_interface(revision):
            raise NotFoundError("active gRPC contract does not exist")
        return ContractBundle(service, contract, revision)

    async def get_latest_contract(self, service_id: uuid.UUID) -> ApiContract | None:
        return await self.session.scalar(
            select(ApiContract)
            .where(
                ApiContract.service_id == service_id,
                ApiContract.status == ContractStatus.READY,
            )
            .order_by(ApiContract.created_at.desc())
            .limit(1)
        )

    async def get_contract(self, contract_id: uuid.UUID) -> ContractBundle:
        row = (
            await self.session.execute(
                select(Service, Revision, ApiContract)
                .join(ApiContract, ApiContract.service_id == Service.id)
                .join(
                    RevisionContract,
                    RevisionContract.contract_id == ApiContract.id,
                )
                .join(Revision, Revision.id == RevisionContract.revision_id)
                .where(
                    ApiContract.id == contract_id,
                    ApiContract.status == ContractStatus.READY,
                )
                .order_by(Revision.created_at.desc())
                .limit(1)
            )
        ).first()
        if row is None:
            raise NotFoundError("gRPC contract does not exist")
        service, revision, contract = row
        return ContractBundle(service, contract, revision)

    async def resolve_endpoint(
        self, service_name: str, endpoint_id: str
    ) -> ResolvedEndpoint:
        row = (
            await self.session.execute(
                select(Service, Revision, Endpoint, RuntimeProfileVersion)
                .join(Revision, Revision.id == Service.active_revision_id)
                .join(Endpoint, Endpoint.revision_id == Revision.id)
                .outerjoin(
                    RevisionRuntimeProfile,
                    RevisionRuntimeProfile.revision_id == Revision.id,
                )
                .outerjoin(
                    RuntimeProfileVersion,
                    RuntimeProfileVersion.id
                    == RevisionRuntimeProfile.runtime_profile_version_id,
                )
                .where(
                    Service.name == service_name,
                    Service.status == ServiceStatus.ACTIVE,
                    Revision.status == RevisionStatus.ACTIVE,
                    Endpoint.endpoint_id == endpoint_id,
                )
            )
        ).one_or_none()
        if row is None:
            raise NotFoundError("active service endpoint does not exist")
        service, revision, endpoint, profile = row
        if not self._supports_current_interface(revision):
            raise NotFoundError("active service endpoint does not exist")
        return self._resolved_endpoint(service, revision, endpoint, profile)

    async def list_active_endpoints(self) -> list[ResolvedEndpoint]:
        rows = await self.session.execute(
            select(Service, Revision, Endpoint, RuntimeProfileVersion)
            .join(Revision, Revision.id == Service.active_revision_id)
            .join(Endpoint, Endpoint.revision_id == Revision.id)
            .outerjoin(
                RevisionRuntimeProfile,
                RevisionRuntimeProfile.revision_id == Revision.id,
            )
            .outerjoin(
                RuntimeProfileVersion,
                RuntimeProfileVersion.id
                == RevisionRuntimeProfile.runtime_profile_version_id,
            )
            .where(
                Service.status == ServiceStatus.ACTIVE,
                Revision.status == RevisionStatus.ACTIVE,
            )
            .order_by(Service.name, Endpoint.endpoint_id)
        )
        return [
            self._resolved_endpoint(service, revision, endpoint, profile)
            for service, revision, endpoint, profile in rows
            if self._supports_current_interface(revision)
        ]

    @staticmethod
    def _resolved_endpoint(
        service: Service,
        revision: Revision,
        endpoint: Endpoint,
        profile: RuntimeProfileVersion | None = None,
    ) -> ResolvedEndpoint:
        endpoint_manifest = [dict(item) for item in revision.manifest["endpoints"]]
        endpoint_spec = next(
            item for item in endpoint_manifest if item["id"] == endpoint.endpoint_id
        )
        return ResolvedEndpoint(
            service_id=service.id,
            service_name=service.name,
            revision_id=revision.id,
            revision=revision.revision,
            artifact_uri=revision.artifact_uri,
            artifact_digest=revision.artifact_digest,
            runtime_profile=(profile.profile_ref if profile else revision.runtime_profile),
            endpoint_manifest=endpoint_manifest,
            endpoint_id=endpoint.endpoint_id,
            task_type=endpoint.task_type,
            entrypoint=endpoint.entrypoint,
            grpc=endpoint_spec.get("grpc"),
            environment_digest=profile.environment_digest if profile else None,
            runtime_env=dict(profile.runtime_env) if profile else None,
            worker_pool=profile.worker_pool if profile else None,
            runtime_profile_version_id=profile.id if profile else None,
            num_cpus=endpoint_spec.get("num_cpus"),
            num_gpus=endpoint_spec.get("num_gpus"),
            io_type=tuple(endpoint_spec.get("io_type", ["rest"])),
            pip_source=profile.pip_source if profile else "default",
            pip_index_url=profile.pip_index_url if profile else None,
        )

    async def begin_invocation(
        self,
        target: ResolvedEndpoint,
        *,
        transport: str,
    ) -> Invocation:
        invocation = Invocation(
            service_id=target.service_id,
            revision_id=target.revision_id,
            endpoint_id=target.endpoint_id,
            transport=transport,
            status=InvocationStatus.RUNNING,
            started_at=datetime.now(UTC),
        )
        self.session.add(invocation)
        await self.session.flush()
        self.session.add(
            InvocationExecution(
                invocation_id=invocation.id,
                runtime_profile_version_id=target.runtime_profile_version_id,
                runtime_profile_ref=target.runtime_profile,
                environment_digest=target.environment_digest,
                artifact_digest=target.artifact_digest,
                execution_kind=(
                    ExecutionKind.COMPUTE_TASK
                    if target.task_type == "compute"
                    else ExecutionKind.IO_ACTOR
                ),
            )
        )
        return invocation

    async def active_runtime_execution_count(
        self,
        environment_digest: str,
    ) -> int:
        return int(
            await self.session.scalar(
                select(func.count())
                .select_from(InvocationExecution)
                .join(
                    Invocation,
                    Invocation.id == InvocationExecution.invocation_id,
                )
                .where(
                    InvocationExecution.environment_digest == environment_digest,
                    Invocation.status.in_(
                        [InvocationStatus.ACCEPTED, InvocationStatus.RUNNING]
                    ),
                )
            )
            or 0
        )

    async def finish_invocation(
        self,
        invocation_id: uuid.UUID,
        status: InvocationStatus,
        error: str | None = None,
        *,
        logs: tuple[CapturedLogChunk, ...] = (),
        log_bytes: int = 0,
        logs_truncated: bool = False,
    ) -> None:
        invocation = await self.session.get(Invocation, invocation_id)
        if invocation is None:
            raise NotFoundError("invocation does not exist")
        invocation.status = status
        invocation.error = error
        invocation.finished_at = datetime.now(UTC)
        execution = await self.session.get(InvocationExecution, invocation_id)
        if execution is not None:
            execution.log_bytes = log_bytes
            execution.logs_truncated = logs_truncated
        if logs:
            self.session.add_all(
                InvocationLog(
                    invocation_id=invocation_id,
                    sequence=chunk.sequence,
                    stream=chunk.stream,
                    content=chunk.content,
                    emitted_at=chunk.emitted_at,
                )
                for chunk in logs
            )
        await self.session.flush()
        retention_count = await self.session.scalar(
            select(EndpointInvocationPolicy.retention_count).where(
                EndpointInvocationPolicy.service_id == invocation.service_id,
                EndpointInvocationPolicy.endpoint_id == invocation.endpoint_id,
            )
        )
        await self._prune_endpoint_invocations(
            invocation.service_id,
            invocation.endpoint_id,
            retention_count or 100,
        )

    async def _prune_endpoint_invocations(
        self,
        service_id: uuid.UUID,
        endpoint_id: str,
        retention_count: int,
    ) -> None:
        terminal_statuses = (
            InvocationStatus.SUCCEEDED,
            InvocationStatus.FAILED,
            InvocationStatus.CANCELLED,
            InvocationStatus.TIMED_OUT,
        )
        stale_ids = list(
            await self.session.scalars(
                select(Invocation.id)
                .where(
                    Invocation.service_id == service_id,
                    Invocation.endpoint_id == endpoint_id,
                    Invocation.status.in_(terminal_statuses),
                )
                .order_by(Invocation.started_at.desc(), Invocation.id.desc())
                .offset(retention_count)
            )
        )
        if stale_ids:
            await self.session.execute(
                delete(Invocation).where(Invocation.id.in_(stale_ids))
            )
