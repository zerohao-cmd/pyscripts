from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol

import ray
from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version

from pyscripts.config import Settings
from pyscripts.ray_client import ensure_ray
from pyscripts.runtime.probe import probe_environment


class RuntimeProfileValidationError(RuntimeError):
    pass


class RuntimeProfileCompatibilityError(RuntimeError):
    pass


def validate_project_compatibility(
    profile: Any,
    *,
    requires_python: str,
    dependencies: list[str],
) -> None:
    """Verify project constraints against one already-resolved environment."""

    python_version = Version(profile.version.python_version)
    if python_version not in SpecifierSet(requires_python):
        raise RuntimeProfileCompatibilityError(
            f"runtime {profile.version.profile_ref} uses Python {python_version}, "
            f"which does not satisfy {requires_python}"
        )

    installed = {
        canonicalize_name(name): Version(version)
        for name, version in profile.version.resolved_dependencies.items()
    }
    marker_environment = default_environment()
    marker_environment["python_version"] = profile.version.python_version
    marker_environment["python_full_version"] = f"{profile.version.python_version}.0"
    errors: list[str] = []
    for raw_requirement in dependencies:
        requirement = Requirement(raw_requirement)
        if requirement.marker and not requirement.marker.evaluate(marker_environment):
            continue
        name = canonicalize_name(requirement.name)
        version = installed.get(name)
        if version is None:
            errors.append(f"{requirement.name} is not installed")
        elif requirement.specifier and version not in requirement.specifier:
            errors.append(
                f"{requirement.name} {version} does not satisfy "
                f"{requirement.specifier}"
            )
    if errors:
        raise RuntimeProfileCompatibilityError("; ".join(errors))


@dataclass(frozen=True, slots=True)
class RuntimeEnvironmentValidation:
    resolved_dependencies: dict[str, str]
    validation_result: dict[str, Any]
    runtime_env: dict[str, Any]
    environment_digest: str


class RuntimeEnvironmentValidator(Protocol):
    async def validate(
        self,
        *,
        profile_ref: str,
        python_version: str,
        worker_pool: str,
        dependencies: list[str],
        import_checks: list[str],
        pip_source: str,
    ) -> RuntimeEnvironmentValidation: ...


def build_runtime_env(
    dependencies: list[str],
    setup_timeout_seconds: int,
) -> dict[str, Any]:
    runtime_env: dict[str, Any] = {
        "config": {"setup_timeout_seconds": setup_timeout_seconds}
    }
    if dependencies:
        runtime_env["pip"] = {
            "packages": dependencies,
            "pip_check": True,
        }
    return runtime_env


def materialize_runtime_env(
    runtime_env: dict[str, Any],
    pip_source: str,
    settings: Settings,
) -> dict[str, Any]:
    """Inject configured package-index options without persisting credentials."""

    result = copy.deepcopy(runtime_env)
    if pip_source == "default":
        return result
    if pip_source != "private":
        raise RuntimeProfileValidationError(f"unknown pip source: {pip_source}")
    if settings.private_pip_index_url is None:
        raise RuntimeProfileValidationError(
            "private PyPI source is not configured on the control plane"
        )
    pip = result.get("pip")
    if not isinstance(pip, dict):
        pip = {"packages": [], "pip_check": True}
        result["pip"] = pip
    options = [
        "--disable-pip-version-check",
        "--no-cache-dir",
        "--index-url",
        settings.private_pip_index_url.get_secret_value(),
    ]
    for value in settings.private_pip_extra_index_urls:
        options.extend(["--extra-index-url", value.get_secret_value()])
    for host in settings.private_pip_trusted_hosts:
        options.extend(["--trusted-host", host])
    pip["pip_install_options"] = options
    return result


def pip_source_fingerprint(pip_source: str, settings: Settings) -> str:
    if pip_source == "default":
        return "default"
    materialized = materialize_runtime_env(
        {"pip": {"packages": []}}, pip_source, settings
    )
    options = materialized["pip"]["pip_install_options"]
    return hashlib.sha256(
        json.dumps(options, separators=(",", ":")).encode()
    ).hexdigest()


def _redact_pip_secrets(message: str, settings: Settings) -> str:
    secrets = [
        settings.private_pip_index_url,
        *settings.private_pip_extra_index_urls,
    ]
    for value in secrets:
        if value is not None:
            message = message.replace(
                value.get_secret_value(), "<redacted-private-pypi-url>"
            )
    return message


class RayRuntimeEnvironmentValidator:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def validate(
        self,
        *,
        profile_ref: str,
        python_version: str,
        worker_pool: str,
        dependencies: list[str],
        import_checks: list[str],
        pip_source: str,
    ) -> RuntimeEnvironmentValidation:
        await ensure_ray(self.settings)
        runtime_env_template = build_runtime_env(
            dependencies,
            self.settings.runtime_env_setup_timeout_seconds,
        )
        runtime_env = materialize_runtime_env(
            runtime_env_template, pip_source, self.settings
        )
        options: dict[str, Any] = {
            "num_cpus": 0.01,
            "runtime_env": runtime_env,
        }
        if self.settings.ray_use_label_selector:
            options["label_selector"] = {
                self.settings.ray_worker_pool_label_key: worker_pool
            }
        reference = None
        try:
            reference = ray.remote(probe_environment).options(**options).remote(
                import_checks
            )
            result = await asyncio.wait_for(
                reference,
                timeout=self.settings.runtime_env_setup_timeout_seconds + 60,
            )
        except TimeoutError as error:
            if reference is not None:
                ray.cancel(reference, force=True)
            raise RuntimeProfileValidationError(
                "temporary runtime environment validation timed out"
            ) from error
        except Exception as error:
            raise RuntimeProfileValidationError(
                _redact_pip_secrets(str(error), self.settings)
            ) from error

        actual_python = str(result["python"])
        if not actual_python.startswith(f"{python_version}."):
            raise RuntimeProfileValidationError(
                f"profile requires Python {python_version}, worker uses {actual_python}"
            )

        resolved = dict(result["installed"])
        digest_payload = {
            "profile_ref": profile_ref,
            "python_version": python_version,
            "worker_pool": worker_pool,
            "runtime_env": runtime_env_template,
            "pip_source": pip_source,
            "pip_source_fingerprint": pip_source_fingerprint(
                pip_source, self.settings
            ),
            "resolved_dependencies": resolved,
        }
        environment_digest = hashlib.sha256(
            json.dumps(
                digest_payload,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ).hexdigest()
        return RuntimeEnvironmentValidation(
            resolved_dependencies=resolved,
            validation_result=result,
            runtime_env=runtime_env_template,
            environment_digest=environment_digest,
        )
