import type { RuntimeProfileInput, RuntimeProfileVersion } from "./types";

export function latestRuntimeVersion(
  versions: RuntimeProfileVersion[],
): RuntimeProfileVersion | undefined {
  return versions.reduce<RuntimeProfileVersion | undefined>(
    (latest, version) => !latest || version.version > latest.version ? version : latest,
    undefined,
  );
}

export function runtimeVersionInput(version: RuntimeProfileVersion): RuntimeProfileInput {
  return {
    python_version: version.python_version,
    worker_pool: version.worker_pool,
    dependencies: [...version.requested_dependencies],
    import_checks: [...version.import_checks],
    pip_source: version.pip_source,
    pip_index_url: version.pip_index_url,
  };
}
