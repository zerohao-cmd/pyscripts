import { describe, expect, test } from "bun:test";

import { latestRuntimeVersion, runtimeVersionInput } from "../src/runtimeProfiles";
import type { RuntimeProfileVersion } from "../src/types";

function profile(overrides: Partial<RuntimeProfileVersion> = {}): RuntimeProfileVersion {
  return {
    id: "version-2",
    label_id: "label-1",
    label_name: "etl",
    version: 2,
    profile_ref: "etl@v2",
    python_version: "3.12",
    worker_pool: "analytics",
    pip_source: "custom",
    pip_index_url: "http://192.168.0.11:12342/shrx/pkg/+simple/",
    requested_dependencies: ["crawlers-rpc==0.1.5", "strand-io==0.2.0"],
    resolved_dependencies: { "crawlers-rpc": "0.1.5", "strand-io": "0.2.0", grpcio: "1.73.0" },
    import_checks: ["crawlers_rpc", "strand_io"],
    environment_digest: null,
    status: "FAILED",
    error: "Installation timed out",
    validation_result: {},
    reference_count: 0,
    created_at: "2026-10-26T06:23:45Z",
    validated_at: null,
    retired_at: null,
    ...overrides,
  };
}

describe("runtime version editing", () => {
  test("defaults to the latest version even when it failed and an older one is active", () => {
    const active = profile({ id: "version-1", version: 1, status: "ACTIVE" });
    const latest = profile();
    const retired = profile({ id: "version-0", version: 0, status: "RETIRED" });

    expect(latestRuntimeVersion([active, latest, retired])).toBe(latest);
    expect(latestRuntimeVersion([latest, retired, active])).toBe(latest);
  });

  test("allows a new label without any previous version", () => {
    expect(latestRuntimeVersion([])).toBeUndefined();
  });

  test("copies all editable configuration from the selected version, including the devpi URL", () => {
    expect(runtimeVersionInput(profile())).toEqual({
      python_version: "3.12",
      worker_pool: "analytics",
      dependencies: ["crawlers-rpc==0.1.5", "strand-io==0.2.0"],
      import_checks: ["crawlers_rpc", "strand_io"],
      pip_source: "custom",
      pip_index_url: "http://192.168.0.11:12342/shrx/pkg/+simple/",
    });
  });

  test("editing a copy preserves the original version and its resolved dependencies", () => {
    const original = profile();
    const before = structuredClone(original);
    const input = runtimeVersionInput(original);

    input.dependencies.push("httpx==0.28.1");
    input.import_checks.splice(0, 1);
    input.worker_pool = "default";
    input.pip_index_url = "https://other.example.com/simple/";

    expect(original).toEqual(before);
    expect(input.dependencies).not.toContain("grpcio==1.73.0");
  });

  test("uses the selected historical version and keeps private and default source settings", () => {
    const historical = profile({
      python_version: "3.11",
      worker_pool: "legacy",
      pip_source: "private",
      pip_index_url: null,
      requested_dependencies: [],
      import_checks: [],
    });
    expect(runtimeVersionInput(historical)).toEqual({
      python_version: "3.11",
      worker_pool: "legacy",
      dependencies: [],
      import_checks: [],
      pip_source: "private",
      pip_index_url: null,
    });
    expect(runtimeVersionInput(profile({ pip_source: "default", pip_index_url: null })).pip_source).toBe("default");
  });
});
