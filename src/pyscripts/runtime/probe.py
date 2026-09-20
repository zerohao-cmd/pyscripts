from __future__ import annotations

import importlib
import importlib.metadata
import re
import subprocess
import sys
from typing import Any


def probe_environment(import_checks: list[str]) -> dict[str, Any]:
    """Inspect a temporary worker environment without control-plane imports."""
    check = subprocess.run(
        [sys.executable, "-m", "pip", "check"],
        capture_output=True,
        check=False,
        text=True,
        timeout=120,
    )
    if check.returncode != 0:
        raise RuntimeError(
            check.stdout.strip() or check.stderr.strip() or "pip check failed"
        )

    imported: dict[str, str] = {}
    for module_name in import_checks:
        module = importlib.import_module(module_name)
        imported[module_name] = str(getattr(module, "__version__", "OK"))

    installed = {
        re.sub(r"[-_.]+", "-", distribution.metadata["Name"]).lower(): distribution.version
        for distribution in importlib.metadata.distributions()
        if distribution.metadata.get("Name")
    }
    return {
        "python": ".".join(map(str, sys.version_info[:3])),
        "installed": dict(sorted(installed.items())),
        "imports": imported,
        "pip_check": "OK",
    }
