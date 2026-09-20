"""pyscripts service hosting platform."""

import os

# Runtime profiles, rather than the control plane's uv environment, own worker
# dependencies. Ray 2.58 otherwise detects ``uv run`` and uploads this entire
# repository as the Ray Client driver's runtime environment.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

__version__ = "0.1.0"
