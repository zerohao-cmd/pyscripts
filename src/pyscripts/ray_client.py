from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import ray

import pyscripts
from pyscripts.config import Settings


_ray_init_lock = threading.Lock()


def pyscripts_module_path() -> str:
    """Return a Ray-uploadable path for the control-plane worker package."""
    module_file = pyscripts.__file__
    if module_file is None:
        raise RuntimeError("cannot locate the pyscripts package")
    return str(Path(module_file).resolve().parent)


def connect_ray(settings: Settings) -> None:
    """Connect once and upload worker code at the Ray Client job level.

    Ray only accepts local ``py_modules`` paths while initializing a job. Task
    and actor runtime environments must contain remote URIs, so their
    dependency-specific environments inherit this uploaded package instead.
    """
    with _ray_init_lock:
        if ray.is_initialized():
            return
        # Ray Client can leave a disconnected session registered locally
        # after its data channel exceeds the reconnection grace period. In
        # that state is_initialized() is false, but a new init() fails with
        # "already connected with allow_multiple=True" until shutdown clears
        # the stale client context.
        ray.shutdown()
        ray.init(
            address=settings.ray_address,
            namespace=settings.ray_namespace,
            ignore_reinit_error=True,
            runtime_env={
                "py_modules": [pyscripts_module_path()],
                # A Ray Client job can remain in GCS after it has completed.
                # Eager installation would make every newly autoscaled worker
                # download the control package for all of those historical
                # jobs, including packages that have already expired in GCS.
                # Install this job environment only when one of its actors or
                # tasks is actually scheduled on the worker instead.
                "config": {"eager_install": False},
            },
        )


async def ensure_ray(settings: Settings) -> None:
    if ray.is_initialized():
        return
    await asyncio.to_thread(connect_ray, settings)
