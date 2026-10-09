from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from enum import StrEnum
from functools import update_wrapper
from typing import Any, Protocol, cast

from .datatype import DATA_TYPE, get_type


class IOType(StrEnum):
    Rest = "rest"
    Grpc = "grpc"
    Inner = "inner"

    def __repr__(self) -> str:
        return self.value


class TaskType(StrEnum):
    """The execution model inferred from the decorated Python function."""

    IO = "io"
    COMPUTE = "compute"


class RemoteCallError(RuntimeError):
    """Base exception raised by the remote-call API."""


class RemoteCallContextError(RemoteCallError):
    """Raised when ``remote_call`` is used outside an IO endpoint."""


class RemoteCallBackend(Protocol):
    """Adapter implemented by the pyscripts runtime.

    The Hook package intentionally knows nothing about Ray actors, tasks,
    revisions, or invocation records. A Ray-backed runtime binds an adapter
    while it executes an IO endpoint and performs those operations itself.
    """

    def remote_call(
        self,
        endpoint: RemoteFunc[Any, Any],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> Awaitable[Any]: ...


_CURRENT_TASK_TYPE: ContextVar[TaskType | None] = ContextVar(
    "pyscripts_current_task_type",
    default=None,
)
_REMOTE_CALL_BACKEND: ContextVar[RemoteCallBackend | None] = ContextVar(
    "pyscripts_remote_call_backend",
    default=None,
)


@contextmanager
def use_remote_call_backend(backend: RemoteCallBackend) -> Iterator[None]:
    """Bind a remote execution backend to the current async context.

    Context variables are task-local, so concurrent IO invocations may safely
    use different backends/revision contexts in the same event loop.
    """

    if not callable(getattr(backend, "remote_call", None)):
        raise TypeError("remote call backend must define remote_call()")

    token = _REMOTE_CALL_BACKEND.set(backend)
    try:
        yield
    finally:
        _REMOTE_CALL_BACKEND.reset(token)


class RemoteFunc[**P, T]:
    """A decorated endpoint which retains the original local function.

    Calling the object itself keeps the original local-call behaviour. Use
    :meth:`remote_call` from an IO endpoint when the call must pass through the
    runtime scheduler.
    """

    def __init__(
        self,
        *,
        func: Callable[P, Any],
        io_types: Sequence[IOType],
        endpoint_id: str,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
    ):
        self.func = func
        self.io_types = tuple(io_types)
        self.endpoint_id = endpoint_id
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.task_type = (
            TaskType.IO if inspect.iscoroutinefunction(func) else TaskType.COMPUTE
        )
        self.params_type, self.return_type = self.get_func_annote(func)

        # Preserve the endpoint's name, docstring, annotations and signature
        # for discovery, documentation generation and normal Python tooling.
        # Do not merge arbitrary attributes from the user function into this
        # object: they could overwrite endpoint_id/task_type/runtime metadata.
        update_wrapper(self, func, updated=())
        self.__signature__ = inspect.signature(func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        """Execute the original function locally using its native call style.

        An IO endpoint returns an awaitable; a Compute endpoint returns its
        result synchronously. The execution scope is installed here so that
        ``remote_call`` can only be used while an IO endpoint is actually
        running, rather than merely from arbitrary async Python code.
        """

        if self.task_type is TaskType.IO:
            return self._call_io(*args, **kwargs)
        if (
            _CURRENT_TASK_TYPE.get() is TaskType.IO
            and _REMOTE_CALL_BACKEND.get() is not None
        ):
            raise RemoteCallContextError(
                "Compute endpoints must be called with await remote_call() "
                "inside a Ray IO endpoint"
            )
        return self._call_compute(*args, **kwargs)

    async def _call_io(self, *args: P.args, **kwargs: P.kwargs) -> T:
        token = _CURRENT_TASK_TYPE.set(TaskType.IO)
        try:
            result = self.func(*args, **kwargs)
            if not inspect.isawaitable(result):
                raise TypeError(
                    f"IO endpoint {self.endpoint_id!r} did not return an awaitable"
                )
            return cast(T, await result)
        finally:
            _CURRENT_TASK_TYPE.reset(token)

    def _call_compute(self, *args: P.args, **kwargs: P.kwargs) -> T:
        token = _CURRENT_TASK_TYPE.set(TaskType.COMPUTE)
        try:
            return cast(T, self.func(*args, **kwargs))
        finally:
            _CURRENT_TASK_TYPE.reset(token)

    async def _local_remote_call(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Local fallback used when no Ray/runtime backend has been bound."""

        if self.task_type is TaskType.IO:
            return cast(T, await self._call_io(*args, **kwargs))

        # This is deliberately a direct/blocking call. It gives local runs the
        # same simple semantics as ordinary Python and is only the non-Ray
        # fallback. Ray execution is delegated to the bound backend.
        return self._call_compute(*args, **kwargs)

    async def remote_call(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """Execute this endpoint remotely, or locally when outside Ray.

        This API may only be awaited while a decorated IO endpoint is
        executing. In a plain Python process it executes Compute endpoints
        synchronously and awaits IO endpoints. When the pyscripts Ray runtime
        binds a :class:`RemoteCallBackend`, execution is delegated to that
        backend and this coroutine waits for its result.
        """

        if _CURRENT_TASK_TYPE.get() is not TaskType.IO:
            raise RemoteCallContextError(
                "remote_call() may only be used while an IO endpoint is executing"
            )

        # Validate the call against the original signature before it crosses
        # a process boundary. This produces normal Python argument errors at
        # the caller rather than an opaque remote execution failure.
        self.__signature__.bind(*args, **kwargs)

        backend = _REMOTE_CALL_BACKEND.get()
        if backend is None:
            return await self._local_remote_call(*args, **kwargs)

        result = backend.remote_call(self, tuple(args), dict(kwargs))
        if not inspect.isawaitable(result):
            raise TypeError("remote call backend must return an awaitable")
        return cast(T, await result)

    @staticmethod
    def get_func_annote(
        func: Callable[..., Any],
    ) -> tuple[dict[str, DATA_TYPE], DATA_TYPE]:
        func_annote: dict[str, Any] = inspect.get_annotations(func, eval_str=True)
        signature = inspect.signature(func)

        if "return" not in func_annote:
            raise ValueError("hooked func must annotate its return type")
        if not signature.parameters:
            raise ValueError("hooked func must declare at least one parameter")
        if "context" in signature.parameters:
            raise ValueError("hooked func parameter 'context' is reserved")
        unsupported = [
            name
            for name, parameter in signature.parameters.items()
            if parameter.kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            }
        ]
        if unsupported:
            names = ", ".join(unsupported)
            raise ValueError(
                "hooked func only supports named fixed parameters: " + names
            )

        missing = [
            name
            for name, parameter in signature.parameters.items()
            if parameter.annotation is inspect.Parameter.empty
        ]
        if missing:
            names = ", ".join(missing)
            raise ValueError(f"hooked func parameters must be annotated: {names}")

        return_type = get_type(func_annote["return"])
        params_type = {
            name: get_type(func_annote[name]) for name in signature.parameters
        }
        return params_type, return_type


def hook[**HookP, HookT](
    io_types: list[IOType],
    endpoint_id: str | None = None,
    *,
    num_cpus: float | None = None,
    num_gpus: float | None = None,
) -> Callable[
    [Callable[HookP, HookT]],
    RemoteFunc[HookP, HookT],
]:
    """Decorate a function as a pyscripts endpoint.

    ``async def`` functions are IO endpoints. All other functions are Compute
    endpoints.
    """

    def inner(func: Callable[HookP, HookT]) -> RemoteFunc[HookP, HookT]:
        if len(io_types) == 0:
            raise ValueError("io_type need")
        if any(not isinstance(i, IOType) for i in io_types):
            raise ValueError("io_type invalid")
        if len(io_types) != len(set(io_types)):
            raise ValueError("io_type values must be unique")

        true_endpoint_id = func.__name__ if endpoint_id is None else endpoint_id
        if not true_endpoint_id or not true_endpoint_id.strip():
            raise ValueError("endpoint_id must not be empty")

        remote = RemoteFunc(
            func=func,
            io_types=io_types,
            endpoint_id=true_endpoint_id,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
        )
        if remote.task_type is TaskType.IO and (
            num_cpus is not None or num_gpus is not None
        ):
            raise ValueError("num_cpus and num_gpus are only valid for compute")
        if num_cpus is not None and num_cpus <= 0:
            raise ValueError("num_cpus must be greater than zero")
        if num_gpus is not None and num_gpus < 0:
            raise ValueError("num_gpus must be greater than or equal to zero")
        return remote

    return inner
