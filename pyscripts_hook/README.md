# pyscripts-hook

`pyscripts-hook` marks Python functions as pyscripts endpoints. An `async def`
function is classified as an IO endpoint; a normal `def` function is classified
as a Compute endpoint.

```python
from sc_hook import Int32, IOType, hook


@hook(io_types=[IOType.Inner])
def add(a: Int32, b: Int32) -> Int32:
    return a + b


@hook(io_types=[IOType.Rest])
async def calculate(a: Int32, b: Int32) -> Int32:
    return await add.remote_call(a, b)
```

## `remote_call`

`RemoteFunc.remote_call()` is asynchronous and may only be used while a
decorated IO endpoint is executing.

When no runtime backend is installed, it provides a local-development fallback:

- Compute functions run synchronously in the current thread, and the caller
  waits for their result.
- IO functions are awaited.

The pyscripts Ray runtime installs a backend for the duration of an IO
invocation. The backend is responsible for revision pinning, scheduling a
Compute task or nested IO call, and returning an awaitable result:

```python
from sc_hook import use_remote_call_backend

with use_remote_call_backend(ray_backend):
    result = await calculate(1, 2)
```

The Hook package does not import Ray directly. This keeps endpoint packages
lightweight and ensures the control plane remains responsible for scheduling.

## Date, time, and Arrow Table values

`Date` is backed by `datetime.date`; `Datetime` and `DatetimeTz` are backed by
`datetime.datetime`. `ArrowTable` marks a value whose runtime representation is
a `pyarrow.Table`:

```python
from sc_hook import ArrowTable, Date, Datetime, IOType, hook


@hook(io_types=[IOType.Inner])
def normalize(table: ArrowTable, day: Date, created_at: Datetime) -> ArrowTable:
    return table
```

The Hook package deliberately does not require PyArrow. On import it uses
`pyarrow.Table` when PyArrow is available and falls back to `object` otherwise,
so endpoint modules remain importable in a lightweight environment. A runtime
profile that executes an `ArrowTable` endpoint must provide `pyarrow`; Ray
internal calls carry the Table value directly, while generated gRPC contracts
carry it as an Arrow IPC stream in a `bytes` field.
