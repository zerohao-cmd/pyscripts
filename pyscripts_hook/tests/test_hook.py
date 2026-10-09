import asyncio
import datetime
import inspect
import unittest
from typing import Any, get_args

from sc_hook import (
    ArrowTable,
    Date,
    Datetime,
    DatetimeTz,
    Int32,
    IOType,
    RemoteCallContextError,
    TaskType,
    hook,
    to_schema,
    use_remote_call_backend,
)


class RecordingBackend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def remote_call(self, endpoint, args, kwargs):
        self.calls.append((endpoint.endpoint_id, args, dict(kwargs)))
        return 99


class HookTests(unittest.IsolatedAsyncioTestCase):
    async def test_infers_io_and_compute_task_types(self) -> None:
        @hook(io_types=[IOType.Rest])
        async def io_endpoint(value: Int32) -> Int32:
            return value

        @hook(io_types=[IOType.Inner])
        def compute_endpoint(value: Int32) -> Int32:
            return value

        self.assertEqual(io_endpoint.task_type, TaskType.IO)
        self.assertEqual(compute_endpoint.task_type, TaskType.COMPUTE)
        self.assertEqual(await io_endpoint(1), 1)
        self.assertEqual(compute_endpoint(2), 2)

    async def test_local_remote_call_runs_compute_and_awaits_io(self) -> None:
        events: list[str] = []

        @hook(io_types=[IOType.Inner])
        def compute_endpoint(value: Int32) -> Int32:
            events.append("compute")
            return value + 1

        @hook(io_types=[IOType.Inner])
        async def child_io(value: Int32) -> Int32:
            await asyncio.sleep(0)
            events.append("child-io")
            return value + 2

        @hook(io_types=[IOType.Rest])
        async def parent_io(value: Int32) -> Int32:
            first = await compute_endpoint.remote_call(value)
            second = await child_io.remote_call(value)
            return first + second

        self.assertEqual(await parent_io(3), 9)
        self.assertEqual(events, ["compute", "child-io"])

    async def test_remote_call_requires_an_io_endpoint_scope(self) -> None:
        @hook(io_types=[IOType.Inner])
        def compute_endpoint(value: Int32) -> Int32:
            return value

        with self.assertRaises(RemoteCallContextError):
            await compute_endpoint.remote_call(1)

    async def test_remote_call_is_rejected_inside_compute_endpoint(self) -> None:
        @hook(io_types=[IOType.Inner])
        def child_compute(value: Int32) -> Int32:
            return value

        @hook(io_types=[IOType.Inner])
        def parent_compute(value: Int32) -> Int32:
            return asyncio.run(child_compute.remote_call(value))

        with self.assertRaises(RemoteCallContextError):
            await asyncio.to_thread(parent_compute, 1)

    async def test_bound_backend_handles_remote_call(self) -> None:
        backend = RecordingBackend()

        @hook(io_types=[IOType.Inner], endpoint_id="calculate")
        def compute_endpoint(value: Int32) -> Int32:
            raise AssertionError("the local function must not run")

        @hook(io_types=[IOType.Rest])
        async def io_endpoint(value: Int32) -> Int32:
            return await compute_endpoint.remote_call(value)

        with use_remote_call_backend(backend):
            result = await io_endpoint(7)

        self.assertEqual(result, 99)
        self.assertEqual(backend.calls, [("calculate", (7,), {})])

    async def test_bound_backend_rejects_direct_compute_call_from_io(self) -> None:
        backend = RecordingBackend()

        @hook(io_types=[IOType.Inner])
        def compute_endpoint(value: Int32) -> Int32:
            return value

        @hook(io_types=[IOType.Rest])
        async def io_endpoint(value: Int32) -> Int32:
            return compute_endpoint(value)

        with use_remote_call_backend(backend):
            with self.assertRaisesRegex(RemoteCallContextError, "remote_call"):
                await io_endpoint(7)

    async def test_remote_call_validates_arguments_before_dispatch(self) -> None:
        backend = RecordingBackend()

        @hook(io_types=[IOType.Inner])
        def compute_endpoint(value: Int32) -> Int32:
            return value

        @hook(io_types=[IOType.Rest])
        async def io_endpoint(value: Int32) -> Int32:
            return await compute_endpoint.remote_call(value, unexpected=1)

        with use_remote_call_backend(backend):
            with self.assertRaises(TypeError):
                await io_endpoint(7)

        self.assertEqual(backend.calls, [])

    async def test_original_signature_and_metadata_are_preserved(self) -> None:
        @hook(io_types=[IOType.Rest])
        async def io_endpoint(value: Int32) -> Int32:
            """Endpoint documentation."""
            return value

        self.assertEqual(io_endpoint.__name__, "io_endpoint")
        self.assertEqual(io_endpoint.__doc__, "Endpoint documentation.")
        self.assertEqual(list(inspect.signature(io_endpoint).parameters), ["value"])


class HookValidationTests(unittest.TestCase):
    def test_date_types_use_datetime_standard_library_classes(self) -> None:
        self.assertIs(get_args(Date)[0], datetime.date)
        self.assertIs(get_args(Datetime)[0], datetime.datetime)
        self.assertIs(get_args(DatetimeTz)[0], datetime.datetime)

    def test_arrow_table_uses_an_optional_pyarrow_type(self) -> None:
        @hook(io_types=[IOType.Inner])
        def identity(value: ArrowTable) -> ArrowTable:
            return value

        try:
            from pyarrow import Table
        except ImportError:
            Table = object
        self.assertIs(get_args(ArrowTable)[0], Table)
        self.assertEqual(
            to_schema(identity.params_type["value"]),
            {"type": "ArrowTable"},
        )
        self.assertEqual(
            to_schema(identity.return_type),
            {"type": "ArrowTable"},
        )

    def test_rejects_unannotated_parameters(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing"):

            @hook(io_types=[IOType.Rest])
            def endpoint(value: Int32, missing) -> Int32:
                return value

    def test_rejects_empty_or_variadic_parameters(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least one parameter"):

            @hook(io_types=[IOType.Rest])
            def empty() -> Int32:
                return 1

        with self.assertRaisesRegex(ValueError, "fixed parameters"):

            @hook(io_types=[IOType.Rest])
            def variadic(*values: Int32) -> Int32:
                return sum(values)


if __name__ == "__main__":
    unittest.main()
