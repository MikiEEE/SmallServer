"""Public adapter-facade typing fixture."""

from smallserver import AdapterError, AdapterRegistry, AsyncioAdapter, ThreadAdapter


def blocking(value: str) -> int:
    return len(value)


async def foreign_async(value: str) -> int:
    return len(value)


def facade() -> AdapterRegistry:
    thread_adapter = ThreadAdapter(max_workers=1, max_pending=4)
    asyncio_adapter = AsyncioAdapter(max_pending=4)
    services = AdapterRegistry(blocking=thread_adapter, async_sdk=asyncio_adapter)

    thread_adapter.call(blocking, "smallserver")
    asyncio_adapter.call(foreign_async, "smallserver")
    return services


def handle(error: AdapterError) -> str:
    return str(error)
