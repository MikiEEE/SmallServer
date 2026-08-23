# Platforms and kernels

SmallServer delegates networking, readiness, task registration, and task
cancellation to SmallOS. Production framework modules do not import Python's
`socket` module directly; kernel-owned transport handles remain opaque to the
application.

## Desktop default

Managed `app.listen()` lazily imports `SmallOS` and the `Unix` kernel, configures
that runtime, and starts it. If the dependency or Unix kernel is unavailable,
it raises `ServerConfigurationError` and asks the caller to provide a suitable
runtime.

The canonical SmallOS dependency is installed from the GitHub `v1.2.0` release
tag by `requirements.txt`. Python package metadata intentionally has no runtime
dependency until SmallOS has an unambiguous package-index distribution
contract.

## Custom and constrained kernels

A supplied runtime must expose a configured `kernel` plus callable `fork`,
`resume_task`, and `cancel_task` operations. Starting it through SmallServer
also requires `start`.

The kernel must satisfy SmallOS's network capability contract for listeners,
streams, readiness, retry direction, addresses, and cleanup. Capability checks
occur before SmallServer binds a listener.

A wakeup channel is optional:

- with one, `ServerHandle.close()` can notify the scheduler from another thread;
- without one, external `close()` raises and a running task must call
  `await handle.close_from_task(task)`;
- after a caller-owned scheduler exits, `handle.finalize()` is the owner-thread
  cleanup path on either kind of kernel.

Do not infer that a MicroPython-like platform supports managed Unix mode or a
thread-safe wakeup just because it can accept TCP connections. Supply the
platform runtime explicitly and test its real capability surface and cleanup
behavior.
