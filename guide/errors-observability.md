# Errors and observability

SmallServer separates expected HTTP responses, configuration mistakes,
runtime failures, and incomplete cleanup ownership.

## Handler-facing errors

Raise `HTTPError(status, detail)` for an expected 4xx or 5xx response. Status
must be between 400 and 599. The detail becomes a plain-text response; do not
put secrets or raw downstream exceptions in it.

The network server converts ordinary handler exceptions into a generic 500.
`app.dispatch()` only catches `HTTPError`, so direct dispatch in tests preserves
programming errors.

## Configuration errors

`ServerConfigurationError` reports a runtime or kernel capability that cannot
support the requested lifecycle. Type and value mistakes generally raise
`TypeError` or `ValueError` before binding.

## Startup and finalization ownership

`ServerStartupError` means startup failed and one or more acquired resources
could not yet be released. Its `primary_error` is the original failure;
`cleanup_errors` contains the current cleanup failures. Retain the exception
and call `retry_cleanup()` or `finalize()` until it returns `True`.

`ServerFinalizationError` means a started runtime returned normally but server
cleanup remains incomplete. It exposes the same `cleanup_errors`,
`cleanup_complete`, `retry_cleanup()`, and `finalize()` contract.

`KeyboardInterrupt` and `SystemExit` retain their identity. If rollback is
incomplete, their `__cause__` is the `ServerStartupError` cleanup owner. An
abandoned incomplete cleanup error makes one best-effort retry and emits a
`ResourceWarning` if ownership remains.

## ServerHandle state

Observe these stable properties:

- `address` and `port`: cached bind result;
- `closed`: shutdown has been requested;
- `finished`: all server-owned cleanup is complete;
- `failure`: first fatal listener or connection-cleanup failure, if any;
- `cleanup_errors`: current failures for still-owned resources;
- `owned_connection_count`: active and retained connection streams.

SmallServer does not provide a logging backend, metrics registry, or tracing
system in this base. Applications should report sanitized handle state and
their own handler/adapter telemetry without reaching into private attributes.
