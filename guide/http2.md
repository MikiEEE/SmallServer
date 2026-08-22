# Cleartext HTTP/2

SmallServer can serve HTTP/2 with cleartext prior knowledge. The feature uses
hyper-h2 as a lazy, optional sans-I/O protocol engine while SmallOS continues
to own task scheduling and all network readiness.

## Install and run

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -e '.[http2]'
python3 examples/http2_prior_knowledge.py
```

In another terminal:

```bash
curl --http2-prior-knowledge http://127.0.0.1:8000/health
curl --http2-prior-knowledge --data-binary hello http://127.0.0.1:8000/echo
```

Select HTTP/2 with `protocol="http2"` on either `listen()` or `serve()`.
HTTP/1.1 remains the default and never imports hyper-h2. A missing or
incompatible optional dependency is rejected before SmallServer binds a port.

## Concurrency and limits

Each TCP connection has one protocol state and one writer task. Complete
request streams are dispatched in separate SmallOS tasks, so one stream can
wait on a bounded execution adapter while unrelated streams complete. The
single writer preserves frame ordering and observes peer flow-control windows.

`HTTP2Config` bounds concurrent streams, decoded and compressed header sizes,
per-stream and per-connection request buffering, response buffering, and frame
size. Requests and responses use the same immutable `Request`, `Headers`, and
`Response` values as HTTP/1.1. The request version is `"HTTP/2"`.

Peer stream resets cancel the associated handler task without stopping other
streams. Protocol/resource violations reset the affected stream when possible.
Connection shutdown emits GOAWAY and then releases the connection through the
SmallOS kernel transport.

## Current protocol boundary

Only cleartext prior knowledge is supported. SmallServer does not implement an
HTTP/1.1 `Upgrade: h2c` transition and does not infer the protocol from bytes.
Configure one listener for one protocol.

TLS with ALPN `h2` is deferred because SmallOS does not currently expose a
server-side TLS kernel capability. SmallServer intentionally does not import
or call platform `ssl` or `socket` APIs to work around that missing boundary.
When the kernel gains that capability, TLS/ALPN negotiation can be added
without changing route handlers or response values.
