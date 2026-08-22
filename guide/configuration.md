# Configuration

Pass a `ServerConfig` to `listen()` or `serve()` to tune finite listener,
parser, and scheduling limits.

```python
from smallserver import ServerConfig, SmallServer

app = SmallServer()
config = ServerConfig(
    max_connections=50,
    max_header_bytes=16 * 1024,
    max_header_count=64,
    max_body_bytes=512 * 1024,
    receive_chunk_bytes=8 * 1024,
    listener_priority=1,
    connection_priority=2,
    accept_batch_size=16,
    max_request_target_bytes=8 * 1024,
    max_route_error_events=16,
)
```

| Setting | Default | Purpose |
| --- | ---: | --- |
| `max_connections` | 100 | Maximum connection streams still owned by the server. |
| `max_header_bytes` | 16 KiB | Maximum HTTP/1.1 request-head bytes. |
| `max_header_count` | 100 | Maximum number of request header fields. |
| `max_body_bytes` | 1 MiB | Maximum `Content-Length` and buffered request body. |
| `receive_chunk_bytes` | 8 KiB | Bytes requested from the transport per read. |
| `listener_priority` | 1 | SmallOS listener and close-watcher task priority. |
| `connection_priority` | 2 | SmallOS connection-task priority. |
| `accept_batch_size` | 16 | Accepts before the listener explicitly yields. |
| `max_request_target_bytes` | 8 KiB | Maximum HTTP/1.1 origin-form request target. |
| `max_route_error_events` | 16 | Bounded sanitized regex-timeout observer queue. |

Every field must be a positive integer; booleans are rejected. The public port
must be an integer from 0 through 65535. `port=0` delegates port selection to
the kernel.

At connection capacity, the listener waits on a scheduler signal instead of
accepting and discarding more streams. Connections whose close failed still
count against the limit because the server continues to own them. A close
failure is fatal to further acceptance and remains visible for cleanup retry.

Limits are per `ServerHandle`. They bound HTTP input and framework-owned
connections, but they do not limit memory allocated by your handlers, response
bodies, adapter queues, or downstream libraries; configure those separately.

## HTTP/2 configuration

Pass `protocol="http2"` and an optional `HTTP2Config` for cleartext
prior-knowledge HTTP/2. Its finite limits cover streams, decoded and compressed
headers, request and response buffers, generated control output, frame size,
reader frame batches, handshake time, and idle time. See
[Cleartext HTTP/2](http2.md) for the complete protocol boundary.

`RegexRouteConfig` separately bounds optional regex pattern length, route and
capture counts, path bytes, per-match time, and total matching time. All time
limits must be finite positive numbers.
