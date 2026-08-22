# WebSockets

Install the optional protocol engine before serving WebSocket routes:

```bash
python3 -m pip install -e '.[websocket]'
```

WebSocket routes use HTTP/1.1 Upgrade while SmallOS continues to own task
scheduling and socket readiness. A normal `GET` route may use the same path;
requests without Upgrade headers remain ordinary HTTP requests.

```python
from smallserver import SmallServer, WebSocket

app = SmallServer()

@app.websocket(
    "/chat",
    origins={"https://app.example.com"},
    subprotocols=("chat.v1",),
)
async def chat(socket: WebSocket) -> None:
    await socket.accept(subprotocol="chat.v1")
    async for message in socket:
        if message.is_text:
            await socket.send_text(message.text)
        else:
            await socket.send_bytes(message.bytes)

app.listen()
```

The application must explicitly call `accept()` or `reject()` before using
message operations. Returning without either decision sends a sanitized 403.
Text, binary, fragmented messages, Ping/Pong, and Close are supported. Queue,
frame, message, connection, handshake, idle, Pong, write, and close limits are
finite and configurable through `WebSocketConfig`.

Only one application Ping may await a Pong at a time. The timeout is armed
before the frame is written, and only a Pong with the matching payload clears
it. Handshake, idle, Pong, and close deadlines also bound cleanup when a peer
stops reading; expired connections cancel handler work owned by that
connection.

An origin allowlist is strongly recommended when browser credentials or
cookies are involved. A selected subprotocol must have been offered by the
client and allowed by the route. Outbound saturation raises
`WebSocketCapacityError`.

Direct calls to `receive()`, `receive_text()`, or `receive_bytes()` raise
`WebSocketDisconnect` after already queued messages have been delivered when
the peer or application closes the connection. `async for message in socket`
instead treats that disconnect as normal iteration completion. Server shutdown
and expired handshake, idle, Pong, write, or close deadlines may cancel the
connection handler to guarantee bounded cleanup, so application resource
cleanup belongs in the handler's `finally` block.

Send calls complete after the serialized frame bytes have been flushed through
the connection writer. They do not mean the peer application has processed the
message.

This release does not implement `wss://` termination, compression, custom
extensions, or RFC 8441 WebSockets over HTTP/2. Put TLS at a trusted reverse
proxy until SmallServer gains a native TLS boundary.

The runnable [`websocket_echo.py`](../examples/websocket_echo.py) accepts
clients without requiring a subprotocol. The `/chat` example above separately
demonstrates explicit negotiation: a client must offer `chat.v1` before the
handler may select it.
