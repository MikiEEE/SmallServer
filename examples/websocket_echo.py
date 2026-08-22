"""Run a bounded WebSocket echo endpoint on localhost:8000."""

from smallserver import SmallServer, WebSocket, WebSocketConfig


app = SmallServer(
    websocket_config=WebSocketConfig(
        max_frame_payload_bytes=64 * 1024,
        max_message_bytes=256 * 1024,
        max_inbound_messages=8,
        max_outbound_commands=8,
    )
)


@app.websocket("/echo")
async def echo(socket: WebSocket) -> None:
    await socket.accept()
    async for message in socket:
        if message.is_text:
            await socket.send_text(message.text)
        else:
            await socket.send_bytes(message.bytes)


if __name__ == "__main__":
    print("Starting WebSocket echo server on ws://127.0.0.1:8000/echo")
    app.listen(host="127.0.0.1", port=8000)
