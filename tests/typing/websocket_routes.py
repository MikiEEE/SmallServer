from smallserver import SmallServer, WebSocket, WebSocketMessage


app = SmallServer()


@app.websocket("/chat", subprotocols=("chat.v1",))
async def chat(socket: WebSocket) -> None:
    await socket.accept(subprotocol="chat.v1")
    message: WebSocketMessage = await socket.receive()
    if message.is_text:
        await socket.send_text(message.text)
    else:
        await socket.send_bytes(message.bytes)
