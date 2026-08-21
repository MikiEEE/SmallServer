"""Run a SmallOS-backed SmallServer listener and loopback TCP client."""

from __future__ import annotations

import json
import socket
import threading

from SmallPackage import SmallOS, Unix

from smallserver import HTTPError, Request, Response, SmallServer


runtime = SmallOS().setKernel(Unix())
app = SmallServer()
tasks: dict[str, dict[str, object]] = {}


def request_json(request: Request) -> dict[str, object]:
    """Decode one JSON object from a demo request body."""
    try:
        value = json.loads(request.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPError(400, "request body must be a JSON object") from exc
    if not isinstance(value, dict):
        raise HTTPError(400, "request body must be a JSON object")
    return value


@app.get("/tasks")
async def list_tasks(request: Request) -> Response:
    return Response.json({"tasks": list(tasks.values())})


@app.post("/tasks")
async def create_task(request: Request) -> Response:
    data = request_json(request)
    title = data.get("title")
    if not isinstance(title, str) or not title.strip():
        raise HTTPError(400, "title is required")
    task = {"id": str(len(tasks) + 1), "title": title, "done": False}
    tasks[task["id"]] = task
    return Response.json(task, status=201)


@app.put("/tasks")
async def replace_tasks(request: Request) -> Response:
    data = request_json(request)
    new_tasks = data.get("tasks")
    if not isinstance(new_tasks, list):
        raise HTTPError(400, "tasks must be a JSON list")
    tasks.clear()
    for position, item in enumerate(new_tasks, start=1):
        if not isinstance(item, dict) or not isinstance(item.get("title"), str):
            raise HTTPError(400, "each task needs a title")
        task = {"id": str(position), "title": item["title"], "done": bool(item.get("done"))}
        tasks[task["id"]] = task
    return Response.json({"tasks": list(tasks.values())})


@app.patch("/tasks")
async def patch_task(request: Request) -> Response:
    data = request_json(request)
    task_id = data.get("id")
    task = tasks.get(task_id)
    if task is None:
        raise HTTPError(404, "task not found")
    if "title" in data:
        if not isinstance(data["title"], str) or not data["title"].strip():
            raise HTTPError(400, "title must be a non-empty string")
        task["title"] = data["title"]
    if "done" in data:
        if not isinstance(data["done"], bool):
            raise HTTPError(400, "done must be a boolean")
        task["done"] = data["done"]
    return Response.json(task)


@app.delete("/tasks")
async def delete_task(request: Request) -> Response:
    data = request_json(request)
    task_id = data.get("id")
    if task_id not in tasks:
        raise HTTPError(404, "task not found")
    del tasks[task_id]
    return Response(status=204)


def send_request(port: int, method: str, path: str, body: object | None = None) -> bytes:
    """Send one HTTP/1.1 request to the demo listener and read it to close."""
    payload = b"" if body is None else json.dumps(body).encode("utf-8")
    lines = ["{} {} HTTP/1.1".format(method, path), "Host: localhost"]
    if payload:
        lines.extend(("Content-Type: application/json", "Content-Length: {}".format(len(payload))))
    request = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + payload
    with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
        client.sendall(request)
        chunks = []
        while True:
            chunk = client.recv(4096)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)


def run_client(port: int, close_server) -> None:
    """Exercise every implemented method from outside the SmallOS thread."""
    try:
        calls = [
            ("POST", "/tasks", {"title": "Ship the first SmallServer demo"}),
            ("GET", "/tasks", None),
            ("PATCH", "/tasks", {"id": "1", "done": True}),
            ("PUT", "/tasks", {"tasks": [{"title": "Add socket listener", "done": False}]}),
            ("DELETE", "/tasks", {"id": "1"}),
            ("GET", "/missing", None),
        ]
        for method, path, body in calls:
            wire = send_request(port, method, path, body)
            status_line, _, response_body = wire.partition(b"\r\n\r\n")
            print("{} {} -> {} {}".format(method, path, status_line.decode(), response_body.decode()))
    finally:
        close_server()


if __name__ == "__main__":
    server = app.serve(runtime, host="127.0.0.1", port=0)
    print("SmallServer listening on http://127.0.0.1:{}".format(server.port))
    threading.Thread(target=run_client, args=(server.port, server.close), daemon=True).start()
    runtime.start()
