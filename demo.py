"""Run the beginner-facing SmallServer task API on localhost:8000."""

from __future__ import annotations

import json

from smallserver import HTTPError, Request, Response, SmallServer


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


if __name__ == "__main__":
    print("SmallServer listening on http://127.0.0.1:8000")
    app.listen(host="127.0.0.1", port=8000)
