"""The Playpen feed service and page.

    GET  /                                  the page (``../ui``); ``?wid=<workflow id>``
    GET  /api/sessions                      the control plane's sessions, newest last
    GET  /api/session/{workflow_id}         the run's state (``state.build_state``)
    GET  /api/session/{workflow_id}/stream  the same, as server-sent events on each change
    POST /api/decisions/{id}                {"decision": "approve" | "deny"} -> proxy

Live, beside the trusted worker::

    python -m examples.governed_program.feed.server

Against the scripted run in ``fixture.py`` (a fake proxy, control plane and history on
``--fixture-port``), with nothing else running::

    python -m examples.governed_program.feed.server --fixture
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from examples.governed_program.feed.sources import (
    AuditTail,
    ControlPlane,
    HistorySource,
    HttpHistory,
    ProxyDecisions,
    SourceError,
    TemporalHistory,
)
from examples.governed_program.feed.state import build_state

UI_DIR = Path(__file__).parent.parent / "ui"


@dataclass
class FeedConfig:
    proxy_url: str = "http://127.0.0.1:8099"
    control_plane_url: str = "http://127.0.0.1:8090"
    temporal_address: str = "127.0.0.1:7233"
    namespace: str = "default"
    temporal_ui: str = "http://localhost:8233"
    poll_seconds: float = 0.5
    ui_dir: Path | None = field(default_factory=lambda: UI_DIR)

    @classmethod
    def from_env(cls) -> FeedConfig:
        env = os.environ.get
        return cls(
            proxy_url=env("PLAYPEN_PROXY_URL", cls.proxy_url),
            control_plane_url=env("PLAYPEN_CONTROL_PLANE_URL", cls.control_plane_url),
            temporal_address=env("TEMPORAL_ADDRESS", cls.temporal_address),
            namespace=env("TEMPORAL_NAMESPACE", cls.namespace),
            temporal_ui=env("PLAYPEN_TEMPORAL_UI", cls.temporal_ui),
        )


class Feed:
    """Reads every source for one workflow and builds its state. Each source failing is
    reported in ``state["sources"]``; the rest still render."""

    def __init__(self, config: FeedConfig, http: httpx.AsyncClient, history: HistorySource) -> None:
        self.config = config
        self.audit = AuditTail(http, config.proxy_url)
        self.decisions = ProxyDecisions(http, config.proxy_url)
        self.control = ControlPlane(http, config.control_plane_url)
        self.history = history
        self._audit_lock = asyncio.Lock()

    async def _audit(self) -> list[dict]:
        async with self._audit_lock:
            return await self.audit.poll()

    async def snapshot(self, workflow_id: str) -> dict:
        names = ("audit", "decisions", "history", "control")
        results = await asyncio.gather(
            self._audit(),
            self.decisions.list(),
            self.history.fetch(workflow_id),
            self.control.session(workflow_id),
            return_exceptions=True,
        )
        values: dict[str, Any] = {}
        errors: dict[str, str] = {}
        for name, result in zip(names, results, strict=True):
            if isinstance(result, BaseException):
                if not isinstance(result, SourceError):
                    raise result
                errors[name] = str(result)
                values[name] = self.audit.records if name == "audit" else None
            else:
                values[name] = result
        return build_state(
            workflow_id,
            audit=values["audit"] or [],
            decisions=values["decisions"] or [],
            history=values["history"],
            control=values["control"],
            temporal_ui=self.config.temporal_ui,
            namespace=self.config.namespace,
            source_errors=errors,
        )


def _changed_part(state: dict) -> str:
    """What a page needs a new event for: everything but the clock."""
    return json.dumps({k: v for k, v in state.items() if k != "now"}, sort_keys=True, default=str)


def create_app(config: FeedConfig, *, http: httpx.AsyncClient | None = None, history: HistorySource | None = None) -> FastAPI:
    owns_http = http is None
    http = http or httpx.AsyncClient(timeout=httpx.Timeout(3.0))
    history = history or TemporalHistory(config.temporal_address, config.namespace)
    feed = Feed(config, http, history)

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if owns_http:
            await http.aclose()

    app = FastAPI(title="Playpen feed", lifespan=lifespan)
    app.state.feed = feed

    @app.get("/api/sessions")
    async def sessions() -> Any:
        try:
            return await feed.control.sessions()
        except SourceError as err:
            return JSONResponse({"error": str(err)}, status_code=502)

    @app.get("/api/session/{workflow_id}")
    async def session(workflow_id: str) -> dict:
        return await feed.snapshot(workflow_id)

    @app.get("/api/session/{workflow_id}/stream")
    async def stream(workflow_id: str, request: Request, limit: int = 0) -> StreamingResponse:
        async def events() -> AsyncIterator[str]:
            last = None
            sent = 0
            idle = 0.0
            while True:
                state = await feed.snapshot(workflow_id)
                part = _changed_part(state)
                if part != last:
                    last = part
                    sent += 1
                    idle = 0.0
                    yield f"data: {json.dumps(state, default=str)}\n\n"
                    if limit and sent >= limit:
                        return
                elif idle >= 15:
                    idle = 0.0
                    yield ": keepalive\n\n"
                if await request.is_disconnected():
                    return
                await asyncio.sleep(config.poll_seconds)
                idle += config.poll_seconds

        return StreamingResponse(
            events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    @app.post("/api/decisions/{decision_id}")
    async def decide(decision_id: str, request: Request) -> JSONResponse:
        body = await request.json()
        decision = body.get("decision")
        if decision not in ("approve", "deny"):
            raise HTTPException(400, "decision must be approve or deny")
        try:
            status, text = await feed.decisions.resolve(decision_id, decision, body.get("reason", ""))
        except SourceError as err:
            return JSONResponse({"ok": False, "error": str(err)}, status_code=502)
        ok = 200 <= status < 300
        return JSONResponse({"ok": ok, "proxy_status": status, "proxy_body": text}, status_code=200 if ok else status)

    if config.ui_dir and config.ui_dir.is_dir():
        # Under Bazel the page's files are symlinks into the source tree.
        app.mount("/", StaticFiles(directory=config.ui_dir, html=True, follow_symlink=True), name="ui")
    return app


async def _serve(apps: list[tuple[FastAPI, int]], host: str) -> None:
    import uvicorn

    servers = [uvicorn.Server(uvicorn.Config(app, host=host, port=port, log_level="warning")) for app, port in apps]
    await asyncio.gather(*(s.serve() for s in servers))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=int(os.environ.get("PLAYPEN_FEED_PORT", "8095")))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--fixture", action="store_true", help="serve the scripted run instead of the live stack")
    parser.add_argument("--fixture-port", type=int, default=8199)
    args = parser.parse_args()

    config = FeedConfig.from_env()
    apps: list[tuple[FastAPI, int]] = []
    if args.fixture:
        from examples.governed_program.feed.fixture import create_backend

        fixture_url = f"http://{args.host}:{args.fixture_port}"
        config.proxy_url = config.control_plane_url = fixture_url
        http = httpx.AsyncClient(timeout=httpx.Timeout(3.0))
        apps.append((create_backend(), args.fixture_port))
        apps.append((create_app(config, http=http, history=HttpHistory(http, fixture_url)), args.port))
        print(f"fixture backend on {fixture_url} (POST /fixture/scene/<n> to move the run)")
    else:
        apps.append((create_app(config), args.port))
    print(f"Playpen on http://{args.host}:{args.port}/")
    asyncio.run(_serve(apps, args.host))


if __name__ == "__main__":
    main()
