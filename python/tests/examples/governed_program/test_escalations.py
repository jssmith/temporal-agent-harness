# ABOUTME: The console-side escalation client against a stand-in for the proxy's escalation API
# (a stdlib HTTP server): it lists pending decisions and resolves them (naming the reviewer),
# reads the audit log, and reports the proxy's 404 and 409 answers as EscalationError.

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from examples.governed_program.escalations import (
    EscalationError,
    audit_since,
    list_decisions,
    resolve_decision,
)

_PENDING = {
    "id": "d1",
    "state": "pending",
    "namespace": "ns",
    "workflowId": "wf-1",
    "runId": "run-1",
    "calls": ["governed-program-tools/run_query"],
    "parameters": [[{"@type": "type.googleapis.com/google.protobuf.Value", "value": {"sql": "SELECT 1"}}]],
    "reason": "pending human review: Risk tables need a person's approval",
    "createdAt": "2026-11-11T18:00:00Z",
}


class _FakeProxy(BaseHTTPRequestHandler):
    # Class-level so the test can read what the handler saw.
    resolved: dict[str, dict] = {}

    def log_message(self, *args) -> None:
        pass

    def _send(self, status: int, body: bytes = b"") -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/decisions?state=pending":
            self._send(200, json.dumps([_PENDING]).encode())
        elif self.path == "/decisions":
            self._send(200, json.dumps([_PENDING, {**_PENDING, "id": "d0", "state": "approved"}]).encode())
        elif self.path == "/audit?since=3":
            self._send(200, json.dumps({"records": [{"seq": 4, "verdict": "escalated"}], "next": 4}).encode())
        else:
            self._send(404)

    def do_POST(self) -> None:
        decision_id = self.path.removeprefix("/decisions/")
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if decision_id == "d1" and decision_id not in self.resolved:
            self.resolved[decision_id] = body
            self._send(204)
        elif decision_id == "d1":
            self._send(409, b"decision already resolved")
        else:
            self._send(404, b"unknown decision")


@pytest.fixture
def proxy_url() -> Iterator[str]:
    _FakeProxy.resolved = {}
    server = HTTPServer(("127.0.0.1", 0), _FakeProxy)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


async def test_list_decisions(proxy_url: str):
    pending = await list_decisions(base_url=proxy_url)
    assert [(d.id, d.state, d.workflow_id, d.run_id) for d in pending] == [("d1", "pending", "wf-1", "run-1")]
    assert pending[0].calls == ["governed-program-tools/run_query"]
    everything = await list_decisions(pending_only=False, base_url=proxy_url)
    assert [d.state for d in everything] == ["pending", "approved"]


async def test_resolve_decision_then_conflict_and_unknown(proxy_url: str):
    await resolve_decision("d1", approve=True, reviewer="Risk reviewer", base_url=proxy_url)
    assert _FakeProxy.resolved["d1"] == {"decision": "approve", "reason": "", "reviewer": "Risk reviewer"}

    with pytest.raises(EscalationError) as conflict:
        await resolve_decision("d1", approve=False, reason="changed my mind", base_url=proxy_url)
    assert conflict.value.status == 409

    with pytest.raises(EscalationError) as unknown:
        await resolve_decision("nope", approve=True, base_url=proxy_url)
    assert unknown.value.status == 404


async def test_unreachable_proxy_is_an_escalation_error():
    with pytest.raises(EscalationError) as err:
        await list_decisions(base_url="http://127.0.0.1:1")
    assert err.value.status == 0


async def test_audit_since(proxy_url: str):
    records, next_seq = await audit_since(3, base_url=proxy_url)
    assert records == [{"seq": 4, "verdict": "escalated"}]
    assert next_seq == 4
