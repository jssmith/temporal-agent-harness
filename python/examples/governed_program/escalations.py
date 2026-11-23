"""The console's side of escalation: list and resolve the proxy's pending human decisions.

When the tool policy escalates a call, the proxy (``temporal-untrusted-workers``, run with
``TEMPORAL_PROXY_ESCALATION_ADDR``) holds the guest's workflow task open and records a pending
decision keyed by the workflow run and the call. It answers the guest ``Unavailable`` until a
person resolves the decision here: approve lets the guest's retry through, and the same call is
not asked about again in that run; deny fails the call in the workflow as ``PolicyDenied`` (with
``TEMPORAL_PROXY_DENIAL_TASK_QUEUE`` set; otherwise the proxy rejects the retry).

:func:`audit_since` reads the proxy's audit log from the same API.

These two functions are what a console approval page calls. A real page is a later slice; the
proxy's API is unauthenticated, so ``base_url`` must be the trusted control plane's own address
for it (loopback), never one the guest can reach.

The proxy's API::

    GET  /decisions[?state=pending]
    POST /decisions/{id}        {"decision": "approve" | "deny", "reason": "...", "reviewer": "..."}
    GET  /audit?since=<seq>     {"records": [...], "next": <seq>}
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# Where the proxy's escalation API listens by default in the local demo.
DEFAULT_BASE_URL = "http://127.0.0.1:8099"

DecisionState = Literal["pending", "approved", "denied"]


class EscalationError(Exception):
    """The proxy refused or could not be asked; ``status`` is the HTTP status, 0 if unreachable."""

    def __init__(self, message: str, status: int = 0) -> None:
        super().__init__(message)
        self.status = status


class PendingDecision(BaseModel):
    """One escalated call, as the proxy lists it."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    id: str
    state: DecisionState
    namespace: str
    workflow_id: str = Field(alias="workflowId")
    run_id: str = Field(alias="runId")
    # "<taskQueue>/<activityType>", in command order.
    calls: list[str]
    # Each call's parameters, in protobuf JSON form, so a reviewer sees what is being approved.
    parameters: list[list[Any]] = Field(default_factory=list)
    # The policy's reason for escalating.
    reason: str
    rule: str = ""
    resolution: str = ""
    resolved_by: str = Field(default="", alias="resolvedBy")
    created_at: datetime = Field(alias="createdAt")


def _request(method: str, url: str, body: dict[str, Any] | None = None) -> Any:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = response.read()
    except urllib.error.HTTPError as err:
        detail = err.read().decode(errors="replace").strip()
        raise EscalationError(f"{method} {url}: {err.code} {detail}", err.code) from err
    except (urllib.error.URLError, OSError) as err:
        raise EscalationError(f"{method} {url}: {err}") from err
    return json.loads(payload) if payload else None


async def list_decisions(
    *, pending_only: bool = True, base_url: str = DEFAULT_BASE_URL
) -> list[PendingDecision]:
    """The proxy's recorded decisions, oldest first; only those waiting for a person by default."""
    url = f"{base_url.rstrip('/')}/decisions" + ("?state=pending" if pending_only else "")
    listed = await asyncio.to_thread(_request, "GET", url)
    return [PendingDecision.model_validate(item) for item in listed or []]


async def resolve_decision(
    decision_id: str,
    *,
    approve: bool,
    reason: str = "",
    reviewer: str = "",
    base_url: str = DEFAULT_BASE_URL,
) -> None:
    """Record a person's answer; ``reviewer`` names them in the audit log. Raises
    :class:`EscalationError` with status 404 for an unknown decision and 409 for one that
    already has an answer."""
    url = f"{base_url.rstrip('/')}/decisions/{decision_id}"
    body = {"decision": "approve" if approve else "deny", "reason": reason}
    if reviewer:
        body["reviewer"] = reviewer
    await asyncio.to_thread(_request, "POST", url, body)


async def audit_since(since: int = 0, *, base_url: str = DEFAULT_BASE_URL) -> tuple[list[dict[str, Any]], int]:
    """The proxy's audit records after ``since``, oldest first, and the cursor to pass next."""
    url = f"{base_url.rstrip('/')}/audit?since={since}"
    page = await asyncio.to_thread(_request, "GET", url)
    return list(page["records"]), int(page["next"])
