"""Clients for the feed's sources. Each returns plain JSON-shaped data for ``state.build_state``
and raises :class:`SourceError` when its source cannot be read, so one source being down shows on
the page as that source being down rather than as an empty run.

The proxy (``temporal-untrusted-workers``, loopback only; the guest can never reach it)::

    GET  /audit?since=<seq>     {"records": [...], "next": <seq>}
    GET  /decisions             [{"id", "state", "workflowId", "runId", "calls", ...}]
    POST /decisions/{id}        {"decision": "approve" | "deny", "reason": "..."}

The control plane: see ``README.md`` beside this file.
"""

from __future__ import annotations

import json
from typing import Any, Protocol

import httpx


class SourceError(Exception):
    def __init__(self, message: str, status: int = 0) -> None:
        super().__init__(message)
        self.status = status


async def _get(http: httpx.AsyncClient, url: str, **params: Any) -> Any:
    try:
        response = await http.get(url, params=params or None)
    except httpx.HTTPError as err:
        raise SourceError(f"GET {url}: {err.__class__.__name__} {err}") from err
    if response.status_code == 404:
        return None
    if response.status_code >= 400:
        raise SourceError(f"GET {url}: {response.status_code} {response.text.strip()}", response.status_code)
    return response.json()


class AuditTail:
    """Follows the proxy's audit feed with its ``since`` cursor and keeps every record seen.

    Each poll asks from just before the last record seen and expects that record back first. If
    it is missing or different, the proxy restarted with a fresh log (which answers a cursor
    beyond its newest record with nothing, not an error): start over."""

    def __init__(self, http: httpx.AsyncClient, base_url: str, limit: int = 20000) -> None:
        self.http = http
        self.url = f"{base_url.rstrip('/')}/audit"
        self.records: list[dict] = []
        self.cursor = 0
        self.limit = limit

    @staticmethod
    def _same(a: dict, b: dict) -> bool:
        return all(a.get(k) == b.get(k) for k in ("seq", "time", "name", "verdict"))

    async def poll(self) -> list[dict]:
        anchor = self.records[-1] if self.records else None
        since = anchor["seq"] - 1 if anchor else self.cursor
        body = await _get(self.http, self.url, since=since)
        if body is None:
            raise SourceError(f"GET {self.url}: 404 (proxy without an audit feed)", 404)
        records = body.get("records") or []
        nxt = body.get("next", since)
        if anchor:
            if not records or not self._same(records[0], anchor):
                self.records, self.cursor = [], 0
                return await self.poll()
            records = records[1:]
        known = {r.get("seq") for r in self.records[-len(records) :]} if records else set()
        self.records.extend(r for r in records if r.get("seq") not in known)
        self.records = self.records[-self.limit :]
        self.cursor = nxt
        return self.records


class ProxyDecisions:
    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self.http = http
        self.base = base_url.rstrip("/")

    async def list(self) -> list[dict]:
        return await _get(self.http, f"{self.base}/decisions") or []

    async def resolve(self, decision_id: str, decision: str, reason: str = "") -> tuple[int, str]:
        url = f"{self.base}/decisions/{decision_id}"
        try:
            response = await self.http.post(url, json={"decision": decision, "reason": reason})
        except httpx.HTTPError as err:
            raise SourceError(f"POST {url}: {err.__class__.__name__} {err}") from err
        return response.status_code, response.text


class ControlPlane:
    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self.http = http
        self.base = base_url.rstrip("/")

    async def session(self, workflow_id: str) -> dict | None:
        return await _get(self.http, f"{self.base}/api/playpen/session/{workflow_id}")

    async def sessions(self) -> list[dict]:
        return await _get(self.http, f"{self.base}/api/playpen/sessions") or []


class HistorySource(Protocol):
    async def fetch(self, workflow_id: str) -> dict | None:
        """``{"status", "run_id", "events": [...]}`` for the workflow's latest run, None if
        there is no such workflow yet."""


class HttpHistory:
    """History already summarized by another service (the fixture backend)."""

    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self.http = http
        self.base = base_url.rstrip("/")

    async def fetch(self, workflow_id: str) -> dict | None:
        return await _get(self.http, f"{self.base}/fixture/history/{workflow_id}")


# ---------------------------------------------------------------- Temporal

_SHORT_QUEUES = ("governed-program-",)


def _queue(name: str) -> str:
    for prefix in _SHORT_QUEUES:
        if name.startswith(prefix):
            return name.removeprefix(prefix)
    return name


def _payload_values(payloads: Any) -> list[Any]:
    out = []
    for p in getattr(payloads, "payloads", []):
        try:
            out.append(json.loads(p.data))
        except ValueError:
            out.append(p.data.decode(errors="replace"))
    return out


def _result_summary(values: list[Any]) -> str:
    if not values:
        return ""
    v = values[0]
    if isinstance(v, dict):
        if isinstance(v.get("rows"), list):
            return f"{len(v['rows'])} rows"
        if v.get("to"):
            return str(v["to"])
        if isinstance(v.get("steps"), list):
            return f"{len(v['steps'])} steps"
    if isinstance(v, list):
        return f"{len(v)} request{'s' if len(v) != 1 else ''}"
    return ""


def _enum_words(name: str, prefix: str) -> str:
    return name.removeprefix(prefix).replace("_", " ").lower()


def history_event_to_dict(ev: Any) -> dict:
    """One Temporal history event as the page shows it: its type, time, and a few words."""
    from temporalio.api.enums.v1 import EventType, TaskQueueKind, TimeoutType

    etype = EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_")
    etype = "".join(part.capitalize() for part in etype.split("_"))
    detail = ""
    identity = ""
    if ev.HasField("workflow_execution_started_event_attributes"):
        a = ev.workflow_execution_started_event_attributes
        detail = f"{a.workflow_type.name} on {_queue(a.task_queue.name)}"
        identity = a.identity
    elif ev.HasField("workflow_task_scheduled_event_attributes"):
        tq = ev.workflow_task_scheduled_event_attributes.task_queue
        detail = "sticky queue" if tq.kind == TaskQueueKind.TASK_QUEUE_KIND_STICKY else _queue(tq.name)
    elif ev.HasField("workflow_task_started_event_attributes"):
        identity = ev.workflow_task_started_event_attributes.identity
        detail = identity
    elif ev.HasField("workflow_task_timed_out_event_attributes"):
        t = ev.workflow_task_timed_out_event_attributes.timeout_type
        detail = _enum_words(TimeoutType.Name(t), "TIMEOUT_TYPE_")
    elif ev.HasField("workflow_task_failed_event_attributes"):
        a = ev.workflow_task_failed_event_attributes
        detail = a.failure.message or a.cause and str(a.cause)
    elif ev.HasField("activity_task_scheduled_event_attributes"):
        a = ev.activity_task_scheduled_event_attributes
        detail = f"{a.activity_type.name} · {_queue(a.task_queue.name)}"
    elif ev.HasField("activity_task_started_event_attributes"):
        identity = ev.activity_task_started_event_attributes.identity
        detail = identity
    elif ev.HasField("activity_task_completed_event_attributes"):
        detail = _result_summary(_payload_values(ev.activity_task_completed_event_attributes.result))
    elif ev.HasField("activity_task_failed_event_attributes"):
        f = ev.activity_task_failed_event_attributes.failure
        kind = f.application_failure_info.type if f.HasField("application_failure_info") else ""
        detail = f"{kind}: {f.message}" if kind else f.message
    elif ev.HasField("workflow_execution_update_accepted_event_attributes"):
        detail = ev.workflow_execution_update_accepted_event_attributes.accepted_request.input.name
    elif ev.HasField("workflow_execution_signaled_event_attributes"):
        detail = ev.workflow_execution_signaled_event_attributes.signal_name
    elif ev.HasField("workflow_execution_completed_event_attributes"):
        detail = _result_summary(_payload_values(ev.workflow_execution_completed_event_attributes.result))
    elif ev.HasField("workflow_execution_failed_event_attributes"):
        detail = ev.workflow_execution_failed_event_attributes.failure.message
    elif ev.HasField("workflow_execution_terminated_event_attributes"):
        detail = ev.workflow_execution_terminated_event_attributes.reason
    return {
        "id": ev.event_id,
        "type": etype,
        "time": ev.event_time.ToJsonString(),
        "detail": detail,
        "identity": identity,
    }


class TemporalHistory:
    def __init__(self, address: str, namespace: str) -> None:
        self.address = address
        self.namespace = namespace
        self._client: Any = None

    async def _connect(self) -> Any:
        if self._client is None:
            from temporalio.client import Client

            try:
                self._client = await Client.connect(self.address, namespace=self.namespace)
            except Exception as err:  # noqa: BLE001 - reported as the source being down
                raise SourceError(f"Temporal {self.address}: {err}") from err
        return self._client

    async def fetch(self, workflow_id: str) -> dict | None:
        from temporalio.client import WorkflowExecutionStatus
        from temporalio.service import RPCError, RPCStatusCode

        client = await self._connect()
        handle = client.get_workflow_handle(workflow_id)
        try:
            desc = await handle.describe()
            events = [history_event_to_dict(ev) async for ev in handle.fetch_history_events()]
        except RPCError as err:
            if err.status == RPCStatusCode.NOT_FOUND:
                return None
            raise SourceError(f"Temporal: {err}") from err
        status = WorkflowExecutionStatus(desc.status).name if desc.status else "UNKNOWN"
        return {"status": status, "run_id": desc.run_id, "events": events}
