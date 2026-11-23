# ABOUTME: The Playpen feed service end to end against the scripted fixture run, over HTTP in
# process: each scene's state, approve forwarded to the proxy, the event stream, the page itself,
# a source that is down, the audit cursor, and Temporal history events as the page shows them.

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.api.common.v1 import ActivityType, Payload, Payloads
from temporalio.api.enums.v1 import EventType
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.history.v1 import (
    ActivityTaskCompletedEventAttributes,
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    HistoryEvent,
)
from temporalio.api.taskqueue.v1 import TaskQueue

from examples.governed_program.feed.fixture import DECISION_ID, WORKFLOW_ID, FixtureWorld, create_backend
from examples.governed_program.feed.server import UI_DIR, FeedConfig, create_app
from examples.governed_program.feed.sources import AuditTail, HttpHistory, history_event_to_dict


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 11, 11, 19, 4, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def world(clock: Clock) -> FixtureWorld:
    return FixtureWorld(clock=clock)


@pytest.fixture
async def feed(world: FixtureWorld) -> AsyncIterator[httpx.AsyncClient]:
    backend = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_backend(world)), base_url="http://fixture")
    config = FeedConfig(proxy_url="http://fixture", control_plane_url="http://fixture", poll_seconds=0.01, ui_dir=UI_DIR)
    app = create_app(config, http=backend, history=HttpHistory(backend, "http://fixture"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://feed") as client:
        yield client
    await backend.aclose()


async def state(feed: httpx.AsyncClient) -> dict:
    response = await feed.get(f"/api/session/{WORKFLOW_ID}")
    assert response.status_code == 200
    return response.json()


async def test_the_run_scene_by_scene(feed: httpx.AsyncClient, world: FixtureWorld, clock: Clock) -> None:
    clock.tick(1)
    s = await state(feed)
    assert not s["started"] and s["status"]["label"] == "Drafting" and s["sandbox"]["state"] == "none"
    assert s["conversation"][0]["role"] == "user"

    world.set_scene(2)
    clock.tick(6)
    s = await state(feed)
    assert s["started"] and s["sandbox"]["name"] == "sbx-1" and s["sandbox"]["network"] == "none"
    assert "launch" in s["map"]["active_edges"]

    world.set_scene(3)
    clock.tick(6)
    s = await state(feed)
    assert [(l["label"], l["verdict"]) for l in s["lanes"]] == [("run_query analytics.branch_deposits", "ALLOW")]
    assert s["program"]["current_step"] == "deposits"

    world.set_scene(4)
    clock.tick(6)
    s = await state(feed)
    assert s["held"] == 1 and s["status"]["label"] == "Waiting for approval"
    [card] = s["approvals"]
    assert card["state"] == "pending" and card["rule"] == "risk-tables-need-approval"
    assert s["lanes"][-1]["kind"] == "pending"

    response = await feed.post(f"/api/decisions/{card['id']}", json={"decision": "approve"})
    assert response.json() == {"ok": True, "proxy_status": 204, "proxy_body": ""}
    again = await feed.post(f"/api/decisions/{card['id']}", json={"decision": "deny"})
    assert again.status_code == 409 and not again.json()["ok"]
    clock.tick(4)
    s = await state(feed)
    assert s["held"] == 0 and s["approvals"][0]["resolved_by"] == "Risk reviewer"
    assert [r["verdict"] for r in s["decisions"]][-3:] == ["ESCALATE", "APPROVED", "ALLOW"]
    assert s["lanes"][-1]["verdict"] == "APPROVED"

    world.set_scene(5)
    clock.tick(6)
    s = await state(feed)
    assert [l["kind"] for l in s["lanes"] if l["fresh"]] == ["blocked-sandbox", "deny-proxy", "deny-proxy"]
    assert s["status"]["label"] == "Running · 3 calls blocked"
    assert any(l["injected"] for l in s["program"]["lines"])

    world.set_scene(6)
    clock.tick(3)
    s = await state(feed)
    assert s["sandbox"]["state"] == "killed" and s["history"]["gap"]["open"]
    world.resume()
    clock.tick(4)
    s = await state(feed)
    assert s["sandbox"]["name"] == "sbx-2" and s["status"]["label"] == "Running on sbx-2"
    assert s["history"]["gap"]["open"] is False

    world.set_scene(7)
    clock.tick(6)
    s = await state(feed)
    assert s["status"]["label"] == "Completed" and s["workflow_status"] == "COMPLETED"
    assert s["decisions"][-1]["call"] == "send_email risk-team@bank.example"
    assert {r["policy_hash"] for r in s["decisions"]} == {"7c41e9a2", "n/a"}
    assert s["sources"] == {}


async def test_deny_and_unknown_decisions(feed: httpx.AsyncClient, world: FixtureWorld, clock: Clock) -> None:
    world.set_scene(4)
    clock.tick(6)
    missing = await feed.post("/api/decisions/nope", json={"decision": "approve"})
    assert missing.status_code == 404
    bad = await feed.post(f"/api/decisions/{DECISION_ID}", json={"decision": "maybe"})
    assert bad.status_code == 400
    await feed.post(f"/api/decisions/{DECISION_ID}", json={"decision": "deny"})
    clock.tick(2)
    s = await state(feed)
    assert s["approvals"][0]["state"] == "denied"
    assert s["lanes"][-1]["verdict"] == "DENIED"


async def test_stream_sends_the_state(feed: httpx.AsyncClient) -> None:
    async with feed.stream("GET", f"/api/session/{WORKFLOW_ID}/stream", params={"limit": 1}) as response:
        assert response.headers["content-type"].startswith("text/event-stream")
        body = (await response.aread()).decode()
    assert body.startswith("data: ")
    assert json.loads(body.removeprefix("data: "))["workflow_id"] == WORKFLOW_ID


async def test_serves_the_page_and_sessions(feed: httpx.AsyncClient) -> None:
    page = await feed.get("/")
    assert page.status_code == 200 and "<title>Playpen</title>" in page.text
    for asset in ("app.js", "map.js", "util.js", "style.css"):
        assert (await feed.get(f"/{asset}")).status_code == 200
    sessions = (await feed.get("/api/sessions")).json()
    assert sessions[-1]["workflow_id"] == WORKFLOW_ID


async def test_a_source_that_is_down_is_reported_and_the_rest_renders(world: FixtureWorld, clock: Clock) -> None:
    clock.tick(1)
    backend_app = create_backend(world)
    backend = httpx.ASGITransport(app=backend_app)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith(("/audit", "/decisions")):
            raise httpx.ConnectError("connection refused", request=request)
        return await backend.handle_async_request(request)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://fixture")
    config = FeedConfig(proxy_url="http://fixture", control_plane_url="http://fixture", ui_dir=None)
    app = create_app(config, http=http, history=HttpHistory(http, "http://fixture"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://feed") as client:
        s = (await client.get(f"/api/session/{WORKFLOW_ID}")).json()
    assert set(s["sources"]) == {"audit", "decisions"}
    assert s["conversation"]  # the control plane still answered
    await http.aclose()


async def test_audit_tail_follows_the_cursor_and_starts_over_after_a_proxy_restart() -> None:
    log: list[dict] = [{"seq": 1, "verdict": "allowed"}, {"seq": 2, "verdict": "denied"}]

    def handler(request: httpx.Request) -> httpx.Response:
        since = int(request.url.params["since"])
        records = [r for r in log if r["seq"] > since]
        # As the proxy does: a cursor beyond the newest record comes back unchanged.
        return httpx.Response(200, json={"records": records, "next": max(since, log[-1]["seq"] if log else 0)})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        tail = AuditTail(http, "http://proxy")
        assert [r["seq"] for r in await tail.poll()] == [1, 2]
        assert [r["seq"] for r in await tail.poll()] == [1, 2]
        log.append({"seq": 3, "verdict": "allowed"})
        assert [r["seq"] for r in await tail.poll()] == [1, 2, 3]
        log[:] = [{"seq": 1, "verdict": "escalated"}]
        assert [r["verdict"] for r in await tail.poll()] == ["escalated"]
        log[:] = [{"seq": i, "verdict": "allowed", "name": f"fresh-{i}"} for i in (1, 2, 3, 4)]
        assert [r["name"] for r in await tail.poll()] == ["fresh-1", "fresh-2", "fresh-3", "fresh-4"]


def _event(event_id: int, event_type: EventType.ValueType, **attrs) -> HistoryEvent:
    t = Timestamp()
    t.FromDatetime(datetime(2026, 11, 11, 19, 4, event_id))
    return HistoryEvent(event_id=event_id, event_type=event_type, event_time=t, **attrs)


def test_history_events_as_the_page_shows_them() -> None:
    scheduled = _event(
        5,
        EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        activity_task_scheduled_event_attributes=ActivityTaskScheduledEventAttributes(
            activity_type=ActivityType(name="run_query"), task_queue=TaskQueue(name="governed-program-tools")
        ),
    )
    rows = Payloads(payloads=[Payload(data=json.dumps({"columns": ["a"], "rows": [[1], [2]]}).encode())])
    completed = _event(
        7,
        EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
        activity_task_completed_event_attributes=ActivityTaskCompletedEventAttributes(result=rows),
    )
    failed = _event(
        8,
        EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
        activity_task_failed_event_attributes=ActivityTaskFailedEventAttributes(
            failure=Failure(
                message="denied by no-email-outside-the-bank",
                application_failure_info=ApplicationFailureInfo(type="PolicyDenied"),
            )
        ),
    )
    assert history_event_to_dict(scheduled) | {"time": ""} == {
        "id": 5,
        "type": "ActivityTaskScheduled",
        "time": "",
        "detail": "run_query · tools",
        "identity": "",
    }
    assert history_event_to_dict(completed)["detail"] == "2 rows"
    assert history_event_to_dict(failed)["detail"] == "PolicyDenied: denied by no-email-outside-the-bank"
    assert history_event_to_dict(failed)["time"] == "2026-11-11T19:04:08Z"
