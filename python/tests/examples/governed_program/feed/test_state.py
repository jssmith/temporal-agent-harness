# ABOUTME: The Playpen feed's state builder: decision rows and their deciders, the call lanes on the
# system map, the approvals inbox, worker gaps in the history, and the run's status line.

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from examples.governed_program.feed.state import (
    approvals,
    build_state,
    call_lanes,
    decision_rows,
    history_items,
    run_status,
)

T0 = datetime(2026, 11, 11, 19, 4, 0, tzinfo=UTC)
WID = "wf-1"


def at(seconds: float) -> str:
    return (T0 + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def record(seq: int, verdict: str, decided_by: str, **kw) -> dict:
    return {
        "seq": seq,
        "time": at(seq),
        "namespace": "default",
        "workflow_id": WID,
        "run_id": "r1",
        "kind": "activity",
        "name": "run_query",
        "target": "risk.loan_applications",
        "args_summary": "",
        "verdict": verdict,
        "decided_by": decided_by,
        "rule": "risk-tables-need-approval",
        "message": "",
        "policy_hash": "7c41e9a2d35b0c8e",
        "decision_id": "",
    } | kw


ESCALATION = [
    record(1, "escalated", "verifier", decision_id="d1"),
    record(2, "escalated", "verifier", decision_id="d1"),
    record(3, "approved", "reviewer", decision_id="d1"),
    record(4, "allowed", "stored-decision", rule="", decision_id="d1"),
]


def test_held_retries_are_one_row_and_each_row_names_its_decider() -> None:
    rows = decision_rows(ESCALATION, [])
    assert [(r["verdict"], r["decided_by"]) for r in rows] == [
        ("ESCALATE", "Dogwood verifier"),
        ("APPROVED", "Risk reviewer"),
        ("ALLOW", "Proxy, not re-asked"),
    ]
    assert rows[0]["attempts"] == 2
    assert rows[2]["rule"] == "stored approval d1"
    assert rows[0]["policy_hash"] == "7c41e9a2"


def test_refusals_outside_the_verifier_are_labelled_as_such() -> None:
    rpc = record(5, "denied", "proxy-allowlist", kind="rpc", name="StartWorkflowExecution", target="", rule="")
    egress = {"time": at(4), "call": "POST https://drop.elsewhere.example/upload", "error": "ENETUNREACH"}
    rows = decision_rows([rpc], [egress])
    assert [(r["call"], r["rule"], r["verdict"], r["decided_by"], r["policy_hash"]) for r in rows] == [
        ("POST https://drop.elsewhere.example/upload", "network none", "NO ROUTE", "gVisor sandbox", "n/a"),
        ("rpc StartWorkflowExecution", "proxy RPC allowlist", "DENY", "Proxy", "n/a"),
    ]


def test_egress_refused_by_a_host_process_guest_is_labelled_simulated() -> None:
    egress = [{"time": at(1), "call": "connect x:443", "error": "ENETUNREACH"}]
    assert decision_rows([], egress, "gVisor (runsc release-20261102.0, rootless)")[0]["decided_by"] == "gVisor sandbox"
    assert decision_rows([], egress, "host process (gVisor on target)")[0]["decided_by"] == "Sandbox (simulated)"


def test_a_target_logged_as_the_task_queue_is_read_from_the_arguments() -> None:
    queue = "governed-program-tools"
    records = [
        record(1, "allowed", "verifier", target=queue, policy_hash="sha256:952437be505a",
               args_summary="sql=SELECT branch_id FROM analytics.branch_deposits WHERE q = 3"),
        record(2, "escalated", "verifier", target=queue, decision_id="d1",
               args_summary="sql=SELECT branch_id, applications, default_rate FROM risk.loan_app…"),
        record(3, "denied", "verifier", name="send_email", target=queue,
               args_summary="subject=Loans, to=external-audit@riskreview-partners.example, body=…"),
        record(4, "denied", "proxy-allowlist", kind="rpc", name="StartWorkflowExecution",
               target="governed-program-guest", args_summary="workflow_id=f-1, workflow_type=ForwardTable"),
    ]
    rows = decision_rows(records, [])
    assert [r["call"] for r in rows] == [
        "run_query analytics.branch_deposits",
        "run_query risk.loan_app…",
        "send_email external-audit@riskreview-partners.example",
        "rpc StartWorkflowExecution",
    ]
    assert rows[0]["policy_hash"] == "952437be"
    assert len(call_lanes(rows)) == 4


def test_the_proxys_subject_names_the_call_over_its_queue_and_arguments() -> None:
    records = [
        record(1, "allowed", "verifier", target="governed-program-tools", subject="analytics.branch_deposits, risk.x",
               args_summary="sql=SELECT * FROM analytics.branch_deposits JOIN risk.x"),
        record(2, "denied", "verifier", name="send_email", target="governed-program-tools",
               subject="a@bank.example, b@elsewhere.example", args_summary="to=…"),
    ]
    assert [r["call"] for r in decision_rows(records, [])] == [
        "run_query analytics.branch_deposits, risk.x",
        "send_email a@bank.example, b@elsewhere.example",
    ]


def test_a_lane_shows_where_the_calls_latest_verdict_stopped_it() -> None:
    def lanes_after(records, egress=()):
        return [(l["label"], l["kind"], l["verdict"]) for l in call_lanes(decision_rows(records, list(egress)))]

    assert lanes_after(ESCALATION[:2]) == [("run_query risk.loan_applications", "pending", "WAITING")]
    assert lanes_after(ESCALATION) == [("run_query risk.loan_applications", "allow", "APPROVED")]
    assert lanes_after([ESCALATION[0], record(3, "rejected", "reviewer", decision_id="d1")]) == [
        ("run_query risk.loan_applications", "deny-proxy", "DENIED")
    ]
    email = record(6, "denied", "verifier", name="send_email", target="a@elsewhere.example")
    assert lanes_after([email], [{"time": at(1), "call": "POST https://x"}]) == [
        ("POST https://x", "blocked-sandbox", "NO ROUTE"),
        ("send_email a@elsewhere.example", "deny-proxy", "DENY"),
    ]


def test_approvals_decode_parameters_and_join_rule_and_answer() -> None:
    decisions = [
        {
            "id": "d1",
            "state": "approved",
            "workflowId": WID,
            "runId": "r1",
            "calls": ["governed-program-tools/run_query"],
            "parameters": [[{"@type": "type.googleapis.com/google.protobuf.Value", "value": {"sql": "SELECT 1"}}]],
            "reason": "pending human review: Risk tables need a person's approval",
            "createdAt": at(1),
        },
        {
            "id": "d2",
            "state": "pending",
            "workflowId": WID,
            "runId": "r1",
            "calls": ["governed-program-tools/send_email"],
            # A Temporal payload: {"to": "x@bank.example", "subject": "Hi"} in base64.
            "parameters": [
                [{"metadata": {"encoding": "anNvbi9wbGFpbg=="}, "data": "eyJ0byI6ICJ4QGJhbmsuZXhhbXBsZSIsICJzdWJqZWN0IjogIkhpIn0="}]
            ],
            "reason": "Big email",
            "createdAt": at(9),
        },
    ]
    inbox = approvals(decisions, decision_rows(ESCALATION, []))
    assert [a["id"] for a in inbox] == ["d2", "d1"]
    assert inbox[0]["summary"] == "to x@bank.example: Hi"
    assert inbox[0]["resolved_by"] is None
    expected = {
        "summary": "SELECT 1",
        "reason": "Risk tables need a person's approval",
        "rule": "risk-tables-need-approval",
        "resolved_at": at(3),
        "resolved_by": "Risk reviewer",
    }
    assert {k: inbox[1][k] for k in expected} == expected


def ev(n: int, etype: str, seconds: float, identity: str = "") -> dict:
    return {"id": n, "type": etype, "time": at(seconds), "detail": identity, "identity": identity}


def test_a_killed_sandbox_shows_an_open_gap_then_a_closed_one_after_restart() -> None:
    events = [
        ev(1, "WorkflowTaskStarted", 0, "guest@sbx-1"),
        ev(2, "WorkflowTaskCompleted", 1),
        ev(3, "ActivityTaskCompleted", 6),  # a long pause that is not a missing worker
        ev(4, "WorkflowTaskScheduled", 11),
    ]
    killed = {"state": "killed", "name": "sbx-1", "stopped_at": at(10), "previous": []}
    items, gap = history_items({"status": "RUNNING", "events": events}, killed, T0 + timedelta(seconds=14))
    notes = [i for i in items if i["kind"] == "note"]
    assert len(notes) == 1 and notes[0]["text"].startswith("No worker since") and notes[0]["tone"] == "red"
    assert gap == {"since": at(10), "until": None, "open": True}

    events.append(ev(5, "WorkflowTaskStarted", 18, "guest@sbx-2"))
    resumed = {"state": "running", "name": "sbx-2", "previous": [{"name": "sbx-1", "stopped_at": at(10)}]}
    items, gap = history_items({"status": "RUNNING", "events": events}, resumed, T0 + timedelta(seconds=20))
    notes = [i for i in items if i["kind"] == "note"]
    assert [n["text"].endswith("no events lost") for n in notes] == [True]
    assert gap == {"since": at(10), "until": at(18), "open": False}
    assert items[-1]["tone"] == "green"  # a new worker picked the run up


def test_a_task_nobody_picks_up_is_an_open_gap_without_a_control_plane() -> None:
    events = [ev(1, "WorkflowTaskCompleted", 0), ev(2, "WorkflowTaskScheduled", 1)]
    _, gap = history_items({"status": "RUNNING", "events": events}, None, T0 + timedelta(seconds=30))
    assert gap and gap["open"]


def test_a_task_held_for_a_person_is_not_a_missing_worker() -> None:
    events = [ev(1, "WorkflowTaskStarted", 0, "guest@sbx-1"), ev(2, "WorkflowTaskTimedOut", 10)]
    items, gap = history_items({"status": "RUNNING", "events": events}, None, T0 + timedelta(seconds=30), held=1)
    assert gap is None
    assert items[-1] | {"time": None} == {
        "kind": "note",
        "text": "Workflow task held at the proxy; guest told to retry",
        "tone": "yellow",
        "time": None,
    }


def test_status_line() -> None:
    running = {"state": "running", "name": "sbx-1", "restarted": False}
    assert run_status({"phase": "drafting"}, None, {"state": "none"}, 0, [])["label"] == "Drafting"
    assert run_status({"phase": "running"}, {"status": "RUNNING"}, running, 1, [])["label"] == "Waiting for approval"
    blocked = [{"kind": "deny-proxy"}, {"kind": "blocked-sandbox"}, {"kind": "allow"}]
    assert run_status({}, {"status": "RUNNING"}, running, 0, blocked)["label"] == "Running · 2 calls blocked"
    assert run_status({}, {"status": "RUNNING"}, {"state": "killed"}, 0, [])["tone"] == "red"
    restarted = {"state": "running", "name": "sbx-2", "restarted": True}
    assert run_status({}, {"status": "RUNNING"}, restarted, 0, [])["label"] == "Running on sbx-2"
    assert run_status({}, {"status": "COMPLETED"}, running, 0, [])["label"] == "Completed"


def test_build_state_keeps_only_this_workflow_and_links_to_temporal() -> None:
    other = record(9, "allowed", "verifier", workflow_id="someone-else")
    state = build_state(
        WID,
        audit=[*ESCALATION, other],
        decisions=[],
        history={"status": "RUNNING", "run_id": "r1", "events": []},
        control={"focus_since": at(3), "program": {"source": "a\nb\nc\n", "steps": [
            {"name": "s", "first_line": 2, "last_line": 3, "injected": True}
        ], "current_step": "s"}},
        now=T0 + timedelta(seconds=5),
    )
    assert len(state["decisions"]) == 3
    assert [r["fresh"] for r in state["decisions"]] == [False, True, True]
    assert state["history"]["temporal_ui_url"] == "http://localhost:8233/namespaces/default/workflows/wf-1/r1/history"
    assert [(l["step"], l["injected"]) for l in state["program"]["lines"]] == [(None, False), ("s", True), ("s", True)]
    assert state["policy"]["hash"] == "7c41e9a2"
    # The feed adds the boundary's verdicts to the conversation.
    assert any(m["text"].startswith("run_query risk.loan_applications is waiting") for m in state["conversation"])
