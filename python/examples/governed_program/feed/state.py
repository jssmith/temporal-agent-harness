"""Turns the feed's raw sources into the one document the Playpen page renders.

Everything here is pure: the server gathers the sources and calls :func:`build_state`. The
sources and their shapes:

- ``audit``: the proxy's audit records for this workflow, in ``seq`` order (``GET /audit``).
- ``decisions``: the proxy's escalation decisions for this workflow (``GET /decisions``).
- ``history``: ``{"status", "run_id", "events": [{"id", "type", "time", "detail", "identity"}]}``
  from Temporal (see ``sources.history_event_to_dict``), or None before the workflow exists.
- ``control``: the control plane's session document (see ``README.md``), or None.

Not every refusal is the verifier's: blocked egress is the sandbox's (it never reaches the
proxy), and a refused RPC is the proxy's allowlist. Each row names its decider.
"""

from __future__ import annotations

import base64
import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

REVIEWER_LABEL = "Risk reviewer"

DECIDER_LABELS = {
    "verifier": "Dogwood verifier",
    "proxy-allowlist": "Proxy",
    "reviewer": REVIEWER_LABEL,
    "stored-decision": "Proxy, not re-asked",
}

VERDICT_LABELS = {
    "allowed": ("ALLOW", "green"),
    "denied": ("DENY", "red"),
    "escalated": ("ESCALATE", "yellow"),
    "approved": ("APPROVED", "green"),
    "rejected": ("DENIED", "red"),
}

# A pause between history events longer than this, ending in a worker picking up a task, is shown
# as a gap: the run had no worker for that long and lost nothing.
GAP_SECONDS = 4.0
# Without a control plane to say when the current beat began, rows this recent are emphasized.
FRESH_FALLBACK = timedelta(seconds=15)

PRE_RUN_PHASES = {
    "drafting": "Drafting",
    "program_ready": "Program ready",
    "launching": "Starting sandbox",
}


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def short_hash(value: str | None) -> str:
    return value.removeprefix("sha256:")[:8] if value else "n/a"


_SQL_TABLE = re.compile(r"\bFROM\s+([\w.]+)(…?)", re.IGNORECASE)
_RECIPIENT = re.compile(r"\bto=([^,\s]+)")
_WORKFLOW_TYPE = re.compile(r"\bworkflow_type=([^,\s]+)")


def call_target(rec: dict) -> str:
    """What the call is about: the table, the recipient, the workflow type. The proxy logs it as
    the subject; a proxy without one logs only the task queue as the target, and then it is read
    from the call's argument summary."""
    if rec.get("subject"):
        return rec["subject"]
    target = rec.get("target") or ""
    if "." in target or "@" in target:
        return target
    summary = rec.get("args_summary") or ""
    if rec.get("kind") == "rpc":
        m = _WORKFLOW_TYPE.search(summary)
        return m.group(1) if m else ""
    m = _SQL_TABLE.search(summary)
    if m:
        return m.group(1) + m.group(2)
    m = _RECIPIENT.search(summary)
    if m:
        return m.group(1)
    return target


def call_label(kind: str, name: str, target: str) -> str:
    if kind == "rpc":
        return f"rpc {name}"
    prefix = "nexus " if kind == "nexus" else ""
    return f"{prefix}{name} {target}".strip()


# ---------------------------------------------------------------- decisions and calls


def egress_decider(runtime: str) -> str:
    """Who refused a connection: the gVisor sandbox's kernel, or, for a guest run as a host
    process, the step runner standing in for one."""
    if re.match(r"gvisor|runsc", runtime or "", re.I):
        return "gVisor sandbox"
    return "Sandbox (simulated)"


def decision_rows(audit: list[dict], egress: list[dict], sandbox_runtime: str = "gVisor") -> list[dict]:
    """One row per verdict, oldest first. A held call that the guest retries while it waits is
    logged as escalated each time; it is one row here. Egress rows name the sandbox that stopped
    them by its runtime."""
    rows: list[dict] = []
    seen_escalations: dict[str, dict] = {}
    for rec in audit:
        verdict = rec.get("verdict", "")
        decision_id = rec.get("decision_id") or ""
        if verdict == "escalated" and decision_id:
            if decision_id in seen_escalations:
                seen_escalations[decision_id]["attempts"] += 1
                continue
        kind = rec.get("kind", "activity")
        name = rec.get("name", "")
        target = call_target(rec)
        decided_by = rec.get("decided_by", "")
        label, tone = VERDICT_LABELS.get(verdict, (verdict.upper() or "?", "neutral"))
        rule = rec.get("rule") or ""
        if not rule and decided_by == "stored-decision":
            rule = f"stored approval {short_hash(decision_id)}"
        if not rule and decided_by == "proxy-allowlist":
            rule = "proxy RPC allowlist"
        # The RPC allowlist is the proxy's own, not the policy's, so no policy hash applies.
        hash_label = "n/a" if decided_by == "proxy-allowlist" else short_hash(rec.get("policy_hash"))
        row = {
            "id": f"audit-{rec.get('seq')}",
            "seq": rec.get("seq"),
            "time": rec.get("time"),
            "source": "proxy",
            "kind": kind,
            "name": name,
            "target": target,
            "call": call_label(kind, name, target),
            "args_summary": rec.get("args_summary", ""),
            "rule": rule or "-",
            "verdict": label,
            "verdict_raw": verdict,
            "tone": tone,
            "decided_by": DECIDER_LABELS.get(decided_by, decided_by or "-"),
            "decided_by_raw": decided_by,
            "policy_hash": hash_label,
            "decision_id": decision_id,
            "message": rec.get("message", ""),
            "attempts": 1,
        }
        if verdict == "escalated" and decision_id:
            seen_escalations[decision_id] = row
        rows.append(row)
    for i, attempt in enumerate(egress):
        target = attempt.get("target", "")
        rows.append(
            {
                "id": f"egress-{i}",
                "seq": None,
                "time": attempt.get("time"),
                "source": "sandbox",
                "kind": "egress",
                "name": "egress",
                "target": target,
                "call": attempt.get("call") or f"connect {target}",
                "args_summary": "",
                "rule": "network none",
                "verdict": "NO ROUTE",
                "verdict_raw": "blocked",
                "tone": "red",
                "decided_by": egress_decider(sandbox_runtime),
                "decided_by_raw": "sandbox",
                "policy_hash": "n/a",
                "decision_id": "",
                "message": attempt.get("error", ""),
                "attempts": 1,
            }
        )
    rows.sort(key=lambda r: (parse_time(r["time"]) or datetime.min.replace(tzinfo=UTC)))
    return rows


def call_lanes(rows: list[dict]) -> list[dict]:
    """One lane per distinct call leaving the sandbox, in first-seen order, showing where its
    latest verdict stopped it: reached the trusted side, stopped at the proxy, parked at the
    proxy for a person, or stopped at the sandbox wall."""
    lanes: dict[str, dict] = {}
    for row in rows:
        key = f"{row['kind']}|{row['name']}|{row['target']}"
        verdict = row["verdict_raw"]
        lane = lanes.get(key)
        if lane is None:
            lane = lanes[key] = {"id": key, "label": row["call"], "first_time": row["time"]}
        if verdict == "blocked":
            lane.update(kind="blocked-sandbox", verdict="NO ROUTE")
        elif verdict == "escalated":
            lane.update(kind="pending", verdict="WAITING")
        elif verdict == "approved":
            lane.update(kind="allow", verdict="APPROVED")
        elif verdict == "allowed":
            stored = row["decided_by_raw"] == "stored-decision" or lane.get("verdict") == "APPROVED"
            lane.update(kind="allow", verdict="APPROVED" if stored else "ALLOW")
        elif verdict == "rejected":
            lane.update(kind="deny-proxy", verdict="DENIED")
        else:
            lane.update(kind="deny-proxy", verdict="DENY")
        lane["time"] = row["time"]
        lane["decided_by"] = row["decided_by"]
    return list(lanes.values())


# ---------------------------------------------------------------- approvals


def _decode_parameter(value: Any) -> Any:
    """The proxy lists parameters in protobuf JSON form: a ``google.protobuf.Value`` wrapper or
    a Temporal payload with base64 data. Unwrap either to the plain argument."""
    if isinstance(value, dict):
        if "value" in value and "@type" in value:
            return value["value"]
        if "data" in value and isinstance(value.get("metadata"), dict):
            try:
                return json.loads(base64.b64decode(value["data"]))
            except (ValueError, TypeError):
                return value["data"]
    return value


def _summarize_argument(arg: Any) -> str:
    if isinstance(arg, dict):
        if "sql" in arg:
            return str(arg["sql"])
        if "to" in arg:
            subject = arg.get("subject")
            return f"to {arg['to']}" + (f": {subject}" if subject else "")
    return json.dumps(arg, separators=(",", ": ")) if not isinstance(arg, str) else arg


def approvals(decisions: list[dict], rows: list[dict]) -> list[dict]:
    """Every escalation for this run, pending ones first, with the rule that escalated it and
    when and by whom it was answered (from the audit rows)."""
    by_decision: dict[str, dict[str, dict]] = {}
    for row in rows:
        if row["decision_id"]:
            by_decision.setdefault(row["decision_id"], {}).setdefault(row["verdict_raw"], row)
    out = []
    for d in decisions:
        related = by_decision.get(d["id"], {})
        escalated = related.get("escalated")
        answer = related.get("approved") or related.get("rejected")
        params = [[_decode_parameter(p) for p in call] for call in d.get("parameters") or []]
        summaries = [_summarize_argument(call[0]) for call in params if call]
        reason = d.get("reason", "")
        reason = reason.removeprefix("pending human review: ")
        out.append(
            {
                "id": d["id"],
                "short_id": short_hash(d["id"]),
                "state": d.get("state", "pending"),
                "calls": d.get("calls", []),
                "call": (d.get("calls") or [""])[0],
                "summary": summaries[0] if summaries else "",
                "parameters": params,
                "reason": reason,
                "rule": d.get("rule") or (escalated["rule"] if escalated else ""),
                "workflow_id": d.get("workflowId", ""),
                "run_id": d.get("runId", ""),
                "created_at": d.get("createdAt"),
                "resolution": d.get("resolution", ""),
                "resolved_at": answer["time"] if answer else None,
                "resolved_by": REVIEWER_LABEL if d.get("state") != "pending" else None,
            }
        )
    out.sort(key=lambda a: (a["state"] != "pending", a["created_at"] or ""))
    return out


# ---------------------------------------------------------------- history


def _clock(t: datetime) -> str:
    return t.astimezone().strftime("%H:%M:%S")


def history_items(
    history: dict | None,
    sandbox: dict | None,
    now: datetime,
    gap_seconds: float = GAP_SECONDS,
    held: int = 0,
) -> tuple[list[dict], dict | None]:
    """History events in order, with notes where the run had no worker: a gap that ended (no
    events lost), or one still open. While a call is held for a person the stalled task is the
    proxy's doing, not a missing worker. Returns the items and the open or latest gap."""
    if not history:
        return [], None
    events = history.get("events") or []
    items: list[dict] = []
    gap: dict | None = None
    restarts = {
        p.get("stopped_at"): p for p in (sandbox or {}).get("previous", []) if p.get("stopped_at")
    }
    restart_times = sorted(t for t in (parse_time(s) for s in restarts) if t)
    last_identity: str | None = None
    prev: dict | None = None
    for n, ev in enumerate(events, start=1):
        t = parse_time(ev.get("time"))
        tone = ev.get("tone")
        etype = ev.get("type", "")
        if etype in ("WorkflowTaskTimedOut",):
            tone = tone or "yellow"
        elif etype in ("WorkflowTaskFailed", "ActivityTaskFailed", "ActivityTaskTimedOut"):
            tone = tone or "red"
        elif etype in ("WorkflowExecutionFailed", "WorkflowExecutionTerminated", "WorkflowExecutionTimedOut"):
            tone = tone or "red"
        elif etype == "WorkflowExecutionCompleted":
            tone = tone or "green"
        pt = parse_time(prev.get("time")) if prev else None
        if prev and t and pt and etype in ("WorkflowTaskStarted", "ActivityTaskStarted"):
            killed_between = [r for r in restart_times if r <= t]
            restart_times = [r for r in restart_times if r > t]
            waited = prev.get("type") in ("WorkflowTaskScheduled", "ActivityTaskScheduled")
            if (waited and (t - pt).total_seconds() > gap_seconds) or killed_between:
                start = killed_between[0] if killed_between else pt
                gap = {"since": iso(start), "until": iso(t), "open": False}
                items.append(
                    {
                        "kind": "note",
                        "text": f"Gap {_clock(start)} to {_clock(t)}, no events lost",
                        "tone": "neutral",
                        "time": iso(start),
                    }
                )
        identity = ev.get("identity") or ""
        if etype == "WorkflowTaskStarted" and identity:
            if last_identity and identity != last_identity and not tone:
                tone = "green"
            last_identity = identity
        items.append(
            {
                "kind": "event",
                "n": n,
                "id": ev.get("id", n),
                "type": etype,
                "time": ev.get("time"),
                "detail": ev.get("detail", ""),
                "tone": tone,
            }
        )
        prev = ev
    status = history.get("status")
    if sandbox and sandbox.get("state") == "killed":
        since = parse_time(sandbox.get("stopped_at")) or now
        gap = {"since": iso(since), "until": None, "open": True}
        items.append(
            {
                "kind": "note",
                "text": f"No worker since {_clock(since)} ({sandbox.get('name', 'sandbox')} killed)",
                "tone": "red",
                "time": iso(since),
            }
        )
    elif held and status == "RUNNING":
        items.append(
            {
                "kind": "note",
                "text": "Workflow task held at the proxy; guest told to retry",
                "tone": "yellow",
                "time": prev.get("time") if prev else None,
            }
        )
    elif status == "RUNNING" and prev and prev.get("type") in ("WorkflowTaskScheduled", "WorkflowTaskTimedOut"):
        since = parse_time(prev.get("time"))
        if since and (now - since).total_seconds() > gap_seconds:
            gap = {"since": iso(since), "until": None, "open": True}
            items.append(
                {"kind": "note", "text": f"No worker since {_clock(since)}", "tone": "red", "time": iso(since)}
            )
    return items, gap


# ---------------------------------------------------------------- program and conversation


def program_lines(program: dict | None) -> list[dict]:
    if not program:
        return []
    if program.get("lines"):
        return [
            {"n": i, "text": l.get("text", ""), "step": l.get("step"), "injected": bool(l.get("injected"))}
            for i, l in enumerate(program["lines"], start=1)
        ]
    steps = program.get("steps") or []
    out = []
    for i, text in enumerate((program.get("source") or "").splitlines(), start=1):
        step = next((s for s in steps if s.get("first_line", 0) <= i <= s.get("last_line", -1)), None)
        out.append(
            {
                "n": i,
                "text": text,
                "step": step.get("name") if step else None,
                "injected": bool(step and step.get("injected")),
            }
        )
    return out


def policy_messages(rows: list[dict]) -> list[dict]:
    """Conversation lines for what the boundary did, so the control plane only reports what it
    knows itself (the agent, the program, the sandbox lifecycle)."""
    out = []
    for row in rows:
        v = row["verdict_raw"]
        by = row["decided_by_raw"]
        if v == "blocked":
            text, tone = f"{row['call']} failed inside the sandbox: network unreachable.", "red"
        elif v == "escalated":
            text, tone = f"{row['call']} is waiting for approval ({row['rule']}).", "yellow"
        elif v == "approved":
            text, tone = f"Approved by {REVIEWER_LABEL}.", "green"
        elif v == "rejected":
            text, tone = f"Denied by {REVIEWER_LABEL}. The call does not run.", "red"
        elif v == "allowed" and by == "stored-decision":
            continue
        elif v == "allowed":
            text, tone = f"{row['call']} allowed by {row['rule']}.", "green"
        elif by == "proxy-allowlist":
            text, tone = f"{row['call']} is not permitted through this proxy.", "red"
        else:
            text, tone = f"{row['call']} denied by {row['rule']}.", "red"
        out.append({"role": "event", "text": text, "tone": tone, "time": row["time"], "origin": "feed"})
    return out


def conversation(control: dict | None, rows: list[dict]) -> list[dict]:
    own = [dict(m, origin="control") for m in (control or {}).get("conversation", [])]
    merged = own + policy_messages(rows)
    floor = datetime.min.replace(tzinfo=UTC)
    # Stable: a control-plane line and a feed line at the same instant keep that order.
    return sorted(merged, key=lambda m: parse_time(m.get("time")) or floor)


# ---------------------------------------------------------------- sandbox, status, map


def _duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    return f"{seconds // 60}:{seconds % 60:02d}"


def sandbox_view(sandbox: dict | None, now: datetime) -> dict:
    if not sandbox or sandbox.get("state") in (None, "none"):
        return {"state": "none"}
    started = parse_time(sandbox.get("started_at"))
    stopped = parse_time(sandbox.get("stopped_at"))
    end = stopped if sandbox.get("state") == "killed" and stopped else now
    egress = sandbox.get("egress") or []
    previous = []
    for p in sandbox.get("previous") or []:
        ps, pe = parse_time(p.get("started_at")), parse_time(p.get("stopped_at"))
        previous.append(
            {
                "name": p.get("name", ""),
                "stopped_at": p.get("stopped_at"),
                "uptime": _duration((pe - ps).total_seconds()) if ps and pe else "",
                "reason": p.get("reason", ""),
            }
        )
    return {
        "state": sandbox.get("state"),
        "name": sandbox.get("name", ""),
        "runtime": sandbox.get("runtime", ""),
        "kernel": sandbox.get("kernel", ""),
        "network": sandbox.get("network", ""),
        "socket": sandbox.get("socket", ""),
        "key_scope": sandbox.get("key_scope", ""),
        "started_at": sandbox.get("started_at"),
        "stopped_at": sandbox.get("stopped_at"),
        "uptime": _duration((end - started).total_seconds()) if started else "",
        "egress_attempts": len(egress),
        "egress_last_error": egress[-1].get("error", "") if egress else "",
        "previous": previous,
        "restarted": bool(previous),
    }


def run_status(
    control: dict | None, history: dict | None, sandbox: dict, pending: int, lanes: list[dict]
) -> dict:
    phase = (control or {}).get("phase")
    wf = (history or {}).get("status")
    blocked = sum(1 for l in lanes if l.get("kind") in ("deny-proxy", "blocked-sandbox"))
    if sandbox.get("state") == "killed":
        return {"label": "Worker lost · resuming", "tone": "red"}
    if wf == "COMPLETED":
        return {"label": "Completed", "tone": "green"}
    if wf in ("FAILED", "TERMINATED", "TIMED_OUT", "CANCELED"):
        return {"label": wf.replace("_", " ").capitalize(), "tone": "red"}
    if pending:
        return {"label": "Waiting for approval", "tone": "yellow"}
    if phase in PRE_RUN_PHASES:
        return {"label": PRE_RUN_PHASES[phase], "tone": "neutral"}
    if wf == "RUNNING" or sandbox.get("state") == "running":
        if sandbox.get("restarted"):
            return {"label": f"Running on {sandbox.get('name')}", "tone": "green"}
        if blocked:
            return {"label": f"Running · {blocked} call{'s' if blocked != 1 else ''} blocked", "tone": "green"}
        return {"label": "Running", "tone": "green"}
    return {"label": "Drafting", "tone": "neutral"}


def active_edges(sandbox: dict, rows: list[dict], fresh_since: datetime, phase: str | None) -> list[str]:
    """Which links on the system map carry the current beat's traffic."""
    state = sandbox.get("state")
    if state in (None, "none", "killed"):
        return []
    started = parse_time(sandbox.get("started_at"))
    fresh = [r for r in rows if (parse_time(r["time"]) or fresh_since) >= fresh_since]
    if not fresh:
        if phase == "launching" or (started and started >= fresh_since):
            if sandbox.get("restarted"):
                return ["launch", "g2p", "p2t"]
            return ["launch", "pg", "g2p", "p2t"]
        return []
    last = fresh[-1]
    v, by = last["verdict_raw"], last["decided_by_raw"]
    if v == "blocked":
        return ["pg"]
    if v == "escalated":
        return ["g2p", "p2v", "v2pol"]
    if v in ("approved",) or by == "stored-decision":
        return ["g2p", "p2v", "v2pol", "p2t", "t2w"]
    if v == "allowed":
        return ["pg", "g2p", "p2v", "v2pol", "p2t", "t2w"]
    if by == "proxy-allowlist":
        return ["pg", "g2p"]
    return ["pg", "g2p", "p2v", "v2pol"]


# ---------------------------------------------------------------- the document


def build_state(
    workflow_id: str,
    *,
    audit: list[dict],
    decisions: list[dict],
    history: dict | None,
    control: dict | None,
    now: datetime | None = None,
    temporal_ui: str = "http://localhost:8233",
    namespace: str = "default",
    source_errors: dict[str, str] | None = None,
) -> dict:
    now = now or datetime.now(UTC)
    control = control or {}
    raw_sandbox = control.get("sandbox")
    sandbox = sandbox_view(raw_sandbox, now)
    rows = decision_rows(
        [r for r in audit if r.get("workflow_id") == workflow_id],
        (raw_sandbox or {}).get("egress") or [],
        (raw_sandbox or {}).get("runtime") or "gVisor",
    )
    lanes = call_lanes(rows)
    mine = [d for d in decisions if d.get("workflowId") == workflow_id]
    inbox = approvals(mine, rows)
    pending = sum(1 for a in inbox if a["state"] == "pending")
    focus_since = parse_time(control.get("focus_since")) or (now - FRESH_FALLBACK)

    def fresh(t: Any) -> bool:
        pt = parse_time(t)
        return bool(pt and pt >= focus_since)

    for row in rows:
        row["fresh"] = fresh(row["time"])
    for lane in lanes:
        lane["fresh"] = fresh(lane["time"])
    messages = conversation(control, rows)
    for m in messages:
        m["fresh"] = fresh(m.get("time"))
    items, gap = history_items(history, raw_sandbox, now, held=pending)
    for item in items:
        item["fresh"] = fresh(item.get("time"))

    run_id = (history or {}).get("run_id") or control.get("run_id") or ""
    program = control.get("program") or {}
    policy = control.get("policy") or {}
    policy_hash = policy.get("hash") or next((r["policy_hash"] for r in reversed(rows) if r["policy_hash"] != "n/a"), "")
    link = (
        f"{temporal_ui.rstrip('/')}/namespaces/{namespace}/workflows/{workflow_id}/{run_id}/history" if run_id else None
    )
    return {
        "workflow_id": workflow_id,
        "run_id": run_id,
        "session_id": control.get("session_id", ""),
        "agent": control.get("agent") or {"name": "bank analyst"},
        "started": history is not None,
        "workflow_status": (history or {}).get("status"),
        "phase": control.get("phase"),
        "focus_since": iso(focus_since),
        "now": iso(now),
        "policy": {
            "name": policy.get("name", "bank.dw"),
            "hash": short_hash(policy_hash) if policy_hash else "",
            "rules": policy.get("rules"),
        },
        "status": run_status(control, history, sandbox, pending, lanes),
        "conversation": messages,
        "program": {
            "filename": program.get("filename", ""),
            "activity": program.get("activity", ""),
            "workflow": program.get("workflow", "GovernedProgramWorkflow"),
            "current_step": program.get("current_step"),
            "lines": program_lines(program),
        },
        "decisions": rows,
        "lanes": lanes,
        "held": pending,
        "map": {"active_edges": active_edges(sandbox, rows, focus_since, control.get("phase"))},
        "approvals": inbox,
        "sandbox": sandbox,
        "history": {
            "items": items,
            "event_count": sum(1 for i in items if i["kind"] == "event"),
            "gap": gap,
            "temporal_ui_url": link,
        },
        "sources": source_errors or {},
    }
