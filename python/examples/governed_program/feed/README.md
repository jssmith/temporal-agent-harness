# Playpen feed

A small service, run beside the trusted worker (not inside it), that joins what the Playpen page
shows about one guest run, and serves that page (`../ui`, plain ES modules, no build step).

| Source | Read with | Feeds |
| --- | --- | --- |
| Proxy audit feed, `GET /audit?since=<seq>` | `sources.AuditTail` | decision stream, call lanes on the map, verdict lines in the conversation |
| Proxy decisions, `GET /decisions`, `POST /decisions/{id}` | `sources.ProxyDecisions` | approvals inbox; Approve / Deny are forwarded |
| Temporal history (`TEMPORAL_ADDRESS`, namespace `default`) | `sources.TemporalHistory` | history strip, worker gaps, run status |
| Control plane, `GET /api/playpen/session/{wid}` | `sources.ControlPlane` | agent conversation, generated program, sandbox status |

`state.build_state` turns them into the one document the page renders. A source that is down
is named in the page's top bar; the rest still render.

## Run

Against the scripted run in `fixture.py` (a fake proxy, control plane and history), with nothing
else running, from `python/`:

```bash
bazel run //python/examples/governed_program/feed:server -- --fixture
# or: PYTHONPATH=. uv run python -m examples.governed_program.feed.server --fixture
```

Open <http://127.0.0.1:8095/> and move the run with `curl -X POST
127.0.0.1:8199/fixture/scene/<0-7>`; Approve and Deny in the page resolve the fixture's
escalation, `POST /api/playpen/sandbox/kill` kills its sandbox, and `POST /fixture/resume`
brings up the next one.

Live, beside the proxy, Temporal and the control plane:

```bash
bazel run //python/examples/governed_program/feed:server
```

Environment: `PLAYPEN_FEED_PORT` (8095), `PLAYPEN_PROXY_URL` (`http://127.0.0.1:8099`),
`PLAYPEN_CONTROL_PLANE_URL` (`http://127.0.0.1:8090`), `TEMPORAL_ADDRESS` (`127.0.0.1:7233`),
`TEMPORAL_NAMESPACE` (`default`), `PLAYPEN_TEMPORAL_UI` (`http://localhost:8233`). The page
takes `?wid=<workflow id>`, or follows the newest session the control plane lists.

## The control plane's contract

The control plane knows what nothing else does: the agent's conversation, the program it is
writing, and the sandbox it launched. It should serve these, on loopback only.

`GET /api/playpen/sessions` lists sessions, oldest first:
`[{"workflow_id", "session_id", "created_at"}]`. The workflow id is assigned when the session is
created, before the workflow starts.

`GET /api/playpen/session/{workflow_id}` (404 for an unknown id):

```json
{
  "session_id": "analyst-4e19",
  "workflow_id": "q3-deposits-7f3c21",
  "run_id": "4f1c…",
  "agent": {"name": "bank analyst"},
  "phase": "drafting | program_ready | launching | running | completed",
  "focus_since": "2026-11-11T19:04:52Z",
  "policy": {"name": "bank.dw", "hash": "7c41e9a2…", "rules": 7},
  "conversation": [
    {"time": "…Z", "role": "user | agent | event", "text": "…", "tone": "green | red | yellow", "quote": "…"}
  ],
  "program": {
    "filename": "plan_q3_deposits.py",
    "activity": "plan_q3_deposits",
    "workflow": "GovernedProgramWorkflow",
    "source": "<the whole generated module as it stands>",
    "steps": [
      {"name": "deposits", "first_line": 7, "last_line": 10, "injected": false, "added_at": "…Z"}
    ],
    "current_step": "deposits"
  },
  "sandbox": {
    "name": "sbx-2",
    "state": "none | starting | running | killed",
    "runtime": "gVisor (runsc)",
    "kernel": "4.19.0-gvisor",
    "network": "none",
    "socket": "/run/playpen/sbx-2/proxy.sock",
    "key_scope": "governed-program-guest",
    "started_at": "…Z",
    "stopped_at": "…Z (when killed)",
    "egress": [{"time": "…Z", "call": "POST https://drop.elsewhere.example/upload", "target": "drop.elsewhere.example", "error": "ENETUNREACH"}],
    "previous": [{"name": "sbx-1", "started_at": "…Z", "stopped_at": "…Z", "reason": "killed"}]
  }
}
```

- `conversation` holds only what the control plane knows: the request, the agent's messages,
  and lifecycle events (program generated, key minted, sandbox started, killed, restarted). The
  feed adds a line for each verdict from the audit feed, so do not repeat those. `quote` shows
  text the agent read (the injected row), set apart under the line.
- `program.steps` grow one at a time as the agent adds them mid-run; `injected` marks a step
  added after the agent read untrusted text (drawn with a red bar). `current_step` is the step
  the run is on; its lines are highlighted.
- `focus_since` is when the current beat began (a step added, the sandbox started or
  restarted, an approval answered). The page emphasizes anything newer and lights the map's links
  for it.
- `sandbox.egress` lists connection attempts the sandbox refused. They never reach the proxy,
  so the control plane (or the launcher's gVisor logs) is the only source.
- `kernel` is the guest's `uname -r`, which shows the gVisor kernel.

The recorder (`demo-take/recorder/record-playpen.mjs`, live mode) also uses:

- `POST /api/playpen/start` `{"request": "…"}` → `{"workflow_id": "…"}`: start a session.
- `POST /api/playpen/sandbox/kill` `{"workflow_id": "…"}`: kill the run's sandbox; the control
  plane starts a new one, which resumes the run from history.
