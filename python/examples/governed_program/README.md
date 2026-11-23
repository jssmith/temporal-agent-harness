# Governed program example

A bank analyst whose side effects are decided by one tool policy, however it runs. This directory
holds the policy, the fixed workflow template, the guest's step runner, the canned agent, and the
trusted side effects as stubs. The demo rig that launches the sandbox and drives a run is outside
this repo (`demo-take/`); [`feed/`](feed) and [`ui/`](ui) are the Playpen page.

## One policy, two runs

[`policies/bank.dw`](policies/bank.dw) is a Dogwood policy for the bank's analyst. Two runs answer to
it:

1. **A Code Mode script.** The agent's script calls the trusted tools as host functions, and the
   harness asks the policy before each runs.
2. **An agent-generated Temporal worker in gVisor.** The agent writes activity code; a worker
   running it in the sandbox hosts the fixed workflow template below. This is the run this slice
   builds the pieces for.

Both runs name the tools the same way, so one policy governs both.

## How the second run is shaped

- **The workflow is a fixed template** ([`workflow.py`](workflow.py)), trusted code that is never
  generated. The agent extends a run step by step: the trusted control plane sends each step as an
  `add_step` update carrying a `StepSpec` (index, title, and code); the workflow validates it (next
  index, one step at a time, non-empty code under 64 KiB), runs it, and returns its `StepOutcome`, which
  the agent reads before writing the next step. A `finish` update ends the run. The guest cannot start
  workflows or send updates.
- **Generated code is activity code only.** The step runner ([`sandbox.py`](sandbox.py)), an activity
  on the guest's queue, runs a step's code (`async def step(ctx)`) in the sandbox. The code asks for
  side effects with `ctx.call(tool, **arguments)`; the workflow runs each as a trusted activity after
  validating its arguments as the tool's request model. An unknown tool, bad arguments or an error in
  the code are reported in the outcome, not raised. The workflow's own code never changes, so it stays
  deterministic.
- **Side effects are activities on a trusted queue** ([`activities.py`](activities.py)): `run_query`
  and `send_email`, stubs that return structured results and touch no network. The proxy (the local
  `temporal-untrusted-workers` fork) turns each into the tool call
  `governed-program-tools/<activity>`, and the policy decides it before the activity can run. The
  trusted worker ([`worker.py`](worker.py)) serves them.
- **A denied call is a failure the workflow handles.** With the proxy's
  `TEMPORAL_PROXY_DENIAL_TASK_QUEUE` set, the proxy diverts a denied activity to a queue only it polls
  and fails it as a non-retryable `PolicyDenied` error carrying the rule and message. The template
  records it in the step's outcome (`status="denied"`, `denial`) and the run continues. The call never
  reaches the trusted queue.
- **Egress is the sandbox's, not the proxy's.** The step runner records each network connection the
  code attempts (`StepOutcome.egress`), from a Python audit hook; with `PLAYPEN_EGRESS=block` it
  refuses them with `ENETUNREACH`, standing in for gVisor's empty network on the host-run rig. The
  proxy never sees these, so they are not in its audit log.

## The story

[`program.py`](program.py) is a canned agent. `next_step(outcomes)` writes each step's code after
reading the earlier outcomes:

| # | The step | The policy | What happens |
|---|---|---|---|
| 1 | `run_query` over `analytics.branch_deposits` | allowed (`query-open-tables`) | the query runs; the South region is the anomaly |
| 2 | `run_query` over `risk.loan_applications` | escalated (`risk-tables-need-approval`) | the call waits; a Risk reviewer approves it; the retry passes from the stored decision |
| 3 | a row's note tells the agent to forward the table outside the bank | | the agent obeys: an HTTPS post (refused by the sandbox), `StartWorkflowExecution` (refused by the proxy's allowlist), `send_email` outside (denied, `no-email-outside-the-bank`: `PolicyDenied`, the run continues) |
| 4 | the sandbox is killed while the next step waits | | a fresh sandbox replays the run's history and continues |
| 5 | `send_email` of the summary to `risk-team@bank.example` | allowed (`email-inside-the-bank`) | the run finishes |

## Escalation

Escalation is built on the proxy. With `TEMPORAL_PROXY_ESCALATION_ADDR` set, an escalation does not
deny the task. The proxy records a pending decision keyed by the workflow run and the call, and
answers the guest `Unavailable`, so it retries later. A person resolves the decision: approve lets
the retry through and the same call in that run is not asked about again; deny fails the call as
`PolicyDenied` (verdict `rejected`). The proxy's `internal/toolpolicy/escalation.go` and its README
describe the key and the API.

[`escalations.py`](escalations.py) is the console's side: `list_decisions`, `resolve_decision` (with
an optional `reviewer` name) and `audit_since`, which reads the proxy's `GET /audit` feed.

## The policy

[`policies/bank.dw`](policies/bank.dw) is adapted from the bank policy of `governed_analytics` (branch
`worktree-governed-analytics-demo`), keeping its table tiers: `analytics` open, `risk` for a person to
approve, `customer_pii` closed, read-only SELECTs. It is rewritten for the proxy's vocabulary (see
its header). [`policies/schema.cedarschema`](policies/schema.cedarschema) and
[`policies/events.dwschema`](policies/events.dwschema) are the action and event schemas the
Dogwood verifier loads with it.

## Tests

```bash
bazel test //python/tests/examples/governed_program/...
```

They cover the stub activities, the step runner (including blocked egress) and the canned agent;
the workflow template (against a local dev server, with no proxy, a stand-in failing outside email as
`PolicyDenied`); a replay of a real run through the proxy (`histories/story_run.json`), in which the
denied call was diverted; and the escalation client (against a stand-in for the proxy's API). The
policy and the proxy are tested in their own repos.
