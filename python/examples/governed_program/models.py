"""The shapes that cross the trust boundary of the governed-program demo.

The trusted control plane sends the run one :class:`StepSpec` at a time (the workflow's
``add_step`` update), each carrying code the agent wrote. The guest's step runner
(``sandbox.py``) runs that code in the sandbox; it returns the side effects it wants as
:class:`ToolRequest`\\ s, and the fixed workflow template turns each into one trusted activity,
whose single argument is a :class:`QueryRequest` or an :class:`EmailRequest`. The tool policy
(``policies/bank.dw``) sees that argument as the call's ``arg0``, so its field names are what the
policy refers to. What the step did comes back as a :class:`StepOutcome`, which the agent reads
before it writes the next step.

No ``from __future__ import annotations`` here (stringized annotations trip the pydantic
converter).
"""

from typing import Any, Literal

from pydantic import BaseModel, Field

TOOL_RUN_QUERY = "run_query"
TOOL_SEND_EMAIL = "send_email"

# The ApplicationError type the proxy fails a denied tool call with.
POLICY_DENIED = "PolicyDenied"


class ProgramInput(BaseModel):
    """What the run is started with: the analysis the agent was asked to do."""

    goal: str


class StepSpec(BaseModel):
    """One step the agent adds to the run. ``code`` is Python source that defines
    ``async def step(ctx)``; it runs in the sandbox, never in the workflow."""

    index: int
    title: str
    code: str


class QueryRequest(BaseModel):
    sql: str


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list[Any]]


class EmailRequest(BaseModel):
    to: str
    subject: str
    body: str


class EmailResult(BaseModel):
    message_id: str
    to: str


class ToolRequest(BaseModel):
    """One side effect a step asks for. ``tool`` and ``arguments`` are checked by the workflow,
    not trusted as sent: ``arguments`` must validate as the tool's own request model."""

    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class EgressAttempt(BaseModel):
    """A network connection the step's code tried to make from the sandbox. The proxy never sees
    these: the sandbox's step runner records them itself."""

    target: str
    # "blocked": the sandbox refused it; "attempted": it was let through to the network.
    outcome: Literal["blocked", "attempted"]
    detail: str = ""


class StepPlan(BaseModel):
    """What the step's code returned from the sandbox: the side effects it asks for, its log,
    and the network connections it attempted."""

    requests: list[ToolRequest] = Field(default_factory=list)
    log: list[str] = Field(default_factory=list)
    egress: list[EgressAttempt] = Field(default_factory=list)
    sandbox_network: str = ""
    error: str = ""


class PolicyDenial(BaseModel):
    """Why the proxy refused a tool call: the details of its ``PolicyDenied`` failure."""

    tool: str = ""
    target: str = ""
    # "denied" by the policy, or "rejected" by a reviewer of an escalation.
    verdict: str = "denied"
    decided_by: str = ""
    rule: str = ""
    message: str = ""
    policy_hash: str = ""
    decision_id: str = ""


class ToolOutcome(BaseModel):
    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    status: Literal["ok", "denied", "failed"]
    query: QueryResult | None = None
    email: EmailResult | None = None
    denial: PolicyDenial | None = None
    error: str = ""


class StepOutcome(BaseModel):
    """What one step did, as the agent reads it."""

    index: int
    title: str
    log: list[str] = Field(default_factory=list)
    egress: list[EgressAttempt] = Field(default_factory=list)
    sandbox_network: str = ""
    error: str = ""
    tools: list[ToolOutcome] = Field(default_factory=list)


class StepInput(BaseModel):
    """The step runner's input: the step and what the earlier steps returned."""

    goal: str
    spec: StepSpec
    previous: list[StepOutcome] = Field(default_factory=list)


class ProgramResult(BaseModel):
    goal: str
    steps: list[StepOutcome]
