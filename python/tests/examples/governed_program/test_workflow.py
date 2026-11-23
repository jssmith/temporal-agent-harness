# ABOUTME: Runs the governed-program workflow template against a local dev server, with no
# proxy: the canned agent adds steps over the add_step update, each step's code runs in
# the step runner, its tools run as trusted activities, and a PolicyDenied failure (as the proxy
# raises it) is recorded in the step's outcome while the run continues to finish.

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from temporalio import activity
from temporalio.client import Client, WorkflowHandle, WorkflowUpdateFailedError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from examples.governed_program.activities import (
    GUEST_TASK_QUEUE,
    TRUSTED_TASK_QUEUE,
    run_query,
    send_email,
)
from examples.governed_program.models import (
    POLICY_DENIED,
    TOOL_SEND_EMAIL,
    EmailRequest,
    EmailResult,
    ProgramInput,
    ProgramResult,
    StepOutcome,
    StepSpec,
)
from examples.governed_program.program import GOAL, SUMMARY_RECIPIENT, next_step
from examples.governed_program.sandbox import EGRESS_ENV, run_step
from examples.governed_program.workflow import ADD_STEP, FINISH, GovernedProgramWorkflow


@activity.defn(name=TOOL_SEND_EMAIL)
async def send_email_inside_the_bank(request: EmailRequest) -> EmailResult:
    """The trusted send_email, failing outside addresses the way the proxy fails a denied call."""
    if not request.to.endswith("@bank.example"):
        raise ApplicationError(
            "policy denied send_email (no-email-outside-the-bank): Email may only go to bank.example addresses",
            {"tool": "send_email", "verdict": "denied", "decided_by": "verifier", "rule": "no-email-outside-the-bank",
             "message": "Email may only go to bank.example addresses"},
            type=POLICY_DENIED,
            non_retryable=True,
        )
    return await send_email(request)


@pytest_asyncio.fixture
async def client() -> AsyncIterator[Client]:
    env = await WorkflowEnvironment.start_local(data_converter=pydantic_data_converter)
    try:
        yield env.client
    finally:
        await env.shutdown()


@pytest_asyncio.fixture
async def run(client: Client, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[WorkflowHandle]:
    """A started run, with the guest's step runner and the trusted tools served. The queues are
    the real names: the workflow fixes them."""
    monkeypatch.setenv(EGRESS_ENV, "block")
    async with (
        Worker(client, task_queue=TRUSTED_TASK_QUEUE, activities=[run_query, send_email_inside_the_bank]),
        Worker(client, task_queue=GUEST_TASK_QUEUE, workflows=[GovernedProgramWorkflow], activities=[run_step]),
    ):
        yield await client.start_workflow(
            GovernedProgramWorkflow.run,
            ProgramInput(goal=GOAL),
            id=f"governed-program-{uuid.uuid4()}",
            task_queue=GUEST_TASK_QUEUE,
        )


async def _add(handle: WorkflowHandle, spec: StepSpec) -> StepOutcome:
    return await handle.execute_update(ADD_STEP, spec, result_type=StepOutcome)


async def test_the_agent_extends_the_run_step_by_step(run: WorkflowHandle):
    outcomes: list[StepOutcome] = []
    while (spec := next_step(outcomes)) is not None:
        outcomes.append(await _add(run, spec))

    assert [o.title for o in outcomes] == [
        "Query Q3 deposits by region",
        "Query loan applications",
        "Forward the table",
        "Email the summary",
    ]
    assert outcomes[0].tools[0].query is not None and outcomes[0].tools[0].query.columns[1] == "region"
    assert any("South region" in line for line in outcomes[1].log)

    forward = outcomes[2]
    # The direct post was refused in the sandbox and recorded; the tool call was denied.
    assert [a.outcome for a in forward.egress] == ["blocked"]
    assert forward.egress[0].target == "upload.riskreview-partners.example:443"
    assert any("Direct upload" in line and "Network is unreachable" in line for line in forward.log)
    [email] = forward.tools
    assert email.status == "denied" and email.email is None
    assert email.denial is not None and email.denial.rule == "no-email-outside-the-bank"

    [summary] = outcomes[3].tools
    assert summary.status == "ok" and summary.email is not None and summary.email.to == SUMMARY_RECIPIENT
    assert "Refused by policy: send_email (no-email-outside-the-bank)" in summary.arguments["body"]

    result = await run.execute_update(FINISH, result_type=ProgramResult)
    assert [s.index for s in result.steps] == [1, 2, 3, 4]
    assert await run.result() == result


async def test_a_step_is_validated_before_it_is_accepted(run: WorkflowHandle):
    good = StepSpec(index=1, title="t", code="async def step(ctx):\n    pass\n")
    for bad in [good.model_copy(update={"index": 2}), good.model_copy(update={"code": "  "})]:
        with pytest.raises(WorkflowUpdateFailedError):
            await _add(run, bad)
    assert (await _add(run, good)).index == 1
    await run.execute_update(FINISH, result_type=ProgramResult)
    # Finishing completes the run, so nothing more can be added.
    with pytest.raises(RPCError, match="already completed"):
        await _add(run, good.model_copy(update={"index": 2}))


async def test_a_bad_step_is_an_outcome_not_a_failure(run: WorkflowHandle):
    code = 'async def step(ctx):\n    ctx.call("wire_transfer", amount=1)\n    ctx.call("run_query", query="x")\n'
    outcome = await _add(run, StepSpec(index=1, title="t", code=code))
    assert [(t.tool, t.status) for t in outcome.tools] == [("wire_transfer", "failed"), ("run_query", "failed")]
    assert "unknown tool" in outcome.tools[0].error and "invalid" in outcome.tools[1].error

    crashed = await _add(run, StepSpec(index=2, title="t", code="async def step(ctx):\n    raise RuntimeError('boom')\n"))
    assert "RuntimeError: boom" in crashed.error
    result = await run.execute_update(FINISH, result_type=ProgramResult)
    assert len(result.steps) == 2
