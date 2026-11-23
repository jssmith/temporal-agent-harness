"""``GovernedProgram``: the fixed workflow template that every guest run uses.

The workflow is trusted code and never generated. It runs on the guest's task queue, inside the
sandbox, because that is where the proxy's checks apply to its commands. The agent extends the
run one step at a time, through the trusted control plane (the guest cannot start workflows or
send updates):

  * ``add_step`` (update) carries a :class:`StepSpec`. The workflow validates it, runs its code
    with the step runner (``sandbox.py``, an activity on the guest's own queue), then runs each
    side effect the code asked for as a trusted activity (``activities.py``), in order, and
    returns the :class:`StepOutcome`, which the agent reads before writing the next step.
  * ``finish`` (update) ends the run with every step's outcome.

Generated code is only ever activity input, so the workflow's own code, and its determinism,
never change.

A trusted activity is on another task queue than the workflow's, so the proxy turns each one
into a tool call and the policy (``policies/bank.dw``) decides it before the activity can run.
Nothing here asks the policy. A denied call never reaches the trusted queue: the proxy fails it
as a non-retryable ``PolicyDenied`` activity failure, which the workflow records in the step's
outcome and moves on. An escalated call waits (the proxy answers ``Unavailable``) until a person
resolves it; a rejection then fails it as ``PolicyDenied`` too. See the README.

No ``from __future__ import annotations`` here (stringized annotations trip the pydantic
converter).
"""

from datetime import timedelta

from pydantic import ValidationError
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from examples.governed_program.activities import GUEST_TASK_QUEUE, TRUSTED_TASK_QUEUE
    from examples.governed_program.models import (
        POLICY_DENIED,
        TOOL_RUN_QUERY,
        TOOL_SEND_EMAIL,
        EmailRequest,
        EmailResult,
        PolicyDenial,
        ProgramInput,
        ProgramResult,
        QueryRequest,
        QueryResult,
        StepInput,
        StepOutcome,
        StepPlan,
        StepSpec,
        ToolOutcome,
        ToolRequest,
    )
    from examples.governed_program.sandbox import STEP_ACTIVITY

WORKFLOW_NAME = "GovernedProgram"
ADD_STEP = "add_step"
FINISH = "finish"
PROGRESS = "progress"
# Long enough for a person to answer an escalation; the proxy answers Unavailable meanwhile and
# the worker retries the workflow task, so this bounds only the activity itself.
TOOL_TIMEOUT = timedelta(seconds=30)
STEP_TIMEOUT = timedelta(minutes=1)
MAX_STEP_CODE = 64 * 1024
MAX_TOOL_CALLS_PER_STEP = 10


@workflow.defn(name=WORKFLOW_NAME)
class GovernedProgramWorkflow:
    @workflow.init
    def __init__(self, program: ProgramInput) -> None:
        self._goal = program.goal
        self._steps: list[StepOutcome] = []
        self._running = False
        self._finished = False

    @workflow.run
    async def run(self, program: ProgramInput) -> ProgramResult:
        await workflow.wait_condition(lambda: self._finished and workflow.all_handlers_finished())
        return self._result()

    @workflow.update(name=ADD_STEP)
    async def add_step(self, spec: StepSpec) -> StepOutcome:
        self._running = True
        try:
            plan = await workflow.execute_activity(
                STEP_ACTIVITY,
                StepInput(goal=self._goal, spec=spec, previous=self._steps),
                task_queue=GUEST_TASK_QUEUE,
                start_to_close_timeout=STEP_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=3),
                result_type=StepPlan,
                summary=f"step {spec.index}: {spec.title}",
            )
            outcome = StepOutcome(
                index=spec.index,
                title=spec.title,
                log=plan.log,
                egress=plan.egress,
                sandbox_network=plan.sandbox_network,
                error=plan.error,
            )
            if len(plan.requests) > MAX_TOOL_CALLS_PER_STEP:
                outcome.error = (
                    f"the step asked for {len(plan.requests)} tool calls; only the first "
                    f"{MAX_TOOL_CALLS_PER_STEP} ran"
                )
            for request in plan.requests[:MAX_TOOL_CALLS_PER_STEP]:
                outcome.tools.append(await self._run_tool(request))
            self._steps.append(outcome)
            return outcome
        finally:
            self._running = False

    @add_step.validator
    def _validate_step(self, spec: StepSpec) -> None:
        if self._finished:
            raise ValueError("the run is finished")
        if self._running:
            raise ValueError("a step is still running; add the next one when it returns")
        if spec.index != len(self._steps) + 1:
            raise ValueError(f"expected step {len(self._steps) + 1}, got step {spec.index}")
        if not spec.code.strip():
            raise ValueError("the step has no code")
        if len(spec.code) > MAX_STEP_CODE:
            raise ValueError(f"the step's code is over {MAX_STEP_CODE} bytes")

    @workflow.update(name=FINISH)
    async def finish(self) -> ProgramResult:
        self._finished = True
        return self._result()

    @finish.validator
    def _validate_finish(self) -> None:
        if self._running:
            raise ValueError("a step is still running")
        if self._finished:
            raise ValueError("the run is already finished")

    @workflow.query(name=PROGRESS)
    def progress(self) -> ProgramResult:
        return self._result()

    def _result(self) -> ProgramResult:
        return ProgramResult(goal=self._goal, steps=list(self._steps))

    async def _run_tool(self, request: ToolRequest) -> ToolOutcome:
        """Run one requested side effect on the trusted queue. The arguments are validated as
        the tool's own request model: the guest does not get to send anything else. A policy
        denial is recorded, not raised."""
        outcome = ToolOutcome(tool=request.tool, arguments=request.arguments, status="ok")
        try:
            if request.tool == TOOL_RUN_QUERY:
                query = QueryRequest.model_validate(request.arguments)
                outcome.query = await workflow.execute_activity(
                    TOOL_RUN_QUERY,
                    query,
                    task_queue=TRUSTED_TASK_QUEUE,
                    start_to_close_timeout=TOOL_TIMEOUT,
                    result_type=QueryResult,
                    summary=f"query: {query.sql}",
                )
            elif request.tool == TOOL_SEND_EMAIL:
                email = EmailRequest.model_validate(request.arguments)
                outcome.email = await workflow.execute_activity(
                    TOOL_SEND_EMAIL,
                    email,
                    task_queue=TRUSTED_TASK_QUEUE,
                    start_to_close_timeout=TOOL_TIMEOUT,
                    result_type=EmailResult,
                    summary=f"email: {email.to}",
                )
            else:
                outcome.status, outcome.error = "failed", f"unknown tool {request.tool!r}"
        except ValidationError as err:
            outcome.status = "failed"
            outcome.error = f"the {request.tool} arguments are invalid: {err}"
        except ActivityError as err:
            cause = err.cause
            outcome.status = "failed"
            outcome.error = str(cause or err)
            if isinstance(cause, ApplicationError) and cause.type == POLICY_DENIED:
                outcome.status = "denied"
                details = cause.details[0] if cause.details else {}
                outcome.denial = PolicyDenial.model_validate(details if isinstance(details, dict) else {})
                if not outcome.denial.message:
                    outcome.denial.message = cause.message
        return outcome
