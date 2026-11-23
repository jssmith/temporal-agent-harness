# ABOUTME: Tests for the runtime-neutral sandbox driver, from a plain Temporal workflow with a fake
# runtime and no Monty: concurrent host calls dispatched as activities, a call started in one step
# and awaited in a later one, a sleep as a durable timer, a failed call settled into the guest, and
# an aborted step that replays from its patch marker without running the runtime again.

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from temporal_agent_kit.sandbox import HostCall, Pending, Sleep, Step, drive

TASK_QUEUE = "sandbox-driver-test"
TIMED_OUT = "TimeoutError: fake step timed out"
CRASHED = "CrashedError: fake runtime crashed"


@activity.defn
async def add(a: int, b: int) -> int:
    if a < 0:
        raise ApplicationError("negative", non_retryable=True)
    return a + b


class ScriptedRun:
    """A guest that adds pairs of numbers: two concurrent calls and a sleep it awaits later."""

    timed_out_error = TIMED_OUT
    crashed_error = CRASHED

    def __init__(self, first: int) -> None:
        self._first = first
        self.resumed: list[dict[int, Any]] = []

    def start(self, *, replaying: bool) -> Step:
        return Step(
            awaiting=[HostCall(1, "add", [self._first, 2], {}), HostCall(2, "add", [3, 4], {})],
            started=[Sleep(3, 5.0)],
        )

    def resume(self, results: Mapping[int, Any], *, replaying: bool) -> Step:
        self.resumed.append(dict(results))
        if len(self.resumed) == 1:
            return Step(awaiting=[Sleep(3, 5.0)])
        first = self.resumed[0][1]
        total = first if isinstance(first, str) else first + self.resumed[0][2]
        return Step(done=True, output=total)


class AbortingRun:
    """A guest whose first resume overruns its limit."""

    timed_out_error = TIMED_OUT
    crashed_error = CRASHED

    def start(self, *, replaying: bool) -> Step:
        return Step(awaiting=[HostCall(1, "add", [1, 1], {})])

    def resume(self, results: Mapping[int, Any], *, replaying: bool) -> Step:
        return Step(done=True, aborted=True, timed_out=True, error="overran")


async def _dispatch(call: Pending) -> Any:
    assert isinstance(call, HostCall)
    return await workflow.execute_activity(
        add, args=call.args, start_to_close_timeout=timedelta(seconds=10)
    )


def _settle(outcome: Any) -> Any:
    if isinstance(outcome, ActivityError) and outcome.cause is not None:
        return f"error: {outcome.cause}"
    return outcome


@workflow.defn
class ScriptedWorkflow:
    @workflow.run
    async def run(self, first: int) -> Any:
        guest = ScriptedRun(first)
        step = await drive(
            guest,
            _dispatch,
            settle=_settle,
            run_id=str(workflow.uuid4()),
            marker_prefix="test/sandbox",
        )
        return {"output": step.output, "error": step.error, "resumes": len(guest.resumed)}


@workflow.defn
class AbortingWorkflow:
    @workflow.run
    async def run(self) -> Any:
        step = await drive(
            AbortingRun(),
            _dispatch,
            settle=_settle,
            run_id=str(workflow.uuid4()),
            marker_prefix="test/sandbox",
        )
        return {"output": step.output, "error": step.error}


WORKFLOWS = [ScriptedWorkflow, AbortingWorkflow]


async def _run(env: WorkflowEnvironment, workflow_run: Any, *args: Any) -> tuple[Any, Any]:
    async with Worker(
        env.client,
        task_queue=TASK_QUEUE,
        workflows=WORKFLOWS,
        activities=[add],
        workflow_runner=UnsandboxedWorkflowRunner(),
    ):
        handle = await env.client.start_workflow(
            workflow_run, args=args, id=f"sandbox-{uuid.uuid4()}", task_queue=TASK_QUEUE
        )
        result = await handle.result()
        return result, await handle.fetch_history()


async def _replay(history: Any) -> None:
    await Replayer(workflows=WORKFLOWS, workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(
        history
    )


async def test_drives_a_runtime_that_is_not_monty() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result, history = await _run(env, ScriptedWorkflow.run, 1)
    assert result == {"output": 10, "error": None, "resumes": 2}
    await _replay(history)


async def test_a_failed_call_is_settled_into_the_guest() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result, _ = await _run(env, ScriptedWorkflow.run, -1)
    assert result == {"output": "error: negative", "error": None, "resumes": 2}


async def test_an_aborted_step_replays_from_its_marker() -> None:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result, history = await _run(env, AbortingWorkflow.run)
    assert result == {"output": None, "error": TIMED_OUT}
    # AbortingRun aborts on every resume, so replay only succeeds if the marker skips the step.
    await _replay(history)
