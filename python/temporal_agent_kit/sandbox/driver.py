"""The workflow-side loop that steps any sandbox runtime to completion.

Each step ends at a batch of calls the guest awaits together. The driver starts every call the
step handed over, waits for the awaited ones CONCURRENTLY, and resumes the guest with their
outcomes, until it finishes. A :class:`~.protocol.Sleep` is a durable timer; every other
suspension goes to the caller's ``dispatch``.

=== Replay-safe step limits ===

Whether a step overruns its limit or crashes its runtime depends on the machine, so it must not
be decided again on replay. When a step is aborted on its original run, the driver records a
patch marker naming the run and step, and the guest ends with the runtime's error. On replay, it
checks for that marker before running the step: if present, it returns the same error without
running the step; if absent, the step completed originally and is re-run. A skipped step must
leave no trace in what the workflow does next, so a guest draws entropy from its own generator
rather than the workflow's.

Workflow-safe: imports no runtime.
"""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from temporalio import workflow

from temporal_agent_kit.sandbox.protocol import Pending, SandboxRun, Sleep, Step


async def drive(
    run: SandboxRun,
    dispatch: Callable[[Pending], Awaitable[Any]],
    *,
    settle: Callable[[Any], Any],
    run_id: str,
    marker_prefix: str,
) -> Step:
    """Step ``run`` until the guest finishes and return its final step.

    ``dispatch`` carries out one suspension other than a sleep. ``settle`` turns a call's
    outcome (its value, or the exception it raised) into what the guest sees; it may re-raise.
    ``run_id`` must be unique per run and drawn deterministically, and ``marker_prefix`` names
    the patch markers; changing either for an existing caller breaks replay of its histories."""

    async def run_call(call: Pending) -> Any:
        if isinstance(call, Sleep):
            await workflow.sleep(call.seconds)
            return None
        return await dispatch(call)

    step = _step(run, run_id, 0, marker_prefix, lambda replaying: run.start(replaying=replaying))
    step_no = 0
    running: dict[int, asyncio.Task[Any]] = {}
    try:
        while not step.done:
            step_no += 1
            # A call that fails is raised inside the guest at its await, where the guest may
            # catch it; the rest of the batch still completes.
            for call in [*step.started, *step.awaiting]:
                if call.call_id not in running:
                    running[call.call_id] = asyncio.create_task(run_call(call))
            outcomes = await asyncio.gather(
                *(running.pop(call.call_id) for call in step.awaiting), return_exceptions=True
            )
            results: Mapping[int, Any] = {
                call.call_id: settle(outcome) for call, outcome in zip(step.awaiting, outcomes)
            }
            step = _step(
                run,
                run_id,
                step_no,
                marker_prefix,
                lambda replaying: run.resume(results, replaying=replaying),
            )
    finally:
        # Calls the guest made but never awaited before it ended.
        for task in running.values():
            task.cancel()
    return step


def _step(
    run: SandboxRun,
    run_id: str,
    step_no: int,
    marker_prefix: str,
    advance: Callable[[bool], Step],
) -> Step:
    """Run one step under the replay-safe limit described in the module docstring."""
    timed_out_marker = f"{marker_prefix}/timed-out/{run_id}/{step_no}"
    crashed_marker = f"{marker_prefix}/crashed/{run_id}/{step_no}"
    if workflow.unsafe.is_replaying():
        if workflow.patched(timed_out_marker):
            return Step(done=True, error=run.timed_out_error)
        if workflow.patched(crashed_marker):
            return Step(done=True, error=run.crashed_error)
        step = advance(True)
        if step.aborted:
            # The step completed on its original run, so this outcome cannot be returned.
            # Fail the workflow task and let Temporal retry it.
            raise RuntimeError(
                f"sandbox run {run_id} step {step_no} completed on its original run "
                f"but not on replay: {step.error}"
            )
        return step

    step = advance(False)
    if not step.aborted:
        return step
    workflow.patched(timed_out_marker if step.timed_out else crashed_marker)
    workflow.logger.warning("sandbox run %s step %d aborted: %s", run_id, step_no, step.error)
    if step.timed_out:
        return Step(done=True, error=run.timed_out_error)
    return Step(done=True, error=run.crashed_error)
