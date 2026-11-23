"""The guest's step runner: the one activity that runs the agent's code, inside the sandbox.

It is part of the fixed template, like the workflow, but it runs on :data:`GUEST_TASK_QUEUE`
in the sandbox, because the code it runs is untrusted. Each step's code (``StepSpec.code``)
defines ``async def step(ctx)``. It may compute anything and try anything the sandbox allows;
its only way to a trusted side effect is ``ctx.call(tool, **arguments)``, which asks the
workflow to run that tool, where the proxy and the policy decide it.

The runner also records the network connections the code attempts (``EgressAttempt``), from a
Python audit hook, because the proxy cannot see them: they never touch Temporal. With
``PLAYPEN_EGRESS=block`` the hook refuses them with ``ENETUNREACH``, standing in for a sandbox
with no network when the guest runs on the host; in gVisor with no network the kernel refuses
them itself and the hook only records. ``PLAYPEN_SANDBOX_NETWORK`` names the sandbox's network
in the step's outcome.

No ``from __future__ import annotations`` here (stringized annotations trip the pydantic
converter).
"""

import contextvars
import errno
import inspect
import os
import sys
import traceback
from typing import Any

from temporalio import activity

from examples.governed_program.models import EgressAttempt, StepInput, StepPlan, ToolRequest

# The name the workflow template calls the step runner by.
STEP_ACTIVITY = "run_step"

EGRESS_ENV = "PLAYPEN_EGRESS"
SANDBOX_NETWORK_ENV = "PLAYPEN_SANDBOX_NETWORK"

# The attempts of the step running in this context; None outside a step.
_attempts: contextvars.ContextVar[list[EgressAttempt] | None] = contextvars.ContextVar(
    "governed_program_egress", default=None
)


def _target(event: str, args: tuple[Any, ...]) -> str | None:
    if event == "socket.getaddrinfo":
        host, port = args[0], args[1]
        if isinstance(host, bytes):
            host = host.decode(errors="replace")
        return f"{host}:{port}" if port is not None else str(host)
    if event == "socket.connect":
        address = args[1]
        if isinstance(address, tuple) and len(address) >= 2:
            return f"{address[0]}:{address[1]}"
        return str(address)
    return None


def _audit_hook(event: str, args: tuple[Any, ...]) -> None:
    attempts = _attempts.get()
    if attempts is None:
        return
    target = _target(event, args)
    if target is None:
        return
    if os.environ.get(EGRESS_ENV) == "block":
        detail = "refused by the sandbox: it has no network"
        attempts.append(EgressAttempt(target=target, outcome="blocked", detail=detail))
        raise OSError(errno.ENETUNREACH, f"Network is unreachable ({detail})")
    if not any(a.target == target for a in attempts):
        attempts.append(EgressAttempt(target=target, outcome="attempted"))


# Audit hooks cannot be removed; this one does nothing outside a step.
sys.addaudithook(_audit_hook)


class StepContext:
    """What a step's code is given: the goal, the earlier steps' outcomes (as plain dicts, oldest
    first), and the way to ask for a tool."""

    def __init__(self, step: StepInput) -> None:
        self.goal = step.goal
        self.index = step.spec.index
        self.previous: list[dict[str, Any]] = [o.model_dump(mode="json") for o in step.previous]
        self.requests: list[ToolRequest] = []
        self.logs: list[str] = []

    def call(self, tool: str, **arguments: Any) -> None:
        """Ask the workflow to run ``tool``; its outcome is in the step's outcome."""
        self.requests.append(ToolRequest(tool=tool, arguments=arguments))

    def log(self, message: object) -> None:
        self.logs.append(str(message))


@activity.defn(name=STEP_ACTIVITY)
async def run_step(step: StepInput) -> StepPlan:
    """Run one step's code. An error in it is the step's outcome, for the agent to read, not a
    failure of the run."""
    ctx = StepContext(step)
    attempts: list[EgressAttempt] = []
    token = _attempts.set(attempts)
    error = ""
    try:
        namespace: dict[str, Any] = {"__name__": f"governed_program_step_{step.spec.index}"}
        exec(compile(step.spec.code, f"<step {step.spec.index}>", "exec"), namespace)
        entry = namespace.get("step")
        if not callable(entry):
            raise TypeError("the step's code must define `async def step(ctx)`")
        result = entry(ctx)
        if inspect.isawaitable(result):
            await result
    except Exception as err:  # noqa: BLE001 - reported to the agent
        error = "".join(traceback.format_exception_only(err)).strip()
    finally:
        _attempts.reset(token)
    return StepPlan(
        requests=ctx.requests,
        log=ctx.logs,
        egress=attempts,
        sandbox_network=os.environ.get(SANDBOX_NETWORK_ENV, "host network (not isolated)"),
        error=error,
    )
