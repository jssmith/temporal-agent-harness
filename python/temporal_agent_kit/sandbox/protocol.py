"""What a sandbox runtime hands the workflow at each step, independent of the runtime.

A guest program runs until it suspends on host calls, then stops; the workflow carries the
calls out and resumes it with their results. Monty (:mod:`..code_mode.monty_stepper`) is one
runtime. Workflow-safe: dataclasses only.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol


class Pending(Protocol):
    """Anything a guest can be suspended on. A runtime may define its own kinds besides
    :class:`HostCall` and :class:`Sleep`; the driver hands those to its dispatcher."""

    @property
    def call_id(self) -> int: ...


@dataclass(frozen=True)
class HostCall:
    """One host function call the guest made, keyed by the runtime's call id."""

    call_id: int
    function_name: str
    args: list[Any]
    kwargs: dict[str, Any]


@dataclass(frozen=True)
class Sleep:
    """A sleep the guest made, answered with a durable timer."""

    call_id: int
    seconds: float


@dataclass(frozen=True)
class Step:
    """Where a step left the guest: awaiting ``awaiting``, or finished.

    ``awaiting`` is what the next resume needs a result for. ``started`` are calls the guest made
    during the step that it is not yet awaiting; the driver starts them now and resolves them
    when a later step awaits them. Every call appears in ``started`` or ``awaiting`` of the step
    it was made in, and an awaited one may already have been started.

    A finished guest has ``output`` (its final value), or ``error`` when it failed. ``aborted``
    marks a step the runtime lost in a way that may not recur on replay, with ``timed_out``
    telling a time limit apart from any other crash."""

    awaiting: list[Pending] = field(default_factory=list)
    started: list[Pending] = field(default_factory=list)
    done: bool = False
    output: Any = None
    error: str | None = None
    aborted: bool = False
    timed_out: bool = False


class SandboxRun(Protocol):
    """One guest program's run, stepped from suspension to suspension.

    ``replaying`` is true when the workflow is replaying, so a runtime can apply a limit that
    only ends a step that never finishes. ``timed_out_error`` and ``crashed_error`` are what the
    guest ends with after an aborted step; the driver replays them without running the step."""

    timed_out_error: str
    crashed_error: str

    def start(self, *, replaying: bool) -> Step: ...

    def resume(self, results: Mapping[int, Any], *, replaying: bool) -> Step: ...
