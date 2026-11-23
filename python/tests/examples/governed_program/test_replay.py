# ABOUTME: Replays the history of a real run of the demo's story through the proxy (captured with
# `temporal workflow show -o json`) against the current workflow template. In it the proxy
# diverted the denied send_email to its policy-denial queue and failed it as PolicyDenied, so the
# history's task queue differs from the one the workflow asks for; replay must still match.

from __future__ import annotations

import json
from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from examples.governed_program.workflow import GovernedProgramWorkflow

HISTORY = Path(__file__).parent / "histories" / "story_run.json"
DENIAL_QUEUE = "governed-program-policy-denied"


def _history() -> dict:
    return json.loads(HISTORY.read_text())


def test_the_captured_run_diverted_the_denied_call():
    scheduled = [
        e["activityTaskScheduledEventAttributes"]
        for e in _history()["events"]
        if "activityTaskScheduledEventAttributes" in e
    ]
    diverted = [a for a in scheduled if a["taskQueue"]["name"] == DENIAL_QUEUE]
    assert [a["activityType"]["name"] for a in diverted] == ["send_email"]
    assert diverted[0]["retryPolicy"]["maximumAttempts"] == 1
    failures = [
        e["activityTaskFailedEventAttributes"]["failure"]
        for e in _history()["events"]
        if "activityTaskFailedEventAttributes" in e
    ]
    assert [f["applicationFailureInfo"]["type"] for f in failures] == ["PolicyDenied"]


async def test_the_template_replays_a_run_with_a_diverted_call():
    history = WorkflowHistory.from_json("q3-deposits", _history())
    replayer = Replayer(workflows=[GovernedProgramWorkflow], data_converter=pydantic_data_converter)
    result = await replayer.replay_workflow(history)
    assert result.replay_failure is None
