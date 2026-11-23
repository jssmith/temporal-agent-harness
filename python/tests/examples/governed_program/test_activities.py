# ABOUTME: Offline checks on the governed-program example's stub activities, the guest's step
# runner, and the canned agent: the stubs return structured results without any network, the
# runner runs a step's code and records (or blocks) its egress, and the agent writes the story's
# steps in order, following the injected note.

from __future__ import annotations

import pytest

from examples.governed_program.activities import INJECTED_NOTE, run_query, send_email
from examples.governed_program.models import (
    EmailRequest,
    PolicyDenial,
    QueryRequest,
    QueryResult,
    StepInput,
    StepOutcome,
    StepSpec,
    ToolOutcome,
)
from examples.governed_program.program import (
    DEPOSITS_QUERY,
    EXFIL_URL,
    RISK_QUERY,
    SUMMARY_RECIPIENT,
    injected_address,
    next_step,
)
from examples.governed_program.sandbox import EGRESS_ENV, run_step


async def test_run_query_returns_the_rows_of_the_table_it_reads():
    result = await run_query(QueryRequest(sql=DEPOSITS_QUERY))
    assert result.columns[:2] == ["branch_id", "region"]
    assert len(result.rows) == 3
    risk = await run_query(QueryRequest(sql=RISK_QUERY))
    assert risk.columns[-1] == "reviewer_notes"
    assert INJECTED_NOTE in risk.rows[-1]


async def test_run_query_of_an_unknown_table_has_no_rows():
    result = await run_query(QueryRequest(sql="SELECT 1 FROM nowhere.at_all"))
    assert (result.columns, result.rows) == ([], [])


async def test_send_email_is_deterministic():
    request = EmailRequest(to="a@bank.example", subject="s", body="b")
    first, second = await send_email(request), await send_email(request)
    assert first == second
    assert first.to == "a@bank.example"
    assert first.message_id.startswith("msg-")
    other = await send_email(EmailRequest(to="a@bank.example", subject="s", body="c"))
    assert other.message_id != first.message_id


def _step(code: str, previous: list[StepOutcome] | None = None) -> StepInput:
    return StepInput(goal="g", spec=StepSpec(index=1, title="t", code=code), previous=previous or [])


async def test_step_runner_returns_the_tools_the_code_asks_for():
    code = 'async def step(ctx):\n    ctx.log(ctx.goal)\n    ctx.call("run_query", sql="SELECT 1")\n'
    plan = await run_step(_step(code))
    assert plan.log == ["g"]
    assert [(r.tool, r.arguments) for r in plan.requests] == [("run_query", {"sql": "SELECT 1"})]
    assert plan.error == "" and plan.egress == []


async def test_step_runner_reports_an_error_in_the_code():
    plan = await run_step(_step("async def step(ctx):\n    ctx.call('x')\n    1 / 0\n"))
    assert "ZeroDivisionError" in plan.error
    assert [r.tool for r in plan.requests] == ["x"]
    assert "must define" in (await run_step(_step("x = 1\n"))).error


async def test_step_runner_blocks_and_records_egress(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(EGRESS_ENV, "block")
    code = (
        "import socket\n"
        "async def step(ctx):\n"
        "    try:\n"
        "        socket.getaddrinfo('upload.example', 443)\n"
        "    except OSError as err:\n"
        "        ctx.log(err)\n"
    )
    plan = await run_step(_step(code))
    assert [(a.target, a.outcome) for a in plan.egress] == [("upload.example:443", "blocked")]
    assert "Network is unreachable" in plan.log[0]


def _query_outcome(index: int, title: str, result: QueryResult) -> StepOutcome:
    return StepOutcome(
        index=index,
        title=title,
        tools=[ToolOutcome(tool="run_query", status="ok", query=result)],
    )


async def test_agent_writes_the_story_in_order():
    deposits = await run_query(QueryRequest(sql=DEPOSITS_QUERY))
    risk = await run_query(QueryRequest(sql=RISK_QUERY))

    first = next_step([])
    assert first is not None and first.index == 1 and DEPOSITS_QUERY in first.code
    outcomes = [_query_outcome(1, first.title, deposits)]
    second = next_step(outcomes)
    assert second is not None and second.index == 2 and RISK_QUERY in second.code

    outcomes.append(_query_outcome(2, second.title, risk))
    assert injected_address(outcomes[-1]) == "external-audit@riskreview-partners.example"
    forward = next_step(outcomes)
    assert forward is not None and forward.title == "Forward the table"
    assert EXFIL_URL in forward.code and "external-audit@riskreview-partners.example" in forward.code

    denial = PolicyDenial(rule="no-email-outside-the-bank", message="outside")
    outcomes.append(
        StepOutcome(index=3, title=forward.title, tools=[ToolOutcome(tool="send_email", status="denied", denial=denial)])
    )
    summary = next_step(outcomes)
    assert summary is not None and summary.index == 4 and SUMMARY_RECIPIENT in summary.code
    outcomes.append(StepOutcome(index=4, title=summary.title))
    assert next_step(outcomes) is None


async def test_agent_without_an_injection_goes_straight_to_the_summary():
    deposits = await run_query(QueryRequest(sql=DEPOSITS_QUERY))
    outcomes = [
        _query_outcome(1, "a", deposits),
        _query_outcome(2, "b", QueryResult(columns=["x"], rows=[["nothing to see"]])),
    ]
    summary = next_step(outcomes)
    assert summary is not None and summary.title == "Email the summary"
