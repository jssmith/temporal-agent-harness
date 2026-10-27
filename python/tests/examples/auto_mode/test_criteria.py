# ABOUTME: Offline checks on the auto-mode example's rulebook. Monty's travel tools carry no
# criteria set of their own, so the example assigns each one BY NAME; these tests catch a tool
# being renamed or added in examples/monty without the rulebook following it.

from __future__ import annotations

from examples.auto_mode.workflow import CODE_MODE_TOOL_NAME, _AUTO_APPROVAL_CRITERIA
from examples.monty import activities, trip_board
from examples.react_agent import tool_activities as weather

_GATED_TOOLS = (
    CODE_MODE_TOOL_NAME,
    *(tool.__name__ for tool in [*activities.ALL_TOOLS, *weather.ALL_TOOLS]),
)


def test_every_gated_tool_is_assigned_a_set_by_name():
    """Assignment is by string, so a rename in examples/monty would otherwise drop the tool
    onto the `cautious` catch-all SILENTLY. That is the safe direction (it always escalates),
    which is exactly why nothing else would notice: searches would just stop auto-approving."""
    assigned = set(_AUTO_APPROVAL_CRITERIA.tools)
    assert set(_GATED_TOOLS) <= assigned, sorted(set(_GATED_TOOLS) - assigned)
    # And nothing in the map names a tool that no longer exists.
    assert assigned <= set(_GATED_TOOLS), sorted(assigned - set(_GATED_TOOLS))


def test_every_assignment_resolves_to_a_real_set():
    """A dangling set name resolves to nothing, which the harness escalates — again safe, and
    again invisible."""
    for tool in _GATED_TOOLS:
        name = _AUTO_APPROVAL_CRITERIA.set_name_for(tool)
        assert _AUTO_APPROVAL_CRITERIA.for_tool(tool) is not None, (tool, name)
        assert name != _AUTO_APPROVAL_CRITERIA.default, tool


def test_board_tools_are_not_in_the_rulebook():
    """The trip-board tools are pre-approved above auto mode by the policy, so Jev never sees
    them; assigning them a set would suggest otherwise."""
    assert not set(trip_board.BOARD_TOOL_NAMES) & set(_AUTO_APPROVAL_CRITERIA.tools)


def test_finding_the_users_location_can_never_be_auto_approved():
    """The demo's location beat depends on these reaching a person every time: the set must
    give Jev no approve rule to match."""
    for tool in (weather.get_ip_address.__name__, weather.get_location_info.__name__):
        criteria_set = _AUTO_APPROVAL_CRITERIA.for_tool(tool)
        assert criteria_set is not None, tool
        assert not criteria_set.approve_when, tool
        assert criteria_set.escalate_when, tool


def test_bookings_are_decided_by_traveller_not_by_irreversibility():
    """Every booking is irreversible, so under the default ceiling every booking would escalate
    and the traveller rules would never decide anything."""
    criteria_set = _AUTO_APPROVAL_CRITERIA.for_tool(activities.book_flight_activity.__name__)
    assert criteria_set is not None
    assert criteria_set.escalate_if_irreversible_above == 1.0
    assert criteria_set.approve_when and criteria_set.escalate_when
