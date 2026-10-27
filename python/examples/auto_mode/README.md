# Auto mode example

The [Monty travel agent](../monty/README.md), with **auto mode** switched on: before a gated
tool call reaches you, Jev judges it against the operator's rules and approves it, denies it,
or leaves it for you. Anything Jev doesn't approve comes to you exactly as it would in the
Monty example, so auto mode only ever reduces how many approvals you see.

It is the same agent, with the same tools, the same Code Mode script tool and the same trip
board, plus the ReAct agent's four weather and location tools
(`examples/react_agent/tool_activities.py`) as extra host functions. This directory adds only the
workflow (`workflow.py`, a copy of Monty's `conversational_workflow.py`) and a worker that needs a
second key.

## The three parts

`workflow.py` wires all three into the `AgentWorkflowRunner`:

- **The switch** — `ToolApprovalPolicy.auto_mode(pre_approved_tools=BOARD_TOOL_NAMES)`. The
  trip-board tools are pre-approved *above* auto mode, so Jev never spends a call on them.
- **The rules** — `_AUTO_APPROVAL_CRITERIA`, named criteria sets (`read_only`,
  `books_travel`, `public_lookup`, `reveals_location`, `run_travel_code`, and a `cautious`
  catch-all), each written as plain-language `approve_when` / `deny_when` / `escalate_when` rules.
- **The mechanism** — `jev_evaluator(extra_state=...)` from
  `temporal_agent_harness.ai_sdks.jev`. The worker registers `TypeSafePlugin`, which makes the
  Jev call. `extra_state` shows Jev the trips on the board and who each is for, which is what
  lets `books_travel` tell a booking for the trip's traveller from one for somebody else.

Monty's travel tools are reused **unchanged**. They don't name a criteria set themselves, so
the rulebook assigns each one by name in `AutoApprovalCriteria.tools`. This is the same
mechanism that brings tools you didn't write, like an MCP server's, under auto mode. A tool
you *do* write can name its own default set instead, with
`@agent.activity_tool_defn(auto_approval_criteria="books_travel")`.

## What to watch in the console

- **Searches run without asking.** `search_flights` / `search_hotels` / `get_trip_summary`
  are `read_only`, which approves calls that only search or summarize.
- **Weather for a named place runs without asking.** `get_coordinates` / `get_weather` are
  `public_lookup`.
- **Finding where you are comes to you.** `get_ip_address` / `get_location_info` are
  `reveals_location`, which always escalates: only the user can agree to share their location.
- **Bookings for the trip's traveller run without asking; anyone else comes to you.**
  `books_travel` approves a `book_flight` / `book_hotel` whose passenger or guest is the
  traveller of a trip on the board, and escalates one for anybody else. Every booking is
  irreversible, so the set lifts the irreversibility ceiling; who the booking is for is the line
  it draws instead.
- **Approvals happen inside the script.** Each host call the script makes is gated on its own,
  so the script pauses at exactly the call that needs you, and carries on once you answer.
- **Every judgment is on the stream.** Each evaluation is bracketed by
  `auto_approval_evaluation_started` → `auto_approval_evaluation_ended` (or `_error` /
  `_superseded` if you answer first). The ended event carries Jev's verdict, its confidence and
  full distribution, the irreversibility judgment, the thresholds applied, and which criteria
  set decided it.
- **You can take it back.** `set_approval_policy` with the `human_approval_only` posture sends
  every gated call straight to you again, and `auto_mode` turns the evaluator back on.

## Setup

Copy the shared env template at the **repo root** and fill it in:

```bash
cp .env.example .env.local     # run from the repo root
```

- Set `GEMINI_API_KEY`, which the agent's conversation runs on.
- Set `TYPESAFE_API_KEY` (from <https://console.typesafe.ai/>) for the Jev evaluator.
  `TYPESAFE_AI_API_KEY` works too. The worker refuses to start without it. Without the key
  the agent would still run, but auto mode would never approve anything, which is harder to
  diagnose than a startup error. If you have no TypeSafe key, run the Monty example instead.
- `TEMPORAL_CONFIG_FILE` defaults to the repo's committed `temporal.local.toml` (a local dev
  server).

## Run

To run this example on its own, each command in its own terminal, all from this directory (the
[demo script](#demo-script) below uses the repo-root `just dev` instead):

```bash
just temporal          # 1. local Temporal dev server (skip if you bring your own)
just session-manager   # 2. session-manager worker
just server            # 3. FastAPI API + built Svelte UI  ->  http://localhost:8000
just worker            # 4. the auto-mode agent
```

Open <http://localhost:8000>, start a **Travel agent (Jev Auto mode)** session, and plan a trip.

## Demo script

One session, three acts. Allow about ten minutes: each turn takes 10–40 seconds, and the first
flight search can sit at "Thinking" for up to a minute before anything shows. That is the model
writing its script, not a hang.

### Before you start

1. Complete [Setup](#setup): `.env.local` at the repo root needs `GEMINI_API_KEY` and
   `TYPESAFE_API_KEY`.
2. From the repo root, start Temporal in one terminal and the stack in another:

   ```bash
   just temporal
   just dev auto_mode
   ```

   `just dev auto_mode` runs the session manager, the API and console, and this agent's worker,
   which also hosts the weather tools; no other example's worker is needed. If the server or
   session manager was already running before you pulled this example's latest changes, run
   `just reset-manager` first: the session manager keeps the agent list it started with, and
   without the reset the console shows a stale description.
3. Open <http://localhost:8000> and start a **Travel agent (Jev Auto mode)** session from the
   **+** (New session) button at the top left.
4. Set up the panes. The console opens with **State flow**, **Agent chat** and the **Timeline**
   drawer. From the **Views** button at the top right, add **Approval decisions**, and
   **Agent state** if you want to show the trip board. On a narrow screen, close **State flow**
   to make room, and collapse the Timeline drawer with its **Collapse Timeline** button (at its
   right end); it grows with every tool call.

**Approval decisions is where to show Jev's judgments.** It lists every gated call with Jev's
verdict (APPROVE or ESCALATE) and confidence, newest first, and the **Decision trace** below it
shows which criteria set decided the selected call. The `jev_evaluator` node in State flow
shows the same verdict, but only while a call is being judged, so for calls Jev approves it is
gone in under a second.

**Approval cards** appear in Agent chat as a **1 NEEDS YOU** card with **Approve**,
**Always allow** and **Reject**. Click **Approve**. Don't click **Always allow**: it
pre-approves that tool for the rest of the session, so the later prompts the script relies on
stop appearing.

### The script

Type each message exactly as written.

1. **"What's the weather in Los Angeles?"** No cards. Approval decisions gains three APPROVE
   rows: `run_travel_code` (the script itself), then `get_coordinates` and `get_weather`, the
   host calls it makes. The agent replies with the weather.
2. **"What is the weather where I am now?"** One script calls `get_ip_address`,
   `get_location_info` and `get_weather` in turn. It pauses at `get_ip_address` with a card,
   then at `get_location_info` (the card shows your IP address). Approve both; `get_weather`
   then runs on Jev's approval and the agent replies with your local weather. Jev escalates
   both lookups because they fall under `reveals_location`, whose only rule is to escalate:
   only you can agree to share where you are. Rejecting either card instead ends the turn with
   the agent saying it couldn't find your location and asking for a city.
3. **Travel.**
   - **"I'm Jordan Lee. Search for a flight from SFO to JFK on November 20."** The agent opens
     a trip for Jordan Lee on the trip board (Agent state shows it) and lists two flights, an
     Alaska nonstop at 20:45 and a Delta flight with two stops. No cards.
   - **"Book the Alaska nonstop."** Booked with no card. Approval decisions shows `book_flight`
     approved: the passenger is the traveller of the trip on the board.
   - **"Search for a flight back from JFK to SFO on November 24."** It lists a JetBlue nonstop
     at 18:15 and an Alaska flight with two stops. No cards.
   - **"Book the JetBlue nonstop, but for Alex Rivera instead of me."** The script pauses at
     `book_flight` with a card showing `passenger_name` Alex Rivera. In Approval decisions,
     select the `book_flight` ESCALATE row: the Decision trace shows the `books_travel`
     criteria decided it. Jev's own reason line only says "the approval criteria do not settle
     this call", so say the why yourself: Alex Rivera is not the traveller on any trip, which
     is outside what the user set up, so a person has to confirm it. Approve, and the booking
     completes and lands on the board.

The agent knows today's date, so "November 20" means the next November 20. Flight ids, times
and prices are generated from the route and that date, so the searches above list the same
flights every run until the date rolls past them; if they differ, adjust the booking lines to
match. Give the second traveller's full name: with only "Alex", the model tends to invent a
surname. The policy control beside the chat box reads "Policy: not set" throughout: nobody has
picked a posture with it this session, so the agent's default, auto mode, applies.
