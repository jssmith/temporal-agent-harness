"""The Playpen feed: one small service that joins what the console shows about a guest run.

It runs beside the trusted worker, not inside it, and reads four sources for one workflow: the
proxy's audit feed and escalation decisions, the workflow's Temporal history, and the control
plane's view of the agent, its generated program, and the sandbox. ``state.build_state`` turns
them into the one JSON document the page in ``../ui`` renders; ``server`` serves both.
"""
