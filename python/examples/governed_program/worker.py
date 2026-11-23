"""The trusted worker of the governed-program demo: it serves the side effects.

Run from the repo root with:
    bazel run //python/examples/governed_program:worker

(or `just worker` from examples/governed_program).

It polls :data:`~examples.governed_program.activities.TRUSTED_TASK_QUEUE` for the stub ``run_query`` and ``send_email`` activities. It
connects to Temporal directly, as a trusted worker; the guest, which hosts the workflow template
and the generated activity, connects only through the proxy and is launched separately.

Connection settings come from a ``temporal.toml`` profile, resolved through temporalio's
``ClientConfig.load_client_connect_config()`` (TEMPORAL_CONFIG_FILE / TEMPORAL_PROFILE, set in
``.env.local``).

Env vars:
    TEMPORAL_CONFIG_FILE    path to a temporal.toml (set in .env.local)
    TEMPORAL_PROFILE        profile name to load (default: "default")
"""

from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.envconfig import ClientConfig
from temporalio.worker import Worker

from examples.governed_program.activities import TRUSTED_ACTIVITIES, TRUSTED_TASK_QUEUE


async def main() -> None:
    connect_config = ClientConfig.load_client_connect_config()
    client = await Client.connect(**connect_config, data_converter=pydantic_data_converter)
    # The queue's name is fixed: the policy's action names start with it.
    worker = Worker(client, task_queue=TRUSTED_TASK_QUEUE, activities=TRUSTED_ACTIVITIES)
    print(f"Governed program trusted worker ready: taskQueue={TRUSTED_TASK_QUEUE}", flush=True)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
