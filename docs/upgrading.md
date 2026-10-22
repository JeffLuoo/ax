# Upgrading ax-server

This page lists the defaults that this version of `ax-server` changes, and what each change does to the tasks that exist when you upgrade. Read it before you replace a running `ax-server`.

## Sandbox size

`ax-server` now writes CPU and memory limits into the actor template of each task, and Agent Substrate books that size on a worker. A task that sets no `spec.resources` gets `--default-task-cpu` (default `1`) and `--default-task-memory` (default `2Gi`). Before this version, the template had no limits, and the sandbox got the size that Agent Substrate gives a template without limits.

The effect on new tasks:

- A worker can hold fewer tasks if its earlier tasks were smaller than 1 vCPU and 2 GiB.
- Agent FinOps prices a sandbox by the size that it books. A larger booking costs more, also when the agent does not use it.
- To keep the earlier size, set `spec.resources` in each task, or set `--default-task-cpu` and `--default-task-memory`.

## Template name

The name of a task's actor template is a hash of the image, the environment, and the sandbox limits. The limits are new in the hash, so the template name of each existing task is different after the upgrade. A change to `--default-task-cpu` or `--default-task-memory` also changes the name.

`ax-server` does not move an existing actor to a new template. On each suspend or resume, it reads the actor from Agent Substrate. If the actor exists, `ax-server` uses the template of the actor, and it does not create a template with the new name. So:

- An existing task keeps its earlier size and its earlier template until you delete it.
- The upgrade does not create a template that no actor uses, and a suspend or a resume does not wait for the golden snapshot of a new template.
- If Agent Substrate deleted the actor, or deletes it at that time, `ax-server` creates the actor again from a template with the new name and the new size. The workspace of the earlier actor is not in the new actor.
- If the actor crashed, `ax-server` reverts it to its last snapshot. If the revert fails, `ax-server` creates the actor again from its earlier template.
- `ax delete` removes every template of the task, with the earlier name and with the new name.

To give an existing task the new size, delete the task and create it again.

## Task outcomes

`ax-server` now reads the command state of each running task without an outcome every 15 seconds (`--outcome-poll-interval`; `0` turns it off). It sends `GET /statusz` to the runner through the atenet router at `--router-addr` (default `$ATENET_ROUTER_ADDR`), with not more than 8 requests at a time (`--outcome-poll-concurrency`). A suspend or a delete of a running task also sends one request first. Without a router address, `ax-server` records no outcomes.

The effect on existing tasks:

- A task that is running at the upgrade gets an outcome when its command exits.
- A task that runs a runner image without `/statusz` gets an error from each request, and it gets no outcome. `ax-server` logs the error at debug level only.
- The router resumes a suspended actor to answer a request. `ax-server` sends a request only for a task that is `Running` in its store, under the task lock. A suspended task stays suspended.

## Task events

When `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` is set, `ax-server` now sends task events (`ax.task.state_changed`) to an OpenTelemetry Collector for downstream task-tree joins. It also sends the current state of every task when it starts, and then every `--task-resync-interval` (default `10m`; `0` turns it off).

## Access control

`ax-server` now checks each caller with a Kubernetes TokenReview and a SubjectAccessReview (`--auth`, default `true`). Give your users RBAC rules for the `ax.io` resources before you upgrade, or they cannot use `ax`. See [Access control](concepts.md#access-control).
