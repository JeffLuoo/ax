# Upgrading ax-server

This page lists the defaults that this version of `ax-server` changes, and what each change does to the tasks that exist when you upgrade. Read it before you replace a running `ax-server`.

## Sandbox size

`ax-server` writes CPU and memory limits into the actor template of each task, and Agent Substrate books that size on a worker. `spec.resources.limits` sets the size. A limit that the task does not set gets a fixed default: `1` CPU (`1000m`) and `512Mi` memory. `ax apply` rejects `spec.resources.requests`. See [Sizing the sandbox](manifests.md#sizing-the-sandbox).

The earlier version of this fork had the flags `--default-task-cpu` (default `1`) and `--default-task-memory` (default `2Gi`), and it used `spec.resources.requests` when a task set no limits. This version removes both flags, and it does not use requests. The effect on new tasks:

- A task that sets no memory limit gets `512Mi`, not `2Gi`. To keep the earlier size, set `spec.resources.limits.memory: 2Gi` in each task.
- A manifest that sets `spec.resources.requests` is rejected. Move the values to `spec.resources.limits`.
- Remove `--default-task-cpu` and `--default-task-memory` from the `ax-server` arguments. `ax-server` does not start with a flag that it does not know.
- The Cost Service prices a sandbox by the size that it books. A larger booking costs more, also when the agent does not use it.

## Template name

The name of a task's actor template is a hash of the image and the environment. The environment holds the task's launch configuration (`AX_TASK_YAML`), which includes `spec.resources`. When a new version of `ax-server` writes a different launch configuration, the template name of each existing task is different after the upgrade.

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
- The router resumes a suspended actor to answer a request. `ax-server` sends a request only for a task whose actor is running. It reads the actor from Agent Substrate under the task lock before each request. A suspended task stays suspended.
- `--router-addr` takes a URL or a `host:port`. Port `443` means https: `ax-server` then presents its pod identity certificate to the router, as it does for its readiness probes.

## Task events

When `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` is set, `ax-server` now sends task events (`ax.task.state_changed`) to an OpenTelemetry Collector for downstream task-tree joins. It also sends the current state of every task when it starts, and then every `--task-resync-interval` (default `10m`; `0` turns it off).

The store keeps only the phases of AX's own work: `Pending`, `Failed`, and `Terminating`. `ax-server` reads the other phases (`Running`, `Resuming`, `Suspending`, `Suspended`, and `Unknown`) from the task's actor. Each event carries the phase that `GetTask` reports at that time. So the resync also reads the actor of each task, under the task lock.

## Access control

`ax-server` now checks each caller with a Kubernetes TokenReview and a SubjectAccessReview (`--auth`, default `true`). Give your users RBAC rules for the `ax.io` resources before you upgrade, or they cannot use `ax`. See [Access control](concepts.md#access-control).

## Budgets and holds

`ax-server` now labels each task's actor with the budget that covers it, and reads the actor's holds. It needs an Agent Substrate with actor labels and holds (`SetActorLabels`, `Actor.labels`, `ActorStatus.holds`).

- A task with `spec.budget.maxUsd` and scope `Tree` (the default) is a budget group. Its actor and every actor under it through `spec.parent` get `agent-cost/budget-group: <task>` (the Cost Service keys the group as `group:<atespace>/<task>`); its own actor also gets `agent-cost/budget-limit-usd` and, when `warnAt` is set, `agent-cost/warn-at`. A `Task` scope covers only that task's actor. The outermost Tree budget wins when budgets nest.
- Substrate must let `ax-server`'s identity manage the `agent-cost/budget-*` label prefix. If `SetActorLabels` fails, the task still runs; it gets a `BudgetLabelsSynced=False` condition, and the next reconcile retries.
- `ax budget set <task> --max-usd N` (the new `UpdateTaskBudget` RPC, verb `update` on `tasks`) changes a budget after create and syncs the labels of the whole subtree. The Cost Service sees a raised limit and removes its `agent-cost/budget` holds.
- `GetTask` and `ListTasks` read holds and the budget group label from the actor on each call, with the same actor read that gives the phase. They return them in `status.holds` and `status.budgetGroup`. The store does not keep the holds. A resume of a held task fails with `FailedPrecondition`. The task stays `Suspended`, and a suspended task with a hold has `Ready=False` with reason `Held`.
- The Cost Service is now `cost.ate-cost.svc`. `ax cost` port-forwards to `svc/cost:9090` in namespace `ate-cost` (`--cost-server`, `--cost-namespace`, `$AX_COST_SERVER`). The old `--finops-*` flags and `$AX_FINOPS_*` variables still work.
