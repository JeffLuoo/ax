# Core concepts

Every AX resource lives in an **atespace**. The default atespace is `default`.

## Task

The smallest unit of isolated execution. A `Task` declares the container image and command, compute limits, environment variables, and optionally a reference to a `Workspace` under `spec.workspace`. The workspace is mounted at `/workspace` unless the binding sets a path, and that path is the command's working directory.

The unit is deliberately small. An agent is not one process that runs to completion; over its lifetime it plans, delegates, retries, and fans work out. AX does not try to model that shape. It gives you one primitive that is cheap to create, isolate, suspend, and throw away, and lets the agent compose as many of them as its work demands. A single task may be the whole job, or it may be the root of a large tree of tasks spawned as the agent breaks the problem down. Either way each node gets the same sandbox, the same lifecycle, and the same tooling.

### Lifecycle

`status.phase` is a one-word summary of where the task is: `Running`, `Suspended`, `Failed`, `Terminating`, and so on. Conditions carry the detail. Check them with `ax describe`.

| Condition | True when |
|---|---|
| `WorkspaceReady` | The workspace has finished setting up. Stays True afterwards. |
| `Ready` | The task is running and `WorkspaceReady` is True. This is the one to wait on. |

Two transitions are worth knowing. Suspending a task sets `Ready` to False with reason `Suspended`; resuming sets it back. Deleting a task tears down the sandbox on Agent Substrate and removes the record. `ax delete` blocks until that has completed.

When the task command exits, the runner records the exit status on the durable volume and serves it at `GET /statusz`. `ax-server` reads it through the atenet router for each running task that has no outcome, and sets `status.outcome` to `Completed` or `Failed`, with `status.exitCode` and `status.finishedAt`. It reads `/statusz` every 15 seconds (`--outcome-poll-interval`). A suspend or a delete of a running task without an outcome also reads `/statusz` once, with a two-second limit, before it acts, so a command that exited after the last read keeps its outcome. If the runner does not answer in time, the suspend or the delete continues without an outcome. The runner does not start the command again after a resume, so a command that completed does not run a second time. The outcome comes from inside the sandbox, so treat it as what the agent reports.

### Task tree and budget

`spec.parent` names the task that started this task, and it must be in the same atespace. The parent links make a task tree, which `ax cost` and `ax describe task` use to add the cost of each sub-task to the cost of its parent. `spec.budget.maxUsd` is the spending limit that the task declares. `ax-server` sends the parent, the budget, the phase, and the outcome to an OpenTelemetry Collector (for example GKE Managed OpenTelemetry exporting to Cloud Logging) as `ax.task.state_changed` events for downstream task-tree joins when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

When `ax-server` creates a task, it sets `status.id` to a unique ID and `status.parentId` to the `status.id` of the parent at that time. If you delete a parent and create a new task with the same name, the new task has a different ID, so it does not adopt the children of the old parent. With authentication on, `ax-server` also sets `status.createdBy` to the verified name of the caller. The events carry these values as `ax.task.id`, `ax.task.parent_id`, and `ax.task.created_by`. Once the actor exists, they also carry `status.actorUid` as `ate.actor.uid`, which downstream cost joins use because a task name can be reused after a delete.

On the events, `ax.task.outcome` is the lowercase structured outcome: `succeeded` (`status.outcome` is `Completed`), `failed` (`Failed`), or `cancelled` for a task deleted before its command reported an exit (with `ax.task.outcome_source=orchestrator`). `unverified` is reserved for a command that exits 0 while a goal verifier fails; `ax-server` runs no verifiers yet. `status.outcome` and `ax.task.phase` keep their values.

`ax-server` sends each event through a queue, and it does not wait for the export. When the queue is full, the exporter drops the event and does not send it again. `ax-server` writes a warning with the number of dropped events to its log. To repair dropped events, `ax-server` sends the current state of every stored task again when it starts and then at an interval (`--task-resync-interval`, default `10m`; `0` turns the resync off). The resync reads 500 tasks at a time and waits one second after each page, so it does not fill the queue. The resync takes the lock of each task for one store read and one actor read only, because each event carries the phase that `GetTask` reports. When an RPC holds the lock of a task for more than two seconds, the resync skips that task, and the RPC sends its own event.


## Workspace

Getting an agent to the point where it can start working is tedious. Before the first useful action it needs its data sources in place, such as repositories cloned at the right revision or buckets mounted, the tools it is allowed to call, and the skills it should bring along. Every task that needs the same environment repeats that setup, and every agent framework reinvents it. A `Workspace` exists to make that work declarative and done once.

A `Workspace` populates the filesystem and tool landscape. Declare it once, bind it from as many tasks as you like, and the runner materializes it inside each sandbox before the command starts:

- **Git repositories** cloned into subdirectories of the workspace path.
- **MCP servers and registries** (Model Context Protocol) that the agent can call.
- **Skill registries** and the path where skills are materialized.

## Model

A configuration for a model provider and its parameters: which provider to call, the model identifier, provider-specific generation parameters such as max_tokens, and references to the Kubernetes secret holding the credentials.

Declaring models as cluster resources makes them manageable and reusable across tasks. The configuration lives in one place instead of in every agent's environment, so rotating a key, pinning a new model version, or tuning parameters is a single `ax apply` rather than a hunt through task definitions. AX's own components read `Model` resources as well.

## Access control

`ax-server` acts on Agent Substrate with its own credentials for all atespaces. For this reason, it checks each caller before it does an RPC. The `ax` CLI sends the bearer token of your kubeconfig context, or the token in `$AX_TOKEN`. `ax-server` verifies the token with a Kubernetes TokenReview. Then it sends a SubjectAccessReview for the RPC.

The SubjectAccessReview uses the API group `ax.io`, a resource (`tasks`, `workspaces`, or `models`), a verb, and the atespace as the namespace. These resources are not in the Kubernetes API. Only `ax-server` reads RBAC rules for them.

| RPC | Verb | Resource |
|---|---|---|
| `GetTask`, `GetWorkspace`, `GetModel` | `get` | `tasks`, `workspaces`, `models` |
| `ListTasks`, `ListWorkspaces`, `ListModels` | `list` | `tasks`, `workspaces`, `models` |
| `CreateTask` | `create` | `tasks` |
| `SuspendTask`, `ResumeTask`, `UpdateTaskBudget` | `update` | `tasks` |
| `UpdateWorkspace`, `UpdateModel` | `update` | `workspaces`, `models` |
| `DeleteTask`, `DeleteWorkspace`, `DeleteModel` | `delete` | `tasks`, `workspaces`, `models` |

To give a user access to one atespace, create a Kubernetes namespace with the name of the atespace, and bind a role there with a RoleBinding. A ClusterRoleBinding gives access to all atespaces. A list with no atespace (`ax get tasks` without `-a`) covers all atespaces, so it needs a ClusterRoleBinding. The deploy manifest has two example roles: `ax-viewer` and `ax-editor`.

`ax-server` rejects a missing or incorrect token with `Unauthenticated`, and a denied RPC with `PermissionDenied`. If it cannot reach the Kubernetes API, it rejects the RPC with `Unavailable`. `GET /healthz` needs no token. The `ax-server` flag `--auth=false` turns the checks off; then each caller can do all RPCs in all atespaces.
