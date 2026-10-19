# Inside the sandbox

Every task container starts with `ax-task-runner` as PID 1. On boot it:

1. Loads the `Task` and every bound `Workspace` spec.
2. Starts its servers: probes and guest management on port 80, and the metadata endpoints on `127.0.0.1:8099`.
3. On the first run, prepares each workspace at its own path, in binding order: clones Git repos, sets up the skills path, and, if the binding has a goal, hands that goal to an Antigravity agent to finish environment setup. The agent needs `GEMINI_API_KEY` in the container and gets 10 minutes by default; set `AX_BOOTSTRAP_TIMEOUT` to a Go duration to change that. The task reports not-ready until every workspace, including any agent run, has finished.
4. Starts `spec.command` as a child process, with the first workspace as its working directory and `AX_METADATA_URL` plus `spec.env` in its environment, and supervises it.

The runner stays up as PID 1 whether or not the command is still running, so the metadata server keeps answering and `ax ssh` still works after the command has exited. Its exit code is logged and written to `/workspace/.ax/command-exit.json` on the durable volume. After a resume, the runner finds that file, serves the stored exit at `/statusz`, and does not set up the workspaces or start the command again. A command that the runner stopped for a suspend has no such file, so it starts again after the resume. When the sandbox is stopped or suspended, the runner sends the command's process group `SIGTERM`, waits ten seconds, and then kills whatever is left.

The task command, and any repository that a workspace clones, can write `/workspace/.ax`. So the agent can write its own exit record, and the outcome that `ax-server` records is what the sandbox reports, not a verified result. The task event to Agent FinOps carries `ax.task.outcome_source=sandbox` to show this. The runner checks the record before it uses it: the file must hold only `exitCode` and `finishedAt`, the exit code must be from -1 to 255, and the finish time must be set and not more than one minute in the future. The runner logs an error, ignores a record that fails the check, and starts the command.

## Probes

Port 80 speaks HTTP/1.1 and `h2c`. Agent Substrate and the AX server use it to check on the task:

| Endpoint | Method | Returns | Description |
|---|---|---|---|
| `/healthz` | `GET` | `text/plain` | Liveness. Always `200 OK`. |
| `/readyz` | `GET` | `text/plain` | Readiness. `503` while the workspace is initializing, `200` once clones, MCP config, and skills are in place. |
| `/statusz` | `GET` | `application/json` | The task command's state: `{"state": "none"}`, `{"state": "running"}`, or `{"state": "exited", "exitCode": 3, "finishedAt": "2026-10-18T08:30:00Z"}`. `ax-server` reads it to set the task's outcome. |

## Metadata server

Your agent can introspect its own configuration without any SDK. The metadata endpoints listen on loopback only, at the address in `AX_METADATA_URL`, so they're reachable from inside the task and nowhere else:

| Endpoint | Method | Returns | Description |
|---|---|---|---|
| `/metadata/v1alpha1/ax/task` | `GET` | `application/yaml` | Task launch configuration, excluding status. |
| `/metadata/v1alpha1/ax/workspaces` | `GET` | `application/yaml` | Every bound `Workspace`, as a multi-document stream in binding order. |

```bash
# From inside a task:
curl -s "$AX_METADATA_URL/metadata/v1alpha1/ax/task"
```

## Guest services

When a task sets `spec.debug: true`, port 80 also serves the [Agent Substrate guest services](https://github.com/agent-substrate/env) over gRPC. They are off by default because they allow arbitrary process execution and file access inside the sandbox, and `ax ssh` refuses to connect to a task that has not enabled them.

- **Process service**: start, inspect, stream output from, and kill processes. This is what powers `ax ssh`.
- **File system service**: streaming file reads and writes inside the workspace.

## Environment provided to your command

| Variable | Value |
|---|---|
| `AX_METADATA_URL` | Base URL of the metadata server, `http://127.0.0.1:8099` by default |
