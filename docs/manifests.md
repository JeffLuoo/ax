# Writing manifests

All four kinds can live in one multi-document YAML file. See [`examples/task.yaml`](../examples/task.yaml) for a complete, working set.

`metadata.name` and `metadata.atespace` become Substrate resource names, so they must be lowercase RFC 1123 labels: at most 63 lowercase alphanumeric characters or `-`, starting and ending with an alphanumeric character. `ax apply` rejects anything else up front rather than letting the task fail later with `ActorCreationFailed`.

## Task

```yaml
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: task123
  atespace: default
spec:
  image: "ghcr.io/my-org/my-agent-image"
  command: ["python", "agent.py"]
  env:
    - name: ENVIRONMENT
      value: "production"

  resources:
    requests:
      cpu: "500m"
      memory: "1Gi"
    limits:
      cpu: "2"
      memory: "4Gi"

  workspaces:
    - name: default-workspace
      path: "/workspace"
      goal: "Install dependencies and run the test suite"   # Antigravity prepares the workspace to this goal on first run

  debug: true   # serve guest services inside the sandbox so `ax ssh` works; off by default

  parent: planner   # optional: the task that started this task, in the same atespace
  budget:
    maxUsd: 5.00    # optional: the spending limit in USD, for model tokens and compute together; 0 means none
```

### Task trees and budgets

`spec.parent` puts a task in a task tree. It names another task in the same atespace, and that task must exist when you apply the child. `ax apply` rejects a parent that does not exist, a parent that is being deleted, and a task that names itself. A parent in another atespace is not possible, so a task tree never crosses an atespace.

`spec.budget.maxUsd` is the spending limit that the task declares, in US dollars. It must be a finite number of 0 or more; 0 means no limit. AX stores the budget and sends it, with the parent, to Agent FinOps in the `ax.task.state_changed` event. `ax cost task <name>` shows the spend against the budget. AX does not stop a task that spends more than its budget.

### Sandbox size

`spec.resources.limits` sets the size of the sandbox: AX writes the CPU and memory limits into the task's Agent Substrate actor template, and Substrate books that size on a worker. When `limits` is not set, AX uses `requests`. When neither is set, AX uses the defaults of `ax-server` (`--default-task-cpu`, default `1`, and `--default-task-memory`, default `2Gi`). The values are Kubernetes quantities, for example `500m`, `2`, or `4Gi`, and they must be greater than zero.

### Outcome

After the task command exits, `status.outcome` is `Completed` (exit status 0) or `Failed` (any other status), with `status.exitCode` and `status.finishedAt`. The runner in the sandbox reports the exit, so the outcome is what the sandbox says. The runner keeps the exit on the durable volume and does not start the command again after a resume.

### Status

`ax-server` owns `status`. It ignores a `status` in the manifest that you apply, so you cannot set an outcome, an exit code, or a finish time. On create, it sets these fields:

- `status.id`: a unique ID for the task. A new task with the name of a deleted task has a different ID.
- `status.parentId`: the `status.id` of the task that `spec.parent` names, at the time of the create.
- `status.createdBy`: the verified name of the caller, from a Kubernetes TokenReview. It is empty when `ax-server` runs with `--auth=false`.

### Binding several workspaces

`spec.workspaces` takes as many entries as you like, so a task can compose reusable `Workspace` resources, for example the code to work on plus a shared set of tools:

```yaml
spec:
  workspaces:
    - name: my-service          # mounted at /workspace/my-service, the command's working directory
      goal: "Install dependencies and run the test suite"
    - name: team-tools
      path: "/workspace/tools"  # explicit mount path
```

Each entry is set up independently at its own path, in order. Every entry needs a `name`; without a `path` it lands at `/workspace/<name>`, and paths must be unique. The first entry is the working directory of `spec.command`, and the task reports `WorkspaceReady` only once all of them are prepared. See [`examples/multi-workspace.yaml`](../examples/multi-workspace.yaml) for a complete set.

## Workspace

```yaml
apiVersion: ax.io/v1alpha1
kind: Workspace
metadata:
  name: default-workspace
  atespace: default
spec:
  git:
    - name: origin
      repo: "https://github.com/chalk/chalk.git"
      branch: "main"
  files:
    - path: "AGENTS.md"
      content: |
        # Project Guidelines
        - Run `go test ./...` before submitting changes.
        - Keep dependencies minimal.
  mcp:
    registries:
      - provider: google
        query: "mcp.tags:build"
    servers:
      - name: git-tools
        endpoint: "http://git-mcp.default.svc.cluster.local:8080"
  skills:
    registries:
      - provider: google
        query: "skills.tags:nodejs"
    path: "/.agents/skills"
```


## Model

For Google models, store the API key and set `provider: google`.

```bash
kubectl create secret generic gemini-api-secret --from-literal=GEMINI_API_KEY="AIzaSy..."
```

Then reference it from the `Model`:

```yaml
apiVersion: ax.io/v1alpha1
kind: Model
metadata:
  name: default-model
  atespace: default
spec:
  provider: google
  model: gemini-3.8-flash
  secretKey:
    name: gemini-api-secret
    key: GEMINI_API_KEY
```

For Anthropic models, store the key the same way and set `provider: anthropic`.

```bash
kubectl create secret generic anthropic-api-secret --from-literal=ANTHROPIC_API_KEY="sk-ant-..."
```

```yaml
apiVersion: ax.io/v1alpha1
kind: Model
metadata:
  name: claude-model
  atespace: default
spec:
  provider: anthropic
  model: claude-opus-5
  secretKey:
    name: anthropic-api-secret
    key: ANTHROPIC_API_KEY
  parameters:
    maxTokens: 16000
```
