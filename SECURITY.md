# Security Policy

Please use https://g.co/vulnz to report security vulnerabilities.

We use https://g.co/vulnz for our intake and triage. For valid issues we will do coordination and disclosure here on GitHub (including using a GitHub Security Advisory when necessary).

The Google Security Team will process your report within a day, and respond within a week (although it will depend on the severity of your report).

## Current trust boundary

AX is in heavy development. Until authentication and isolation land, treat a deployment as follows:

- `ax-server` verifies the bearer token of each caller with a Kubernetes TokenReview, and checks each RPC with a SubjectAccessReview on the API group `ax.io` (see [Access control](docs/concepts.md#access-control)). The deploy manifest accepts kubeconfig tokens (`--auth-allow-apiserver-tokens=true`). Such a token is not bound to `ax-server`, so a compromised `ax-server` can use a caller's token against the Kubernetes API. With `--auth=false`, anyone who can reach the API can read, create, suspend, resume and delete any Task, Workspace or Model.
- Atespaces organize resources. They are not a tenant isolation boundary.
- Setting `spec.debug: true` turns on the guest services, which allow arbitrary command execution and file access inside the sandbox. Whoever can create a Task can set it.
- Creating a Task injects the Gemini API key (the `gemini-api-secret` secret in the task's atespace, or the server's own `GEMINI_API_KEY`) into the sandbox environment, so anyone who can create a Task can read it.
- Anything inside a task's sandbox, including the agent and the task runner, should be treated as untrusted. Don't rely on values reported from inside the sandbox for security decisions.
