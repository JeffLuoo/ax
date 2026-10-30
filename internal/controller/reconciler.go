// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const (
	// templateDigestBytes is how many bytes of the spec digest go into a
	// per-task ActorTemplate name.
	templateDigestBytes = 4
	// defaultWorkspaceReadyTimeout is how long Reconcile waits for a freshly
	// resumed actor to finish workspace setup.
	defaultWorkspaceReadyTimeout = 15 * time.Second
	workspaceReadyPollInterval   = 500 * time.Millisecond

	defaultServiceDNSCAFile      = "/run/servicedns-ca/trust-bundle.pem"
	defaultPodIdentityBundleFile = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"
	defaultRouterServerName      = "atenet-router.ate-system.svc"
)

// TaskReconciler reconciles Task resources by provisioning and orchestrating
// sandboxed Actors on Agent Substrate.
type TaskReconciler struct {
	client                  *substrate.Client
	httpClient              *http.Client
	defaultTemplate         string
	defaultTemplateAtespace string
	// defaultTaskImage is applied to spec.image for tasks that leave it empty.
	defaultTaskImage string

	// WorkspaceReadyTimeout bounds how long Reconcile waits for the actor's workspace
	// to report ready before recording it as still initializing.
	WorkspaceReadyTimeout time.Duration

	// Tasks reads a task from the store, to find the task that owns the
	// budget of a task tree. Nil limits budget labels to the task's own
	// spec.budget.
	Tasks budget.Lookup
}

// NewTaskReconciler creates a new TaskReconciler. Empty defaults fall back to
// "default-template", v1alpha1.DefaultSystemNamespace and
// v1alpha1.DefaultTaskImage respectively.
func NewTaskReconciler(client *substrate.Client, defaultTemplate, defaultTemplateAtespace, defaultTaskImage string) *TaskReconciler {
	if defaultTemplate == "" {
		defaultTemplate = "default-template"
	}
	if defaultTemplateAtespace == "" {
		defaultTemplateAtespace = v1alpha1.DefaultSystemNamespace
	}
	if defaultTaskImage == "" {
		defaultTaskImage = v1alpha1.DefaultTaskImage
	}
	return &TaskReconciler{
		client:                  client,
		httpClient:              newProbeHTTPClient(),
		defaultTemplate:         defaultTemplate,
		defaultTemplateAtespace: defaultTemplateAtespace,
		defaultTaskImage:        defaultTaskImage,
		WorkspaceReadyTimeout:   defaultWorkspaceReadyTimeout,
	}
}

// Client returns the Substrate client used by the reconciler.
func (r *TaskReconciler) Client() *substrate.Client {
	return r.client
}

// NewRouterHTTPClient returns an HTTP client for requests to a task through
// the atenet router, with a 2 s timeout. For an https router address, it
// presents the pod identity certificate and trusts the service DNS CA, as the
// router's mTLS listener on port 443 requires. The reconciler uses the same
// client for its readiness probes.
func NewRouterHTTPClient() *http.Client {
	return newProbeHTTPClient()
}

// RouterURL returns the URL of path on the atenet router at addr. addr is a
// URL, or a host:port: port 443 means https, any other port http.
func RouterURL(addr, path string) string {
	u := addr
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		if strings.HasSuffix(u, ":443") {
			u = "https://" + u
		} else {
			u = "http://" + u
		}
	}
	return strings.TrimRight(u, "/") + path
}

func newProbeHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: defaultRouterServerName,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			cert, err := tls.LoadX509KeyPair(defaultPodIdentityBundleFile, defaultPodIdentityBundleFile)
			if err != nil {
				return &tls.Certificate{}, nil
			}
			return &cert, nil
		},
	}
	if caBytes, err := os.ReadFile(defaultServiceDNSCAFile); err == nil {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(caBytes) {
			tlsCfg.RootCAs = pool
		}
	}
	transport.TLSClientConfig = tlsCfg
	return &http.Client{
		Timeout:   2 * time.Second,
		Transport: transport,
	}
}

// Reconcile handles the reconciliation loop for a single Task.
func (r *TaskReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, ws *v1alpha1.Workspace) (*v1alpha1.Task, error) {
	if task.Metadata == nil {
		task.Metadata = &v1alpha1.ObjectMeta{}
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	atespace := task.Metadata.Atespace
	if atespace == "" {
		atespace = "default"
	}
	task.Metadata.Atespace = atespace

	if task.Spec == nil {
		task.Spec = &v1alpha1.TaskSpec{}
	}
	if task.Spec.Image == "" {
		task.Spec.Image = r.defaultTaskImage
	}

	slog.Info("reconciling task", "name", task.Metadata.Name, "atespace", atespace, "image", task.Spec.Image)

	now := time.Now()

	// 1. Ensure the Atespace exists in Substrate
	if err := r.client.EnsureAtespace(ctx, atespace); err != nil {
		if substrate.IsTransientError(err) {
			r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
			task.Status.Phase = v1alpha1.PhasePending
			return task, fmt.Errorf("ensuring atespace: %w", err)
		}
		r.setNotReady(task, v1alpha1.ReasonAtespaceCreationFailed, err.Error(), now)
		task.Status.Phase = v1alpha1.PhaseFailed
		return task, fmt.Errorf("ensuring atespace: %w", err)
	}

	// 2. The actor is always named after the task, so the two can be used
	// interchangeably (for example in the router's ate-target-actor header).
	// Whatever a client put in status.actor is overwritten.
	actorName := task.Metadata.Name
	task.Status.Actor = actorName
	if task.Status.Id == "" {
		task.Status.Id = fmt.Sprintf("task-%s-%d", task.Metadata.Name, now.Unix())
	}

	// 3. Ensure Actor exists on Substrate
	templateName := r.defaultTemplate
	templateAtespace := r.defaultTemplateAtespace
	if parts := strings.SplitN(templateName, "/", 2); len(parts) == 2 {
		templateAtespace = parts[0]
		templateName = parts[1]
	}

	// Prepare container environment variables (task env + specs)
	extraEnv := make(map[string]string)
	if task.Spec != nil {
		for _, e := range task.Spec.Env {
			if e.Name != "" {
				extraEnv[e.Name] = e.Value
			}
		}
	}

	// Only launch configuration belongs in the template; status changes
	// must not create new golden snapshots.
	launchTask := proto.Clone(task).(*v1alpha1.Task)
	launchTask.Status = nil
	if taskYAML, err := yaml.Marshal(launchTask); err == nil {
		extraEnv["AX_TASK_YAML"] = string(taskYAML)
	}

	// Inject the bound Workspace spec, if any.
	if ws != nil {
		if wsYAML, err := yaml.Marshal(ws); err == nil {
			extraEnv["AX_WORKSPACE_YAML"] = string(wsYAML)
		}
	}

	// Resource limits are enforced by the per-task template, so they are checked
	// here as well as at apply time: a task must never run without limits it asked for.
	if err := v1alpha1.ValidateResources(task.Spec.Resources); err != nil {
		r.setNotReady(task, v1alpha1.ReasonInvalidResources, err.Error(), now)
		task.Status.Phase = v1alpha1.PhaseFailed
		return task, fmt.Errorf("validating resources: %w", err)
	}
	resources := substrate.ResourceLimits(task.Spec.Resources)

	// An existing actor keeps the template that it was created from: Substrate
	// does not move an actor to another template. The template name hashes
	// the launch configuration, so an upgrade of ax-server can change the name
	// for an existing task. A new template then has no actor, and its golden
	// snapshot costs time and storage, so do not create it.
	if ref := r.existingActorTemplate(ctx, atespace, actorName); ref != nil {
		templateAtespace = ref.GetAtespace()
		templateName = ref.GetName()
	} else if task.Spec != nil && (task.Spec.Image != "" || len(extraEnv) > 0) {
		// If a custom image, workspace, or extra environment is specified, provision or use a dedicated ActorTemplate
		slog.Info("ensuring custom ActorTemplate for task", "image", task.Spec.Image)
		customTemplateName := taskTemplateName(task.Metadata.Name, task.Spec.Image, extraEnv)

		// spec.resources rides along in AX_TASK_YAML, so a limits change is already
		// part of the template digest and re-provisions the template.
		tmpl, err := r.client.EnsureActorTemplateWithImage(ctx, templateAtespace, templateName, atespace, customTemplateName, task.Spec.Image, resources, extraEnv)
		if err != nil {
			if substrate.IsTransientError(err) {
				r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
				task.Status.Phase = v1alpha1.PhasePending
				return task, fmt.Errorf("ensuring actor template: %w", err)
			}
			// Falling back to the default template would run a different image than
			// the user asked for, without the limits it asked for, or fail later with
			// a misleading "actor template not found". Surface the real error on the
			// task and stop here.
			r.setNotReady(task, v1alpha1.ReasonTemplateCreationFailed, substrateErrorMessage(err), now)
			task.Status.Phase = v1alpha1.PhaseFailed
			return task, fmt.Errorf("ensuring actor template: %w", err)
		}
		if tmpl != nil && tmpl.Metadata != nil {
			templateAtespace = tmpl.Metadata.Atespace
			templateName = tmpl.Metadata.Name
			slog.Info("using custom ActorTemplate for actor", "templateAtespace", templateAtespace, "templateName", templateName)
		}
	}

	actorLabels := budget.Resolve(ctx, r.Tasks, task)
	actor, err := r.client.EnsureActorWithLabels(ctx, atespace, actorName, templateAtespace, templateName, actorLabels.Set)
	if err != nil {
		if substrate.IsTransientError(err) {
			r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
			task.Status.Phase = v1alpha1.PhasePending
			return task, fmt.Errorf("ensuring actor: %w", err)
		}
		r.setNotReady(task, v1alpha1.ReasonActorCreationFailed, err.Error(), now)
		task.Status.Phase = v1alpha1.PhaseFailed
		return task, fmt.Errorf("ensuring actor: %w", err)
	}
	if uid := actor.GetMetadata().GetUid(); uid != "" {
		task.Status.ActorUid = uid
	}
	actor = r.applyBudgetLabels(ctx, task, actor, actorLabels, now)
	task.Status.Holds = substrate.Holds(actor)

	// 4. Suspend or Resume the Actor
	// Tasks are suspended by default upon creation until explicitly resumed.
	if task.Status.Phase == v1alpha1.PhaseSuspended || task.Status.Phase == "" {
		slog.Info("suspending actor on Substrate", "actor", actorName)
		if err := r.client.SuspendActor(ctx, atespace, actorName); err != nil {
			return task, fmt.Errorf("suspending actor: %w", err)
		}
		task.Status.WorkerIp = ""
		task.Status.Phase = v1alpha1.PhaseSuspended
		slog.Info("task successfully suspended", "name", task.Metadata.Name, "actor", actorName)
		return task, nil
	}

	// Resume the Actor to activate container execution
	slog.Info("resuming actor on Substrate worker", "actor", actorName)
	_, workerIP, err := r.client.ResumeActor(ctx, atespace, actorName)
	if err != nil {
		if held, ok := substrate.AsHeld(err); ok {
			// A hold is not a failure: the task keeps its state and resumes
			// once the hold's owner removes it.
			task.Status.WorkerIp = ""
			task.Status.Phase = v1alpha1.PhaseSuspended
			r.setNotReady(task, condReasonHeld, held.Error(), now)
			if a, getErr := r.client.GetActor(ctx, atespace, actorName); getErr == nil {
				task.Status.Holds = substrate.Holds(a)
			}
			return task, held
		}
		if substrate.IsTransientError(err) {
			r.setNotReady(task, v1alpha1.ReasonSubstrateUnavailable, err.Error(), now)
			task.Status.Phase = v1alpha1.PhasePending
			return task, fmt.Errorf("resuming actor: %w", err)
		}
		r.setNotReady(task, v1alpha1.ReasonActorResumeFailed, err.Error(), now)
		task.Status.Phase = v1alpha1.PhaseFailed
		return task, fmt.Errorf("resuming actor: %w", err)
	}

	task.Status.WorkerIp = workerIP
	task.Status.Phase = v1alpha1.PhaseRunning

	// Check if workspace setup inside the actor has completed
	host := workerIP
	port := "80"
	if h, p, err := net.SplitHostPort(workerIP); err == nil {
		host = h
		port = p
	}
	readyURL := fmt.Sprintf("http://%s/readyz?check=workspace", net.JoinHostPort(host, port))
	// Workspace setup happens once per task. After it has completed, WorkspaceReady stays
	// True across suspend/resume cycles, so only poll while it is still initializing.
	workspaceReady := conditionTrue(task, v1alpha1.ConditionWorkspaceReady)
	if workerIP != "" && !workspaceReady {
		// Poll briefly for workspace setup completion
		pollCtx, cancel := context.WithTimeout(ctx, r.WorkspaceReadyTimeout)
		defer cancel()

		ticker := time.NewTicker(workspaceReadyPollInterval)
		defer ticker.Stop()

		checkReady := func() bool {
			// 1. Direct readyz check
			req, _ := http.NewRequestWithContext(pollCtx, http.MethodGet, readyURL, nil)
			if resp, err := r.httpClient.Do(req); err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return true
				}
			}

			// 2. Router-based readyz check (in-cluster via atenet-router)
			if routerAddr := os.Getenv("ATENET_ROUTER_ADDR"); routerAddr != "" {
				routerURL := RouterURL(routerAddr, "/readyz?check=workspace")
				rReq, _ := http.NewRequestWithContext(pollCtx, http.MethodGet, routerURL, nil)
				if rReq != nil {
					rReq.Header.Set("ate-target-actor", fmt.Sprintf("%s/%s", atespace, actorName))
					if resp, err := r.httpClient.Do(rReq); err == nil {
						_ = resp.Body.Close()
						if resp.StatusCode == http.StatusOK {
							return true
						}
					}
				}
			}
			return false
		}

		// Initial check
		workspaceReady = checkReady()

		for !workspaceReady {
			select {
			case <-pollCtx.Done():
				goto DonePolling
			case <-ticker.C:
				if checkReady() {
					workspaceReady = true
					goto DonePolling
				}
			}
		}
	}
DonePolling:

	// The server derives Ready from the actor's state and WorkspaceReady.
	if workspaceReady {
		if !conditionTrue(task, v1alpha1.ConditionWorkspaceReady) {
			setCondition(task, v1alpha1.ConditionWorkspaceReady, "True", v1alpha1.ReasonSetupComplete, fmt.Sprintf("Workspace setup completed at %s", workerIP), time.Now())
		}
	} else {
		setCondition(task, v1alpha1.ConditionWorkspaceReady, "False", v1alpha1.ReasonInitializing, fmt.Sprintf("Workspace is initializing at %s", workerIP), time.Now())
	}

	slog.Info("task successfully reconciled and running",
		"name", task.Metadata.Name,
		"actor", actorName,
		"workerIP", workerIP,
		"workspaceReady", workspaceReady,
		"phase", task.Status.Phase,
	)

	return task, nil
}

// Condition types and reasons that only this fork sets on Task status.
// Package v1alpha1 has the shared ones.
const (
	// condBudgetLabels reports whether the actor carries the budget labels of
	// the budget that covers the task. It is added only after a failed sync.
	condBudgetLabels = "BudgetLabelsSynced"
	// condReasonHeld is the Ready reason of a task whose actor has a hold.
	condReasonHeld = "Held"
)

// applyBudgetLabels brings the actor's agent-cost/budget-* labels to want and
// records the budget group on the task. It returns the updated actor, or
// actor unchanged when nothing changed or the sync failed. A failed sync does
// not fail the task: the next reconcile retries it.
func (r *TaskReconciler) applyBudgetLabels(ctx context.Context, task *v1alpha1.Task, actor *ateapipb.Actor, want budget.Labels, now time.Time) *ateapipb.Actor {
	task.Status.BudgetGroup = want.Group(task.GetMetadata().GetAtespace())
	set, del := want.Diff(actor.GetLabels())
	if set == nil && del == nil {
		if hasCondition(task, condBudgetLabels) {
			setCondition(task, condBudgetLabels, "True", "Synced", "Actor budget labels are up to date", now)
		}
		return actor
	}
	atespace, name := task.GetMetadata().GetAtespace(), task.GetStatus().GetActor()
	updated, err := r.client.SetActorLabels(ctx, atespace, name, set, del)
	if err != nil {
		slog.Warn("could not sync actor budget labels", "actor", atespace+"/"+name, "error", err)
		setCondition(task, condBudgetLabels, "False", "SyncFailed", err.Error(), now)
		return actor
	}
	slog.Info("synced actor budget labels", "actor", atespace+"/"+name, "set", set, "deleted", del)
	if hasCondition(task, condBudgetLabels) {
		setCondition(task, condBudgetLabels, "True", "Synced", "Actor budget labels are up to date", now)
	}
	if updated == nil {
		return actor
	}
	return updated
}

// SyncBudgetLabels brings the budget labels of the task's existing actor up
// to date, and refreshes the task's holds. It does not create, suspend, or
// resume the actor. A task with no actor yet is returned unchanged.
func (r *TaskReconciler) SyncBudgetLabels(ctx context.Context, task *v1alpha1.Task) (*v1alpha1.Task, error) {
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	atespace := task.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	name := task.GetMetadata().GetName()
	actor, err := r.client.GetActor(ctx, atespace, name)
	if status.Code(err) == codes.NotFound {
		return task, nil
	}
	if err != nil {
		return task, fmt.Errorf("reading actor %s/%s: %w", atespace, name, err)
	}
	task.Status.Actor = name
	want := budget.Resolve(ctx, r.Tasks, task)
	actor = r.applyBudgetLabels(ctx, task, actor, want, time.Now())
	task.Status.Holds = substrate.Holds(actor)
	return task, nil
}

// hasCondition reports whether the task has the given condition type.
func hasCondition(task *v1alpha1.Task, condType string) bool {
	for _, c := range task.GetStatus().GetConditions() {
		if c.Type == condType {
			return true
		}
	}
	return false
}

// setNotReady marks the task's Ready condition False. WorkspaceReady is left untouched:
// workspace setup is a one-time step whose result outlives actor failures and suspends.
func (r *TaskReconciler) setNotReady(task *v1alpha1.Task, reason, message string, t time.Time) {
	setCondition(task, v1alpha1.ConditionReady, "False", reason, message, t)
}

// conditionTrue reports whether the task currently has the given condition with status True.
func conditionTrue(task *v1alpha1.Task, condType string) bool {
	if task.Status == nil {
		return false
	}
	for _, c := range task.Status.Conditions {
		if c.Type == condType {
			return c.Status == "True"
		}
	}
	return false
}

// substrateErrorMessage returns the message to show users for a failed Substrate call.
// Substrate errors reach the reconciler wrapped (fmt.Errorf("...: %w", grpcErr)), and
// status.Convert on a wrapped error yields the full err.Error() including the
// "rpc error: code = ... desc = " boilerplate. Unwrapping to the underlying gRPC
// status recovers just the description Substrate wrote, e.g.
// `actor_template.containers[0].image: Invalid value: "...": must be pinned by digest`.
// Non-gRPC errors are returned verbatim.
func substrateErrorMessage(err error) string {
	var gs interface{ GRPCStatus() *status.Status }
	if errors.As(err, &gs) {
		if s := gs.GRPCStatus(); s != nil && s.Message() != "" {
			return s.Message()
		}
	}
	return err.Error()
}

func setCondition(task *v1alpha1.Task, condType, status, reason, message string, t time.Time) {
	ts := timestamppb.New(t)
	for i, c := range task.Status.Conditions {
		if c.Type == condType {
			task.Status.Conditions[i].Status = status
			task.Status.Conditions[i].Reason = reason
			task.Status.Conditions[i].Message = message
			task.Status.Conditions[i].UpdateTime = ts
			return
		}
	}
	task.Status.Conditions = append(task.Status.Conditions, &v1alpha1.Condition{
		Type:       condType,
		Status:     status,
		Reason:     reason,
		Message:    message,
		UpdateTime: ts,
	})
}

// existingActorTemplate returns the template of the task's actor, or nil when
// the actor does not exist, is being deleted, or cannot be read. A crashed
// actor counts: EnsureActor reverts it, or creates it again from the same
// template.
func (r *TaskReconciler) existingActorTemplate(ctx context.Context, atespace, actorName string) *ateapipb.ObjectRef {
	actor, err := r.client.GetActor(ctx, atespace, actorName)
	if err != nil || actor == nil {
		return nil
	}
	if actor.GetStatus().GetState() == ateapipb.ActorState_ACTOR_STATE_DELETING {
		return nil
	}
	ref := actor.GetActorTemplate()
	if ref.GetName() == "" {
		return nil
	}
	return ref
}

// taskTemplateName derives the per-task ActorTemplate name from the task name and a
// digest of the image and container environment, so a spec change yields a new
// template. An existing actor keeps its template; see existingActorTemplate.
func taskTemplateName(taskName, image string, env map[string]string) string {
	h := sha256.New()
	h.Write([]byte(image))
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k + "=" + env[k] + ";"))
	}
	return fmt.Sprintf("%s-tmpl-%x", taskName, h.Sum(nil)[:templateDigestBytes])
}

// taskTemplatePattern matches every ActorTemplate name taskTemplateName can produce
// for the given task, across all spec revisions.
func taskTemplatePattern(taskName string) *regexp.Regexp {
	return regexp.MustCompile(fmt.Sprintf("^%s-tmpl-[0-9a-f]{%d}$", regexp.QuoteMeta(taskName), 2*templateDigestBytes))
}

// ReconcileDelete cleans up Substrate resources when a Task is deleted: the actor,
// which shares the task's name, then every ActorTemplate provisioned for the task.
func (r *TaskReconciler) ReconcileDelete(ctx context.Context, atespace, taskName string) error {
	if atespace == "" {
		atespace = "default"
	}
	slog.Info("deleting Substrate actor for task", "atespace", atespace, "task", taskName)
	if err := r.client.DeleteActor(ctx, atespace, taskName); err != nil {
		return err
	}
	if err := r.deleteTaskTemplates(ctx, atespace, taskName); err != nil {
		slog.Warn("could not clean up actor templates for task", "task", taskName, "error", err)
	}
	return nil
}

// deleteTaskTemplates removes all ActorTemplates belonging to the task. Templates
// accumulate across spec revisions, so this matches by name pattern rather than
// recomputing a single digest. If an actor deletion is still finishing in Substrate,
// template deletion may briefly return Aborted, so we retry with backoff.
func (r *TaskReconciler) deleteTaskTemplates(ctx context.Context, atespace, taskName string) error {
	templates, err := r.client.ListActorTemplates(ctx, atespace)
	if err != nil {
		return err
	}
	pattern := taskTemplatePattern(taskName)
	var errs []error
	for _, tmpl := range templates {
		name := tmpl.GetMetadata().GetName()
		if !pattern.MatchString(name) {
			continue
		}
		slog.Info("deleting Substrate actor template for task", "atespace", atespace, "task", taskName, "template", name)
		var delErr error
		for attempt := 0; attempt < 5; attempt++ {
			delErr = r.client.DeleteActorTemplate(ctx, atespace, name)
			if delErr == nil || status.Code(delErr) == codes.NotFound {
				delErr = nil
				break
			}
			select {
			case <-ctx.Done():
				delErr = ctx.Err()
				break
			case <-time.After(500 * time.Millisecond):
			}
		}
		if delErr != nil {
			errs = append(errs, delErr)
		}
	}
	return errors.Join(errs...)
}
