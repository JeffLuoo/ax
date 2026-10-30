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

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/ax/internal/auth"
	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/events"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// reasonHeld is the Ready reason of a suspended task whose actor has a hold.
// The reconciler sets the same reason when Substrate refuses a resume because
// of a hold.
const reasonHeld = "Held"

// Options configures the AX API Server.
type Options struct {
	// Locker serializes changes to a resource. It must be shared by all
	// ax-server replicas; it defaults to an in-process lock.MemoryLocker.
	Locker lock.Locker
	// Events receives a task lifecycle event for every create, phase change,
	// delete, and outcome. Nil sends nothing.
	Events events.Emitter
	// Guard authenticates and authorizes every RPC. Nil allows every caller,
	// and tasks then have no status.created_by.
	Guard *auth.Guard
	// RouterAddr is the atenet router's address: a URL, or a host:port where
	// port 443 means https. When it is set, SuspendTask and DeleteTask ask the
	// runner of a Running task for its command state before they act, and
	// record the outcome of a command that exited. An empty RouterAddr turns
	// this check off.
	RouterAddr string
	// RouterClient sends that request; nil means
	// controller.NewRouterHTTPClient, which has a 2 s timeout and the
	// router's mTLS credentials.
	RouterClient *http.Client
}

// Server provides the gRPC API for AX.
type Server struct {
	v1alpha1.UnimplementedAXServer
	store      store.Store
	locker     lock.Locker
	reconciler *controller.TaskReconciler
	// substrate is the reconciler's client. It is read for the parts of a
	// task's status owned by its actor, which the store never holds.
	substrate  *substrate.Client
	events     events.Emitter
	grpcServer *grpc.Server

	routerAddr   string
	routerClient *http.Client
}

// NewServer creates a new AX API server.
func NewServer(s store.Store, reconciler *controller.TaskReconciler, opts ...Options) *Server {
	if reconciler == nil || reconciler.Client() == nil {
		panic("server: NewServer requires a non-nil TaskReconciler with a Substrate client")
	}
	var opt Options
	if len(opts) > 0 {
		opt = opts[0]
	}
	locker := opt.Locker
	if locker == nil {
		locker = lock.NewMemoryLocker()
	}

	emitter := opt.Events
	if emitter == nil {
		emitter = events.Nop{}
	}

	var serverOpts []grpc.ServerOption
	if opt.Guard != nil {
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(opt.Guard.Unary), grpc.ChainStreamInterceptor(opt.Guard.Stream))
	}

	routerClient := opt.RouterClient
	if routerClient == nil {
		routerClient = controller.NewRouterHTTPClient()
	}

	srv := &Server{
		store:        s,
		locker:       locker,
		reconciler:   reconciler,
		substrate:    reconciler.Client(),
		events:       emitter,
		grpcServer:   grpc.NewServer(serverOpts...),
		routerAddr:   opt.RouterAddr,
		routerClient: routerClient,
	}
	v1alpha1.RegisterAXServer(srv.grpcServer, srv)
	return srv
}

// GRPCServer returns the underlying gRPC server instance.
func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

// Handler returns the HTTP handler for the server, routing gRPC and HTTP health checks.
func (s *Server) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			s.grpcServer.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.NotFound(w, r)
	})
}

// newTaskID returns a unique ID for a new task. The random part makes the IDs
// of two tasks with the same name different, also when they are created in the
// same second.
func newTaskID(name string) string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("task-%s-%d-%s", name, time.Now().Unix(), hex.EncodeToString(b))
}

// --- gRPC AXServer implementation ---

// --- Tasks ---

func (s *Server) GetTask(ctx context.Context, req *v1alpha1.GetTaskRequest) (*v1alpha1.Task, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	task, err := s.getTask(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	return task, nil
}

// UpdateTaskBudget replaces a task's spec.budget and syncs the budget labels
// of its actor and of every actor under it. Raising the limit is how a user
// lets a held tree run again: the Cost Service sees the new limit label and
// removes its agent-cost/budget holds.
func (s *Server) UpdateTaskBudget(ctx context.Context, req *v1alpha1.UpdateTaskBudgetRequest) (*v1alpha1.Task, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	if err := v1alpha1.ValidateBudget(req.GetBudget()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	taskName := req.GetName()

	task, err := s.updateTaskBudget(ctx, atespace, taskName, req.GetBudget())
	if err != nil {
		return nil, err
	}
	s.syncSubtreeBudgetLabels(ctx, task)
	return task, nil
}

// updateTaskBudget stores the new budget of one task and syncs the budget
// labels of its actor, under the task's lock. It returns a gRPC status error.
func (s *Server) updateTaskBudget(ctx context.Context, atespace, taskName string, b *v1alpha1.Budget) (*v1alpha1.Task, error) {
	unlock, err := s.locker.Lock(ctx, "task", atespace, taskName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking task %s/%s: %v", atespace, taskName, err)
	}
	defer unlock()

	// The stored task: the budget sync below needs only what AX owns.
	task, err := s.storedTask(ctx, atespace, taskName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", taskName, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	if task.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
		return nil, status.Errorf(codes.FailedPrecondition, "task %s/%s is being deleted", atespace, taskName)
	}
	if task.Spec == nil {
		task.Spec = &v1alpha1.TaskSpec{}
	}
	task.Spec.Budget = nil
	if b.GetMaxUsd() > 0 || b.GetWarnAt() > 0 || b.GetScope() != "" {
		task.Spec.Budget = proto.Clone(b).(*v1alpha1.Budget)
	}
	if err := s.saveTask(ctx, task); err != nil {
		return nil, status.Errorf(codes.Internal, "saving task: %v", err)
	}
	if synced, err := s.reconciler.SyncBudgetLabels(ctx, task); err != nil {
		slog.Warn("could not sync budget labels", "task", atespace+"/"+taskName, "error", err)
	} else {
		task.Status = synced.Status
		if err := s.updateTask(ctx, atespace, taskName, task.Status); err != nil {
			slog.Warn("could not store task status after a budget sync", "task", atespace+"/"+taskName, "error", err)
		}
	}

	// Report the actor as it is now, as GetTask would.
	s.applyActor(ctx, task)
	s.events.TaskChanged(ctx, task)
	return task, nil
}

// syncSubtreeBudgetLabels syncs the budget labels of every task under root,
// each under its own lock. A task whose lock is busy is skipped: the RPC that
// holds it reconciles the task, which syncs its labels.
func (s *Server) syncSubtreeBudgetLabels(ctx context.Context, root *v1alpha1.Task) {
	atespace := root.GetMetadata().GetAtespace()
	var all []*v1alpha1.Task
	const pageSize = 500
	for offset := int64(0); ; offset += pageSize {
		page, err := s.storedTasks(ctx, atespace, pageSize, offset)
		if err != nil {
			slog.Warn("listing tasks for a budget sync", "atespace", atespace, "error", err)
			return
		}
		all = append(all, page...)
		if int64(len(page)) < pageSize {
			break
		}
	}
	for _, d := range budget.Descendants(root, all) {
		name := d.GetMetadata().GetName()
		lockCtx, cancel := context.WithTimeout(ctx, defaultResyncLockTimeout)
		unlock, err := s.locker.Lock(lockCtx, "task", atespace, name)
		cancel()
		if err != nil {
			slog.Warn("skipping a locked task in a budget sync", "task", atespace+"/"+name, "error", err)
			continue
		}
		t, err := s.storedTask(ctx, atespace, name)
		if err == nil {
			if synced, err := s.reconciler.SyncBudgetLabels(ctx, t); err != nil {
				slog.Warn("could not sync budget labels", "task", atespace+"/"+name, "error", err)
			} else if err := s.updateTask(ctx, atespace, name, synced.Status); err != nil {
				slog.Warn("could not store task status after a budget sync", "task", atespace+"/"+name, "error", err)
			}
		}
		unlock()
	}
}

func (s *Server) ListTasks(ctx context.Context, req *v1alpha1.ListTasksRequest) (*v1alpha1.ListTasksResponse, error) {
	atespace := ""
	limit := int64(50)
	offset := int64(0)
	if req != nil {
		atespace = req.Atespace
		if req.Limit > 0 {
			limit = req.Limit
		}
		if req.Offset >= 0 {
			offset = req.Offset
		}
	}
	tasks, err := s.listTasks(ctx, atespace, limit, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing tasks: %v", err)
	}
	return &v1alpha1.ListTasksResponse{Tasks: tasks}, nil
}

func (s *Server) CreateTask(ctx context.Context, req *v1alpha1.CreateTaskRequest) (*v1alpha1.Task, error) {
	if req == nil || req.Task == nil {
		return nil, status.Error(codes.InvalidArgument, "task required")
	}
	task := req.Task
	// The server owns the status. A caller cannot set an outcome, an exit
	// code, a creator, or a parent ID on create.
	task.Status = nil
	if err := v1alpha1.ValidateTask(task); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if task.Metadata == nil {
		task.Metadata = &v1alpha1.ObjectMeta{}
	}
	atespace := task.Metadata.Atespace
	if atespace == "" {
		atespace = "default"
		task.Metadata.Atespace = atespace
	}
	taskName := task.Metadata.GetName()

	// Acquire exclusive lock for this task
	unlock, err := s.locker.Lock(ctx, "task", atespace, taskName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking task %s/%s: %v", atespace, taskName, err)
	}
	defer unlock()

	existing, err := s.getTask(ctx, atespace, taskName)
	if err == nil {
		if existing.GetStatus().GetPhase() != v1alpha1.PhasePending || !proto.Equal(existing.GetSpec(), task.GetSpec()) {
			return nil, status.Errorf(codes.FailedPrecondition, "task %s/%s already exists and is immutable", atespace, taskName)
		}
		task = existing
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.Internal, "checking existing task: %v", err)
	}

	// A parent is always in the task's own atespace, so a task tree never
	// crosses an atespace and cost rollups cannot move cost to another tenant.
	var parentID string
	if parent := task.GetSpec().GetParent(); parent != "" {
		p, err := s.storedTask(ctx, atespace, parent)
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.FailedPrecondition, "parent task %q not found in atespace %q", parent, atespace)
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "checking parent task: %v", err)
		}
		if p.GetStatus().GetPhase() == v1alpha1.PhaseTerminating {
			return nil, status.Errorf(codes.FailedPrecondition, "parent task %s/%s is being deleted", atespace, parent)
		}
		parentID = p.GetStatus().GetId()
	}

	if task.Metadata.CreateTime == nil {
		task.Metadata.CreateTime = timestamppb.Now()
	}
	if task.Status == nil {
		// A new task. A retry of a Pending create keeps the status of the
		// first attempt.
		task.Status = &v1alpha1.TaskStatus{Id: newTaskID(taskName), ParentId: parentID}
		if p, ok := auth.PrincipalFrom(ctx); ok {
			task.Status.CreatedBy = p.Username
		}
	}
	// The task is Pending until its actor exists. New tasks start suspended.
	task.Status.Phase = v1alpha1.PhasePending
	if err := s.saveTask(ctx, task); err != nil {
		return nil, status.Errorf(codes.Internal, "saving task: %v", err)
	}
	task.Status.Phase = v1alpha1.PhaseSuspended

	// Directly reconcile with Substrate
	reconciled, err := s.reconciler.Reconcile(ctx, task, s.fetchWorkspace(ctx, atespace, task))
	if err != nil {
		slog.Error("direct reconcile error on create task", "task", taskName, "error", err)
		if reconciled != nil && reconciled.Status != nil {
			task.Status = reconciled.Status
		}
		if substrate.IsTransientError(err) {
			task.Status.Phase = v1alpha1.PhasePending
		} else if task.Status.Phase != v1alpha1.PhasePending {
			task.Status.Phase = v1alpha1.PhaseFailed
		}
		_ = s.updateTask(ctx, atespace, taskName, task.Status)
		// The phase is Pending or Failed, which GetTask reports as stored.
		s.events.TaskChanged(ctx, task)
		if substrate.IsTransientError(err) {
			return nil, status.Errorf(codes.Unavailable, "provisioning task on substrate: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "provisioning task on substrate: %v", err)
	}
	task.Status = reconciled.Status
	if err := s.updateTask(ctx, atespace, taskName, task.Status); err != nil {
		return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
	}

	// Report the actor as it is now, as GetTask would.
	s.applyActor(ctx, task)
	s.events.TaskChanged(ctx, task)
	return task, nil
}

func (s *Server) DeleteTask(ctx context.Context, req *v1alpha1.DeleteTaskRequest) (*v1alpha1.DeleteTaskResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	taskName := req.Name

	// Acquire exclusive lock for this task
	unlock, err := s.locker.Lock(ctx, "task", atespace, taskName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking task %s/%s: %v", atespace, taskName, err)
	}
	defer unlock()

	task, err := s.getTask(ctx, atespace, taskName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", taskName, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}

	// The delete removes the runner, so read its outcome first.
	s.recordFinalOutcome(ctx, task)

	// Mark the task as terminating while actor cleanup is in progress.
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = v1alpha1.PhaseTerminating
	if err := s.updateTask(ctx, atespace, taskName, task.Status); err != nil {
		return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
	}
	s.events.TaskChanged(ctx, task)

	if err := s.deleteTask(ctx, atespace, taskName); err != nil {
		return nil, err
	}
	deleted := proto.Clone(task).(*v1alpha1.Task)
	deleted.Status.Phase = events.PhaseDeleted
	s.events.TaskChanged(ctx, deleted)
	return &v1alpha1.DeleteTaskResponse{}, nil
}

func (s *Server) SuspendTask(ctx context.Context, req *v1alpha1.SuspendTaskRequest) (*v1alpha1.Task, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	taskName := req.Name

	// Acquire exclusive lock for this task
	unlock, err := s.locker.Lock(ctx, "task", atespace, taskName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking task %s/%s: %v", atespace, taskName, err)
	}
	defer unlock()

	task, err := s.getTask(ctx, atespace, taskName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", taskName, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	// The poller does not ask a suspended task, so read the outcome first.
	s.recordFinalOutcome(ctx, task)
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = v1alpha1.PhaseSuspended

	reconciled, err := s.reconciler.Reconcile(ctx, task, s.fetchWorkspace(ctx, atespace, task))
	if err != nil {
		if substrate.IsTransientError(err) {
			return nil, status.Errorf(codes.Unavailable, "suspending task on substrate: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "suspending task on substrate: %v", err)
	}
	task.Status = reconciled.Status
	if err := s.updateTask(ctx, atespace, taskName, task.Status); err != nil {
		return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
	}

	// Report the actor as it is now, as GetTask would.
	s.applyActor(ctx, task)
	s.events.TaskChanged(ctx, task)
	return task, nil
}

func (s *Server) ResumeTask(ctx context.Context, req *v1alpha1.ResumeTaskRequest) (*v1alpha1.Task, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	taskName := req.Name

	// Acquire exclusive lock for this task
	unlock, err := s.locker.Lock(ctx, "task", atespace, taskName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking task %s/%s: %v", atespace, taskName, err)
	}
	defer unlock()

	task, err := s.getTask(ctx, atespace, taskName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", taskName, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = v1alpha1.PhaseRunning

	reconciled, err := s.reconciler.Reconcile(ctx, task, s.fetchWorkspace(ctx, atespace, task))
	if err != nil {
		if reconciled != nil && reconciled.Status != nil {
			task.Status = reconciled.Status
		}
		if held, ok := substrate.AsHeld(err); ok {
			// The reconciler left the task Suspended with a Held
			// condition; it resumes once the hold is removed.
			_ = s.updateTask(ctx, atespace, taskName, task.Status)
			s.applyActor(ctx, task)
			s.events.TaskChanged(ctx, task)
			return nil, status.Errorf(codes.FailedPrecondition, "task %s/%s cannot resume: %v", atespace, taskName, held)
		}
		if substrate.IsTransientError(err) {
			task.Status.Phase = v1alpha1.PhasePending
		} else if task.Status.Phase != v1alpha1.PhasePending {
			task.Status.Phase = v1alpha1.PhaseFailed
		}
		_ = s.updateTask(ctx, atespace, taskName, task.Status)
		// The phase is Pending or Failed, which GetTask reports as stored.
		s.events.TaskChanged(ctx, task)
		if substrate.IsTransientError(err) {
			return nil, status.Errorf(codes.Unavailable, "resuming task on substrate: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "resuming task on substrate: %v", err)
	}
	task.Status = reconciled.Status
	if err := s.updateTask(ctx, atespace, taskName, task.Status); err != nil {
		return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
	}

	// Report the actor as it is now, as GetTask would.
	s.applyActor(ctx, task)
	s.events.TaskChanged(ctx, task)
	return task, nil
}

// Task storage and Substrate.
//
// A task's actor on Agent Substrate is the source of truth for whether the task
// is running, where, whether it is healthy, and whether it is held. The store
// keeps only what AX itself knows.
//
// Handlers never call s.store for tasks to report them; they use the methods
// below. saveTask and updateTask drop the actor-owned status before writing.
// getTask and listTasks fill it in from Substrate. Task events carry the task
// as these methods report it.

// getTask returns a task with its Substrate-owned status filled in.
func (s *Server) getTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	task, err := s.store.GetTask(ctx, atespace, name)
	if err != nil {
		return nil, err
	}
	s.applyActor(ctx, task)
	return task, nil
}

// listTasks returns a page of tasks with their Substrate-owned status filled
// in, using one ListActors call per atespace.
func (s *Server) listTasks(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error) {
	tasks, err := s.store.ListTasks(ctx, atespace, limit, offset)
	if err != nil {
		return tasks, err
	}
	byAtespace := make(map[string][]*v1alpha1.Task)
	for _, t := range tasks {
		ns := t.GetMetadata().GetAtespace()
		byAtespace[ns] = append(byAtespace[ns], t)
	}
	for ns, group := range byAtespace {
		actors, err := s.substrate.ListActors(ctx, ns)
		byName := make(map[string]*ateapipb.Actor, len(actors))
		for _, a := range actors {
			byName[a.GetMetadata().GetName()] = a
		}
		for _, t := range group {
			setActorStatus(t, byName[t.GetMetadata().GetName()], err)
		}
	}
	return tasks, nil
}

// saveTask stores task without its Substrate-owned status. task is not modified.
//
// The stores save an empty phase as Pending. When the phase follows from the
// actor, the stored phase must stay empty, so saveTask then writes the status
// again.
func (s *Server) saveTask(ctx context.Context, task *v1alpha1.Task) error {
	stored := proto.Clone(task).(*v1alpha1.Task)
	stored.Status = storedStatus(task.Status)
	phase := stored.Status.GetPhase()
	if err := s.store.SaveTask(ctx, stored); err != nil {
		return err
	}
	if phase != "" {
		return nil
	}
	return s.updateTask(ctx, stored.GetMetadata().GetAtespace(), stored.GetMetadata().GetName(), task.Status)
}

// storedTask returns a task as the store keeps it, without its
// Substrate-owned status. Use it only to read what AX owns, such as the spec,
// the IDs, or a phase of AX's own; use getTask to report a task.
func (s *Server) storedTask(ctx context.Context, atespace, name string) (*v1alpha1.Task, error) {
	return s.store.GetTask(ctx, atespace, name)
}

// storedTasks returns a page of tasks as the store keeps them, without their
// Substrate-owned status. Callers that need the actors read them in bulk;
// use listTasks to report tasks.
func (s *Server) storedTasks(ctx context.Context, atespace string, limit, offset int64) ([]*v1alpha1.Task, error) {
	return s.store.ListTasks(ctx, atespace, limit, offset)
}

// updateTask stores a task's status without its Substrate-owned parts. st is
// not modified.
func (s *Server) updateTask(ctx context.Context, atespace, name string, st *v1alpha1.TaskStatus) error {
	return s.store.UpdateTaskStatus(ctx, atespace, name, storedStatus(st))
}

// deleteTask removes a task's actor and templates from Substrate, then its
// record from the store. It returns a gRPC status error.
func (s *Server) deleteTask(ctx context.Context, atespace, name string) error {
	if err := s.reconciler.ReconcileDelete(ctx, atespace, name); err != nil {
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, err.Error())
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, err.Error())
		}
		if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
			return err
		}
		return status.Errorf(codes.Internal, "cleaning up substrate resources for task %s/%s: %v", atespace, name, err)
	}
	if err := s.store.DeleteTask(ctx, atespace, name); err != nil {
		return status.Errorf(codes.Internal, "deleting task record: %v", err)
	}
	return nil
}

// applyActor fills in the Substrate-owned status of task from its actor, with
// one GetActor call.
func (s *Server) applyActor(ctx context.Context, task *v1alpha1.Task) {
	actor, err := s.substrate.GetActor(ctx, task.GetMetadata().GetAtespace(), task.GetMetadata().GetName())
	if status.Code(err) == codes.NotFound {
		actor, err = nil, nil
	}
	setActorStatus(task, actor, err)
}

// setActorStatus overlays the actor's state on the task's status: actor,
// workerIP, holds, budget group, and, unless the phase is one of AX's own, the
// phase and Ready condition. actor is nil when the task has no actor; err is
// set when the actor could not be looked up, in which case the task is
// reported with phase Unknown and keeps its stored budget group.
func setActorStatus(task *v1alpha1.Task, actor *ateapipb.Actor, err error) {
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	st := task.Status
	st.Actor = ""
	st.WorkerIp = ""
	st.Holds = nil

	switch {
	case err != nil:
		slog.Warn("could not read task actor", "atespace", task.GetMetadata().GetAtespace(), "task", task.GetMetadata().GetName(), "error", err)
		if !v1alpha1.IsAXPhase(st.Phase) {
			st.Phase = v1alpha1.PhaseUnknown
			setReady(task, "Unknown", v1alpha1.ReasonSubstrateUnavailable, err.Error())
		}
	case actor == nil:
		// A task that finished provisioning always has an actor, so a missing one
		// was removed outside AX.
		if !v1alpha1.IsAXPhase(st.Phase) {
			st.Phase = v1alpha1.PhaseFailed
			setReady(task, "False", v1alpha1.ReasonActorNotFound, "The task's actor no longer exists on Substrate")
		}
	default:
		st.Actor = actor.GetMetadata().GetName()
		st.WorkerIp = substrate.WorkerIP(actor)
		// Holds and the budget group label change without AX, so each call
		// reads them from the actor.
		st.Holds = substrate.Holds(actor)
		st.BudgetGroup = budget.GroupID(task.GetMetadata().GetAtespace(), actor.GetLabels()[budget.LabelGroup])
		if v1alpha1.IsAXPhase(st.Phase) {
			return
		}
		state := actor.GetStatus().GetState()
		st.Phase = actorPhase(state)
		switch st.Phase {
		case v1alpha1.PhaseRunning:
			if conditionTrue(task, v1alpha1.ConditionWorkspaceReady) {
				setReady(task, "True", v1alpha1.ReasonTaskRunning, "Task is running and its workspace is ready")
			} else {
				setReady(task, "False", v1alpha1.ReasonWorkspaceInitializing, "Waiting for workspace setup to complete")
			}
		case v1alpha1.PhaseUnknown:
			setReady(task, "Unknown", v1alpha1.PhaseUnknown, "Actor is in state "+state.String())
		default:
			// A hold keeps a suspended actor from a resume, so it is the
			// reason that the task is not ready.
			if st.Phase == v1alpha1.PhaseSuspended && len(st.Holds) > 0 {
				held := &substrate.HeldError{Hold: st.Holds[0].GetName(), Reason: st.Holds[0].GetReason()}
				setReady(task, "False", reasonHeld, held.Error())
				return
			}
			// The task is not ready, and its phase says why.
			message := actor.GetStatus().GetCrash().GetMessage()
			if message == "" {
				message = "Actor is " + strings.ToLower(strings.TrimPrefix(state.String(), "ACTOR_STATE_"))
			}
			setReady(task, "False", st.Phase, message)
		}
	}
}

// actorPhase maps an actor's state to the task's phase.
func actorPhase(state ateapipb.ActorState) string {
	switch state {
	case ateapipb.ActorState_ACTOR_STATE_RUNNING:
		return v1alpha1.PhaseRunning
	case ateapipb.ActorState_ACTOR_STATE_RESUMING, ateapipb.ActorState_ACTOR_STATE_REVERTING:
		return v1alpha1.PhaseResuming
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDING:
		return v1alpha1.PhaseSuspending
	case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
		return v1alpha1.PhaseSuspended
	case ateapipb.ActorState_ACTOR_STATE_CRASHED:
		return v1alpha1.PhaseFailed
	case ateapipb.ActorState_ACTOR_STATE_DELETING:
		return v1alpha1.PhaseTerminating
	default:
		return v1alpha1.PhaseUnknown
	}
}

// conditionTrue reports whether the task has the given condition with status
// True.
func conditionTrue(task *v1alpha1.Task, condType string) bool {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == condType {
			return c.GetStatus() == "True"
		}
	}
	return false
}

// setReady sets the task's Ready condition. Its update time changes only when
// the condition does, so that repeated reads report a stable timestamp.
func setReady(task *v1alpha1.Task, condStatus, reason, message string) {
	for _, c := range task.Status.Conditions {
		if c.GetType() != v1alpha1.ConditionReady {
			continue
		}
		if c.Status != condStatus || c.Reason != reason || c.Message != message {
			c.Status, c.Reason, c.Message = condStatus, reason, message
			c.UpdateTime = timestamppb.Now()
		}
		return
	}
	task.Status.Conditions = append(task.Status.Conditions, &v1alpha1.Condition{
		Type:       v1alpha1.ConditionReady,
		Status:     condStatus,
		Reason:     reason,
		Message:    message,
		UpdateTime: timestamppb.Now(),
	})
}

// storedStatus returns the part of a task's status that AX owns and stores. The
// actor, its worker, its holds, and the phase and Ready condition that follow
// from the actor's state are dropped. st is not modified.
func storedStatus(st *v1alpha1.TaskStatus) *v1alpha1.TaskStatus {
	out := &v1alpha1.TaskStatus{}
	if st != nil {
		out = proto.Clone(st).(*v1alpha1.TaskStatus)
	}
	out.Actor = ""
	out.WorkerIp = ""
	out.Holds = nil
	if !v1alpha1.IsAXPhase(out.Phase) {
		out.Phase = ""
		conds := out.Conditions[:0]
		for _, c := range out.Conditions {
			if c.GetType() != v1alpha1.ConditionReady {
				conds = append(conds, c)
			}
		}
		out.Conditions = conds
	}
	return out
}

// fetchWorkspace loads the Workspace the task binds, or nil when the task
// binds none or the Workspace does not exist.
func (s *Server) fetchWorkspace(ctx context.Context, atespace string, task *v1alpha1.Task) *v1alpha1.Workspace {
	name := task.GetSpec().GetWorkspace().GetName()
	if name == "" {
		return nil
	}
	ws, err := s.store.GetWorkspace(ctx, atespace, name)
	if err != nil {
		return nil
	}
	return ws
}

// --- Workspaces ---

func (s *Server) GetWorkspace(ctx context.Context, req *v1alpha1.GetWorkspaceRequest) (*v1alpha1.Workspace, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	ws, err := s.store.GetWorkspace(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "workspace %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting workspace: %v", err)
	}
	return ws, nil
}

func (s *Server) ListWorkspaces(ctx context.Context, req *v1alpha1.ListWorkspacesRequest) (*v1alpha1.ListWorkspacesResponse, error) {
	atespace := ""
	if req != nil {
		atespace = req.Atespace
	}
	workspaces, err := s.store.ListWorkspaces(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing workspaces: %v", err)
	}
	return &v1alpha1.ListWorkspacesResponse{Workspaces: workspaces}, nil
}

func (s *Server) UpdateWorkspace(ctx context.Context, req *v1alpha1.UpdateWorkspaceRequest) (*v1alpha1.Workspace, error) {
	if req == nil || req.Workspace == nil {
		return nil, status.Error(codes.InvalidArgument, "workspace required")
	}
	if err := v1alpha1.ValidateWorkspace(req.Workspace); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Workspace.Metadata = defaultMetadata(req.Workspace.Metadata, func(atespace, name string) *v1alpha1.ObjectMeta {
		existing, err := s.store.GetWorkspace(ctx, atespace, name)
		if err != nil {
			return nil
		}
		return existing.GetMetadata()
	})

	atespace := req.Workspace.Metadata.Atespace
	wsName := req.Workspace.Metadata.Name

	// Acquire exclusive lock for this workspace
	unlock, err := s.locker.Lock(ctx, "workspace", atespace, wsName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking workspace %s/%s: %v", atespace, wsName, err)
	}
	defer unlock()

	if err := s.store.SaveWorkspace(ctx, req.Workspace); err != nil {
		return nil, status.Errorf(codes.Internal, "saving workspace: %v", err)
	}
	return req.Workspace, nil
}

func (s *Server) DeleteWorkspace(ctx context.Context, req *v1alpha1.DeleteWorkspaceRequest) (*v1alpha1.DeleteWorkspaceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	wsName := req.Name

	// Acquire exclusive lock for this workspace
	unlock, err := s.locker.Lock(ctx, "workspace", atespace, wsName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking workspace %s/%s: %v", atespace, wsName, err)
	}
	defer unlock()

	if err := s.store.DeleteWorkspace(ctx, atespace, wsName); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting workspace: %v", err)
	}
	return &v1alpha1.DeleteWorkspaceResponse{}, nil
}

// --- Models ---

func (s *Server) GetModel(ctx context.Context, req *v1alpha1.GetModelRequest) (*v1alpha1.Model, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	model, err := s.store.GetModel(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "model %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting model: %v", err)
	}
	return model, nil
}

func (s *Server) ListModels(ctx context.Context, req *v1alpha1.ListModelsRequest) (*v1alpha1.ListModelsResponse, error) {
	atespace := ""
	if req != nil {
		atespace = req.Atespace
	}
	models, err := s.store.ListModels(ctx, atespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "listing models: %v", err)
	}
	return &v1alpha1.ListModelsResponse{Models: models}, nil
}

func (s *Server) UpdateModel(ctx context.Context, req *v1alpha1.UpdateModelRequest) (*v1alpha1.Model, error) {
	if req == nil || req.Model == nil {
		return nil, status.Error(codes.InvalidArgument, "model required")
	}
	if err := v1alpha1.ValidateModel(req.Model); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Model.Metadata = defaultMetadata(req.Model.Metadata, func(atespace, name string) *v1alpha1.ObjectMeta {
		existing, err := s.store.GetModel(ctx, atespace, name)
		if err != nil {
			return nil
		}
		return existing.GetMetadata()
	})

	atespace := req.Model.Metadata.Atespace
	modelName := req.Model.Metadata.Name

	// Acquire exclusive lock for this model
	unlock, err := s.locker.Lock(ctx, "model", atespace, modelName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking model %s/%s: %v", atespace, modelName, err)
	}
	defer unlock()

	if err := s.store.SaveModel(ctx, req.Model); err != nil {
		return nil, status.Errorf(codes.Internal, "saving model: %v", err)
	}
	return req.Model, nil
}

func (s *Server) DeleteModel(ctx context.Context, req *v1alpha1.DeleteModelRequest) (*v1alpha1.DeleteModelResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	modelName := req.Name

	// Acquire exclusive lock for this model
	unlock, err := s.locker.Lock(ctx, "model", atespace, modelName)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking model %s/%s: %v", atespace, modelName, err)
	}
	defer unlock()

	if err := s.store.DeleteModel(ctx, atespace, modelName); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting model: %v", err)
	}
	return &v1alpha1.DeleteModelResponse{}, nil
}

func defaultMetadata(meta *v1alpha1.ObjectMeta, existing func(atespace, name string) *v1alpha1.ObjectMeta) *v1alpha1.ObjectMeta {
	if meta == nil {
		meta = &v1alpha1.ObjectMeta{}
	}
	if meta.Atespace == "" {
		meta.Atespace = "default"
	}
	if meta.CreateTime == nil {
		var prev *v1alpha1.ObjectMeta
		if existing != nil {
			prev = existing(meta.Atespace, meta.Name)
		}
		if prev.GetCreateTime() != nil {
			meta.CreateTime = prev.GetCreateTime()
		} else {
			meta.CreateTime = timestamppb.Now()
		}
	}
	return meta
}
