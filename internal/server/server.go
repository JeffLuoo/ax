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
	"github.com/google/ax/internal/events"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Reconciler coordinates sandbox/actor lifecycles on Agent Substrate directly.
type Reconciler interface {
	Reconcile(ctx context.Context, task *v1alpha1.Task, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error)
	ReconcileDelete(ctx context.Context, atespace, taskName string) error
}

// Options configures the AX API Server.
type Options struct {
	Locker     lock.Locker
	Reconciler Reconciler
	// Events receives a task lifecycle event for every create, phase change,
	// delete, and outcome. Nil sends nothing.
	Events events.Emitter
	// Guard authenticates and authorizes every RPC. Nil allows every caller,
	// and tasks then have no status.created_by.
	Guard *auth.Guard
}

// Server provides the gRPC API for AX.
type Server struct {
	v1alpha1.UnimplementedAXServer
	store      store.Store
	locker     lock.Locker
	reconciler Reconciler
	events     events.Emitter
	grpcServer *grpc.Server
}

// NewServer creates a new AX API server.
func NewServer(s store.Store, opts ...Options) *Server {
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

	srv := &Server{
		store:      s,
		locker:     locker,
		reconciler: opt.Reconciler,
		events:     emitter,
		grpcServer: grpc.NewServer(serverOpts...),
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
	task, err := s.store.GetTask(ctx, atespace, req.Name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", req.Name, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	return task, nil
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
	tasks, err := s.store.ListTasks(ctx, atespace, limit, offset)
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

	existing, err := s.store.GetTask(ctx, atespace, taskName)
	if err == nil {
		if existing.GetStatus().GetPhase() != "Pending" || !proto.Equal(existing.GetSpec(), task.GetSpec()) {
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
		p, err := s.store.GetTask(ctx, atespace, parent)
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
	task.Status.Phase = "Suspended"
	if err := s.store.SaveTask(ctx, task); err != nil {
		return nil, status.Errorf(codes.Internal, "saving task: %v", err)
	}

	// Directly reconcile with Substrate
	if s.reconciler != nil {
		workspaces := s.fetchWorkspaces(ctx, atespace, task)
		reconciled, err := s.reconciler.Reconcile(ctx, task, workspaces...)
		if err != nil {
			slog.Error("direct reconcile error on create task", "task", taskName, "error", err)
			if reconciled != nil && reconciled.Status != nil {
				task.Status = reconciled.Status
			}
			if substrate.IsTransientError(err) {
				task.Status.Phase = "Pending"
			} else if task.Status.Phase != "Pending" {
				task.Status.Phase = "Failed"
			}
			_ = s.store.UpdateTaskStatus(ctx, atespace, taskName, task.Status)
			s.events.TaskChanged(ctx, task)
			if substrate.IsTransientError(err) {
				return nil, status.Errorf(codes.Unavailable, "provisioning task on substrate: %v", err)
			}
			return nil, status.Errorf(codes.Internal, "provisioning task on substrate: %v", err)
		}
		task.Status = reconciled.Status
		if err := s.store.UpdateTaskStatus(ctx, atespace, taskName, task.Status); err != nil {
			return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
		}
	}

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

	task, err := s.store.GetTask(ctx, atespace, taskName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", taskName, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}

	// Mark the task as terminating while actor cleanup is in progress.
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = v1alpha1.PhaseTerminating
	if err := s.store.UpdateTaskStatus(ctx, atespace, taskName, task.Status); err != nil {
		return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
	}
	s.events.TaskChanged(ctx, task)

	// Directly clean up Substrate actor and templates
	if s.reconciler != nil {
		if err := s.reconciler.ReconcileDelete(ctx, atespace, taskName); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, status.Error(codes.Canceled, err.Error())
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, status.Error(codes.DeadlineExceeded, err.Error())
			}
			if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
				return nil, err
			}
			return nil, status.Errorf(codes.Internal, "cleaning up substrate resources for task %s/%s: %v", atespace, taskName, err)
		}
	}

	if err := s.store.DeleteTask(ctx, atespace, taskName); err != nil {
		return nil, status.Errorf(codes.Internal, "deleting task record: %v", err)
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

	task, err := s.store.GetTask(ctx, atespace, taskName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", taskName, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = "Suspended"

	if s.reconciler != nil {
		workspaces := s.fetchWorkspaces(ctx, atespace, task)
		reconciled, err := s.reconciler.Reconcile(ctx, task, workspaces...)
		if err != nil {
			if substrate.IsTransientError(err) {
				return nil, status.Errorf(codes.Unavailable, "suspending task on substrate: %v", err)
			}
			return nil, status.Errorf(codes.Internal, "suspending task on substrate: %v", err)
		}
		task.Status = reconciled.Status
		if err := s.store.UpdateTaskStatus(ctx, atespace, taskName, task.Status); err != nil {
			return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
		}
	} else {
		if err := s.store.SaveTask(ctx, task); err != nil {
			return nil, status.Errorf(codes.Internal, "suspending task: %v", err)
		}
	}

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

	task, err := s.store.GetTask(ctx, atespace, taskName)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", taskName, atespace)
		}
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = "Running"

	if s.reconciler != nil {
		workspaces := s.fetchWorkspaces(ctx, atespace, task)
		reconciled, err := s.reconciler.Reconcile(ctx, task, workspaces...)
		if err != nil {
			if reconciled != nil && reconciled.Status != nil {
				task.Status = reconciled.Status
			}
			if substrate.IsTransientError(err) {
				task.Status.Phase = "Pending"
			} else if task.Status.Phase != "Pending" {
				task.Status.Phase = "Failed"
			}
			_ = s.store.UpdateTaskStatus(ctx, atespace, taskName, task.Status)
			s.events.TaskChanged(ctx, task)
			if substrate.IsTransientError(err) {
				return nil, status.Errorf(codes.Unavailable, "resuming task on substrate: %v", err)
			}
			return nil, status.Errorf(codes.Internal, "resuming task on substrate: %v", err)
		}
		task.Status = reconciled.Status
		if err := s.store.UpdateTaskStatus(ctx, atespace, taskName, task.Status); err != nil {
			return nil, status.Errorf(codes.Internal, "updating task status: %v", err)
		}
	} else {
		if err := s.store.SaveTask(ctx, task); err != nil {
			return nil, status.Errorf(codes.Internal, "resuming task: %v", err)
		}
	}

	s.events.TaskChanged(ctx, task)
	return task, nil
}

func (s *Server) fetchWorkspaces(ctx context.Context, atespace string, task *v1alpha1.Task) []*v1alpha1.Workspace {
	var workspaces []*v1alpha1.Workspace
	if task.Spec == nil {
		return workspaces
	}
	for _, ref := range task.Spec.WorkspaceRefs() {
		if ref.Name == "" {
			continue
		}
		if wsp, err := s.store.GetWorkspace(ctx, atespace, ref.Name); err == nil {
			workspaces = append(workspaces, wsp)
		}
	}
	return workspaces
}

func (s *Server) WatchTask(req *v1alpha1.WatchTaskRequest, stream grpc.ServerStreamingServer[v1alpha1.WatchTaskResponse]) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "missing request")
	}
	atespace := req.Atespace
	if atespace == "" {
		atespace = "default"
	}
	ctx := stream.Context()
	ch, closer, err := s.store.WatchTask(ctx, atespace, req.Name)
	if err != nil {
		return status.Errorf(codes.Internal, "watching task: %v", err)
	}
	defer closer.Close()

	if initial, err := s.store.GetTask(ctx, atespace, req.Name); err == nil {
		if err := stream.Send(&v1alpha1.WatchTaskResponse{Task: initial, Action: "INITIAL"}); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&v1alpha1.WatchTaskResponse{Task: task, Action: "MODIFIED"}); err != nil {
				return err
			}
			if task.Status != nil && (task.Status.Phase == "Failed" || task.Status.Phase == "Completed") {
				return nil
			}
		}
	}
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
