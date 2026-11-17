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
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/ax/internal/apivalidation"
	"github.com/google/ax/internal/store"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxForks bounds the forks that one ForkTask call creates.
const maxForks = 20

// ForkTask suspends a task, tags its snapshot, and creates one task for each
// name in fork_names from that snapshot. Each fork runs on the source task's
// ActorTemplate and starts where the source task was. The source task stays
// suspended.
func (s *Server) ForkTask(ctx context.Context, req *v1alpha1.ForkTaskRequest) (*v1alpha1.ForkTaskResponse, error) {
	if req == nil || req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	names := req.GetForkNames()
	if len(names) == 0 || len(names) > maxForks {
		return nil, status.Errorf(codes.InvalidArgument, "fork_names must have 1 to %d names", maxForks)
	}
	if slices.Contains(names, req.GetName()) {
		return nil, status.Errorf(codes.InvalidArgument, "fork name %q is the source task", req.GetName())
	}
	seen := make(map[string]bool)
	for _, n := range names {
		if seen[n] {
			return nil, status.Errorf(codes.InvalidArgument, "fork name %q is listed twice", n)
		}
		seen[n] = true
	}
	atespace := req.GetAtespace()
	if atespace == "" {
		atespace = v1alpha1.DefaultAtespace
	}
	tagName := req.GetTag()
	if tagName == "" {
		tagName = fmt.Sprintf("%s-%d", req.GetName(), time.Now().Unix())
	}

	unlock, err := s.locker.Lock(ctx, "task", atespace, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking task %s/%s: %v", atespace, req.GetName(), err)
	}
	defer unlock()

	source, err := s.getTask(ctx, atespace, req.GetName())
	if errors.Is(err, store.ErrNotFound) {
		return nil, status.Errorf(codes.NotFound, "task %q not found in atespace %q", req.GetName(), atespace)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "getting task: %v", err)
	}
	if source.GetStatus().GetAteActorTemplate() == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "task %q has no ActorTemplate yet", req.GetName())
	}
	for _, n := range names {
		if _, err := s.getStoredTask(ctx, atespace, n); err == nil {
			return nil, status.Errorf(codes.AlreadyExists, "task %q already exists in atespace %q", n, atespace)
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.Internal, "checking task %q: %v", n, err)
		}
	}

	template, err := s.reconciler.Checkpoint(ctx, source, tagName)
	if err != nil {
		if substrate.IsTransientError(err) {
			return nil, status.Errorf(codes.Unavailable, "checkpointing task: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "checkpointing task: %v", err)
	}

	workspace := s.fetchWorkspace(ctx, atespace, source)
	gateway := s.fetchGateway(ctx, atespace, source)
	resp := &v1alpha1.ForkTaskResponse{Tag: tagName}
	for _, n := range names {
		fork, err := s.createFork(ctx, source, n, tagName, template, req.GetResume(), workspace, gateway)
		if err != nil {
			return nil, err
		}
		resp.Forks = append(resp.Forks, fork)
	}
	return resp, nil
}

// createFork stores a task named name that starts from tagName, creates its
// actor from the tag, and resumes it when resume is set.
func (s *Server) createFork(ctx context.Context, source *v1alpha1.Task, name, tagName, template string, resume bool, ws *v1alpha1.Workspace, gw *v1alpha1.Gateway) (*v1alpha1.Task, error) {
	atespace := source.GetMetadata().GetAtespace()
	unlock, err := s.locker.Lock(ctx, "task", atespace, name)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "locking task %s/%s: %v", atespace, name, err)
	}
	defer unlock()

	now := timestamppb.Now()
	uid := uuid.NewString()
	spec := proto.Clone(source.GetSpec()).(*v1alpha1.TaskSpec)
	spec.SessionId = uid
	spec.ForkSource = &v1alpha1.ForkSource{Task: source.GetMetadata().GetName(), Tag: tagName}
	fork := &v1alpha1.Task{
		ApiVersion: source.GetApiVersion(),
		Kind:       source.GetKind(),
		Metadata: &v1alpha1.ObjectMeta{
			Name:       name,
			Atespace:   atespace,
			Uid:        uid,
			CreateTime: now,
			UpdateTime: now,
		},
		Spec: spec,
		Status: &v1alpha1.TaskStatus{
			State:            v1alpha1.TaskState_TASK_STATE_QUEUED,
			Attempt:          1,
			Generation:       1,
			AteActorTemplate: template,
			ResolvedArgv:     slices.Clone(source.GetStatus().GetResolvedArgv()),
		},
	}
	// The snapshot already holds the source task's workspace, so a fork does
	// not wait for workspace setup again.
	for _, c := range source.GetStatus().GetConditions() {
		if c.GetType() == v1alpha1.ConditionWorkspaceReady {
			fork.Status.Conditions = append(fork.Status.Conditions, proto.Clone(c).(*v1alpha1.Condition))
		}
	}
	if err := apivalidation.ValidateTask(fork); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "fork %q: %v", name, err)
	}
	if err := s.saveTask(ctx, fork); err != nil {
		return nil, status.Errorf(codes.Internal, "saving fork %q: %v", name, err)
	}
	if err := s.reconciler.Suspend(ctx, fork, ws, gw); err != nil {
		return nil, s.recordFailure(ctx, fork, "provisioning fork", err)
	}
	if resume {
		if err := s.reconciler.Resume(ctx, fork, ws, gw); err != nil {
			return nil, s.recordFailure(ctx, fork, "resuming fork", err)
		}
	}
	if err := s.clearStoredTaskState(ctx, fork); err != nil {
		return nil, err
	}
	s.applyActor(ctx, fork)
	return fork, nil
}
