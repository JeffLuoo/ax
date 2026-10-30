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

package controller_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// labelControlServer is a Substrate fake that keeps actor labels and holds.
type labelControlServer struct {
	ateapipb.UnimplementedControlServer

	mu         sync.Mutex
	actors     map[string]*ateapipb.Actor
	createReqs []*ateapipb.CreateActorRequest
	labelReqs  []*ateapipb.SetActorLabelsRequest
}

func (s *labelControlServer) CreateAtespace(context.Context, *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	return &ateapipb.Atespace{}, nil
}

func (s *labelControlServer) GetActorTemplate(context.Context, *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	return nil, status.Error(codes.NotFound, "no template")
}

func (s *labelControlServer) CreateActorTemplate(_ context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	return req.GetActorTemplate(), nil
}

func (s *labelControlServer) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createReqs = append(s.createReqs, req)
	name := req.GetActor().GetMetadata().GetName()
	if _, ok := s.actors[name]; ok {
		return nil, status.Error(codes.AlreadyExists, "exists")
	}
	a := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Name: name, Atespace: req.GetActor().GetMetadata().GetAtespace(), Uid: "uid-" + name},
		ActorTemplate: req.GetActor().GetActorTemplate(),
		Labels:        req.GetActor().GetLabels(),
		Status:        &ateapipb.ActorStatus{State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED},
	}
	s.actors[name] = a
	return a, nil
}

func (s *labelControlServer) GetActor(_ context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.actors[req.GetActor().GetName()]; ok {
		return a, nil
	}
	return nil, status.Error(codes.NotFound, "no actor")
}

func (s *labelControlServer) SetActorLabels(_ context.Context, req *ateapipb.SetActorLabelsRequest) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.labelReqs = append(s.labelReqs, req)
	a, ok := s.actors[req.GetActor().GetName()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no actor")
	}
	if a.Labels == nil {
		a.Labels = map[string]string{}
	}
	for k, v := range req.GetLabels() {
		a.Labels[k] = v
	}
	for _, k := range req.GetDeleteKeys() {
		delete(a.Labels, k)
	}
	return a, nil
}

func (s *labelControlServer) SuspendActor(context.Context, *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	return &ateapipb.SuspendActorResponse{}, nil
}

func (s *labelControlServer) ResumeActor(_ context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.actors[req.GetActor().GetName()]
	if holds := a.GetStatus().GetHolds(); len(holds) > 0 {
		st, _ := status.New(codes.FailedPrecondition, "actor is held").WithDetails(&errdetails.ErrorInfo{
			Reason:   substrate.ActorHeldReason,
			Domain:   "ate.dev",
			Metadata: map[string]string{"hold": holds[0].GetName(), "reason": holds[0].GetReason()},
		})
		return nil, st.Err()
	}
	return &ateapipb.ResumeActorResponse{Actor: a, Resumed: true}, nil
}

func newLabelReconciler(t *testing.T, tasks ...*v1alpha1.Task) (*controller.TaskReconciler, *labelControlServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fake := &labelControlServer{actors: map[string]*ateapipb.Actor{}}
	srv := grpc.NewServer()
	ateapipb.RegisterControlServer(srv, fake)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	client, err := substrate.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	r := controller.NewTaskReconciler(client, "test-template", v1alpha1.DefaultSystemNamespace, "")
	r.WorkspaceReadyTimeout = 50 * time.Millisecond
	byName := map[string]*v1alpha1.Task{}
	for _, task := range tasks {
		byName[task.GetMetadata().GetName()] = task
	}
	r.Tasks = func(_ context.Context, _, name string) (*v1alpha1.Task, error) {
		if task, ok := byName[name]; ok {
			return task, nil
		}
		return nil, errors.New("not found")
	}
	return r, fake
}

func budgetTask(name, parent string, maxUSD float64) *v1alpha1.Task {
	t := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "team"},
		Spec:     &v1alpha1.TaskSpec{Parent: parent, Image: "img"},
		Status:   &v1alpha1.TaskStatus{Id: "id-" + name, Phase: "Suspended"},
	}
	if parent != "" {
		t.Status.ParentId = "id-" + parent
	}
	if maxUSD > 0 {
		t.Spec.Budget = &v1alpha1.Budget{MaxUsd: maxUSD, WarnAt: 0.5}
	}
	return t
}

func TestReconcile_StampsBudgetLabelsOnCreate(t *testing.T) {
	root := budgetTask("root", "", 5)
	child := budgetTask("child", "root", 0)
	r, fake := newLabelReconciler(t, root, child)

	if _, err := r.Reconcile(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}
	got, err := r.Reconcile(context.Background(), child, nil)
	if err != nil {
		t.Fatal(err)
	}
	wantRoot := map[string]string{budget.LabelGroup: "root", budget.LabelLimitUSD: "5", budget.LabelWarnAt: "0.5"}
	if l := fake.createReqs[0].GetActor().GetLabels(); !reflect.DeepEqual(l, wantRoot) {
		t.Errorf("root CreateActor labels = %v, want %v", l, wantRoot)
	}
	wantChild := map[string]string{budget.LabelGroup: "root"}
	if l := fake.createReqs[1].GetActor().GetLabels(); !reflect.DeepEqual(l, wantChild) {
		t.Errorf("child CreateActor labels = %v, want %v", l, wantChild)
	}
	if got.GetStatus().GetBudgetGroup() != "team/root" {
		t.Errorf("child status.budgetGroup = %q", got.GetStatus().GetBudgetGroup())
	}
	if len(fake.labelReqs) != 0 {
		t.Errorf("new actors with correct labels got %d SetActorLabels calls", len(fake.labelReqs))
	}
}

func TestReconcile_SyncsLabelsOfExistingActor(t *testing.T) {
	root := budgetTask("root", "", 5)
	r, fake := newLabelReconciler(t, root)
	if _, err := r.Reconcile(context.Background(), root, nil); err != nil {
		t.Fatal(err)
	}

	// Raising the limit updates the root label in place.
	root.Spec.Budget = &v1alpha1.Budget{MaxUsd: 10}
	if _, err := r.SyncBudgetLabels(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if len(fake.labelReqs) != 1 {
		t.Fatalf("SetActorLabels calls = %d, want 1", len(fake.labelReqs))
	}
	req := fake.labelReqs[0]
	if !reflect.DeepEqual(req.GetLabels(), map[string]string{budget.LabelLimitUSD: "10"}) || !reflect.DeepEqual(req.GetDeleteKeys(), []string{budget.LabelWarnAt}) {
		t.Errorf("SetActorLabels = set %v delete %v", req.GetLabels(), req.GetDeleteKeys())
	}

	// Removing the budget deletes every managed key.
	root.Spec.Budget = nil
	got, err := r.SyncBudgetLabels(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if l := fake.actors["root"].GetLabels(); len(l) != 0 {
		t.Errorf("labels after removing the budget = %v", l)
	}
	if got.GetStatus().GetBudgetGroup() != "" {
		t.Errorf("status.budgetGroup = %q, want empty", got.GetStatus().GetBudgetGroup())
	}

	// An up-to-date actor gets no call.
	n := len(fake.labelReqs)
	if _, err := r.SyncBudgetLabels(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	if len(fake.labelReqs) != n {
		t.Error("an up-to-date actor got a SetActorLabels call")
	}
}

func TestReconcile_HeldResumeKeepsTaskSuspended(t *testing.T) {
	task := budgetTask("worker", "", 1)
	r, fake := newLabelReconciler(t, task)
	if _, err := r.Reconcile(context.Background(), task, nil); err != nil {
		t.Fatal(err)
	}
	fake.actors["worker"].Status.Holds = []*ateapipb.ActorHold{{
		Name: "agent-cost/budget", Reason: "group team/worker exhausted", CreatedBy: "agent-cost", CreatedAt: timestamppb.Now(),
	}}

	task.Status.Phase = "Running"
	got, err := r.Reconcile(context.Background(), task, nil)
	held, ok := substrate.AsHeld(err)
	if !ok {
		t.Fatalf("Reconcile error = %v, want a hold", err)
	}
	if held.Hold != "agent-cost/budget" {
		t.Errorf("hold = %q", held.Hold)
	}
	if got.Status.Phase != "Suspended" {
		t.Errorf("phase = %q, want Suspended", got.Status.Phase)
	}
	if len(got.Status.Holds) != 1 || got.Status.Holds[0].GetName() != "agent-cost/budget" || got.Status.Holds[0].GetCreatedBy() != "agent-cost" {
		t.Errorf("status.holds = %v", got.Status.Holds)
	}
	assertCondition(t, got, "Ready", "False", "Held")
}
