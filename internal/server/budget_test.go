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

package server_test

import (
	"context"
	"testing"

	"github.com/google/ax/internal/budget"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newBudgetServer returns a server on an in-memory Substrate whose reconciler
// finds the budget of a task tree in the server's store.
func newBudgetServer(t *testing.T) (*server.Server, *memory.MemoryStore, *substratetest.Server) {
	t.Helper()
	st := memory.NewStore()
	sub := substratetest.NewServer(t)
	rec := newTestReconciler(sub)
	rec.Tasks = st.GetTask
	return server.NewServer(st, rec), st, sub
}

func createTree(t *testing.T, srv *server.Server, specs ...[2]string) {
	t.Helper()
	for _, s := range specs {
		if _, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: s[0]},
			Spec:     &v1alpha1.TaskSpec{Image: "alpine", Parent: s[1]},
		}}); err != nil {
			t.Fatalf("CreateTask %s: %v", s[0], err)
		}
	}
}

// actorLabels returns the labels of the named actor in atespace default.
func actorLabels(t *testing.T, sub *substratetest.Server, name string) map[string]string {
	t.Helper()
	a, ok := sub.Actor("default", name)
	if !ok {
		t.Fatalf("actor %s not found", name)
	}
	return a.GetLabels()
}

// holdActor puts a hold on the named actor in atespace default, as the Cost
// Service does when a budget is exhausted.
func holdActor(t *testing.T, sub *substratetest.Server, name string) {
	t.Helper()
	a, ok := sub.Actor("default", name)
	if !ok {
		t.Fatalf("actor %s not found", name)
	}
	if a.Status == nil {
		a.Status = &ateapipb.ActorStatus{}
	}
	a.Status.Holds = append(a.Status.Holds, &ateapipb.ActorHold{Name: "agent-cost/budget", Reason: "group default/root exhausted"})
	sub.SetActor(a)
}

func TestUpdateTaskBudget_SyncsTheSubtree(t *testing.T) {
	srv, st, sub := newBudgetServer(t)
	createTree(t, srv, [2]string{"root", ""}, [2]string{"child", "root"}, [2]string{"grand", "child"}, [2]string{"other", ""})

	got, err := srv.UpdateTaskBudget(context.Background(), &v1alpha1.UpdateTaskBudgetRequest{
		Name: "root", Budget: &v1alpha1.Budget{MaxUsd: 10, WarnAt: 0.8},
	})
	if err != nil {
		t.Fatalf("UpdateTaskBudget: %v", err)
	}
	if got.GetSpec().GetBudget().GetMaxUsd() != 10 || got.GetSpec().GetBudget().GetWarnAt() != 0.8 {
		t.Errorf("spec.budget = %v", got.GetSpec().GetBudget())
	}
	if got.GetStatus().GetBudgetGroup() != "default/root" {
		t.Errorf("status.budgetGroup = %q", got.GetStatus().GetBudgetGroup())
	}
	if got.GetStatus().GetPhase() != "Suspended" {
		t.Errorf("status.phase = %q, want the actor's phase Suspended", got.GetStatus().GetPhase())
	}
	root := actorLabels(t, sub, "root")
	if root[budget.LabelGroup] != "root" || root[budget.LabelLimitUSD] != "10" || root[budget.LabelWarnAt] != "0.8" {
		t.Errorf("root actor labels = %v, want the group, the limit, and warn-at", root)
	}
	for _, name := range []string{"child", "grand"} {
		if l := actorLabels(t, sub, name); l[budget.LabelGroup] != "root" || l[budget.LabelLimitUSD] != "" {
			t.Errorf("%s actor labels = %v, want group root only", name, l)
		}
	}
	if l := actorLabels(t, sub, "other"); l[budget.LabelGroup] != "" {
		t.Errorf("other actor labels = %v, want no budget group", l)
	}
	stored, err := st.GetTask(context.Background(), "default", "root")
	if err != nil || stored.GetSpec().GetBudget().GetMaxUsd() != 10 {
		t.Errorf("stored budget = %v, %v", stored.GetSpec().GetBudget(), err)
	}
	// The actor owns the phase, so the update stores none.
	if stored.GetStatus().GetPhase() != "" {
		t.Errorf("stored phase = %q, want none", stored.GetStatus().GetPhase())
	}
	if read, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "grand"}); err != nil || read.GetStatus().GetBudgetGroup() != "default/root" {
		t.Errorf("GetTask grand budgetGroup = %q, %v, want default/root", read.GetStatus().GetBudgetGroup(), err)
	}

	// Clearing removes the budget, and the labels of the whole subtree.
	got, err = srv.UpdateTaskBudget(context.Background(), &v1alpha1.UpdateTaskBudgetRequest{Name: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSpec().GetBudget() != nil {
		t.Errorf("budget after clear = %v", got.GetSpec().GetBudget())
	}
	for _, name := range []string{"root", "child", "grand"} {
		if l := actorLabels(t, sub, name); len(l) != 0 {
			t.Errorf("%s actor labels after clear = %v, want none", name, l)
		}
	}
}

func TestUpdateTaskBudget_Validates(t *testing.T) {
	srv, _, _ := newBudgetServer(t)
	createTree(t, srv, [2]string{"root", ""})
	for _, b := range []*v1alpha1.Budget{{MaxUsd: -1}, {MaxUsd: 1, WarnAt: 1.5}, {MaxUsd: 1, Scope: "Forest"}} {
		if _, err := srv.UpdateTaskBudget(context.Background(), &v1alpha1.UpdateTaskBudgetRequest{Name: "root", Budget: b}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("budget %v: err = %v, want InvalidArgument", b, err)
		}
	}
	if _, err := srv.UpdateTaskBudget(context.Background(), &v1alpha1.UpdateTaskBudgetRequest{Name: "missing", Budget: &v1alpha1.Budget{MaxUsd: 1}}); status.Code(err) != codes.NotFound {
		t.Errorf("missing task: err = %v, want NotFound", err)
	}
}

func TestGetAndListTasks_ShowLiveHolds(t *testing.T) {
	srv, st, sub := newBudgetServer(t)
	createTree(t, srv, [2]string{"root", ""})
	holdActor(t, sub, "root")

	task, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.GetStatus().GetHolds()) != 1 || task.GetStatus().GetHolds()[0].GetName() != "agent-cost/budget" {
		t.Errorf("GetTask holds = %v", task.GetStatus().GetHolds())
	}
	if c := readyCondition(task); c.GetStatus() != "False" || c.GetReason() != "Held" {
		t.Errorf("GetTask Ready = %v, want False with reason Held", c)
	}
	list, err := srv.ListTasks(context.Background(), &v1alpha1.ListTasksRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetTasks()) != 1 || len(list.GetTasks()[0].GetStatus().GetHolds()) != 1 {
		t.Errorf("ListTasks holds = %v", list.GetTasks())
	}
	// Holds change without AX, so the store does not keep them.
	stored, err := st.GetTask(context.Background(), "default", "root")
	if err != nil || len(stored.GetStatus().GetHolds()) != 0 {
		t.Errorf("stored holds = %v, %v, want none", stored.GetStatus().GetHolds(), err)
	}
}

func TestResumeTask_HeldIsFailedPrecondition(t *testing.T) {
	rec := &recordingEmitter{}
	st := memory.NewStore()
	sub := substratetest.NewServer(t)
	srv := server.NewServer(st, newTestReconciler(sub), server.Options{Events: rec})
	createTree(t, srv, [2]string{"root", ""})
	holdActor(t, sub, "root")

	_, err := srv.ResumeTask(context.Background(), &v1alpha1.ResumeTaskRequest{Name: "root"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ResumeTask err = %v, want FailedPrecondition", err)
	}
	task, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if task.GetStatus().GetPhase() != "Suspended" {
		t.Errorf("phase after a held resume = %q, want Suspended", task.GetStatus().GetPhase())
	}
	if c := readyCondition(task); c.GetReason() != "Held" {
		t.Errorf("Ready after a held resume = %v, want reason Held", c)
	}
	if phases := rec.phases(); len(phases) != 2 || phases[1] != "Suspended" {
		t.Errorf("event phases = %v, want the create and the held resume, both Suspended", phases)
	}
}

// TestListTasks_ReadsHoldsAndBudgetGroupsFromActors checks that ListTasks
// fills in the holds and the budget group of each task from its actor, with
// the same actor read that gives the phase.
func TestListTasks_ReadsHoldsAndBudgetGroupsFromActors(t *testing.T) {
	srv, _, sub := newBudgetServer(t)
	if _, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "root"},
		Spec:     &v1alpha1.TaskSpec{Image: "alpine", Budget: &v1alpha1.Budget{MaxUsd: 2}},
	}}); err != nil {
		t.Fatal(err)
	}
	createTree(t, srv, [2]string{"child", "root"})
	holdActor(t, sub, "child")

	list, err := srv.ListTasks(context.Background(), &v1alpha1.ListTasksRequest{Atespace: "default"})
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*v1alpha1.Task{}
	for _, task := range list.GetTasks() {
		byName[task.GetMetadata().GetName()] = task
	}
	for _, name := range []string{"root", "child"} {
		if got := byName[name].GetStatus().GetBudgetGroup(); got != "default/root" {
			t.Errorf("%s budgetGroup = %q, want default/root", name, got)
		}
	}
	if holds := byName["child"].GetStatus().GetHolds(); len(holds) != 1 || holds[0].GetName() != "agent-cost/budget" {
		t.Errorf("child holds = %v", holds)
	}
	if holds := byName["root"].GetStatus().GetHolds(); len(holds) != 0 {
		t.Errorf("root holds = %v, want none", holds)
	}
}
