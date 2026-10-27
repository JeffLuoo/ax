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
	"reflect"
	"sort"
	"testing"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// budgetReconciler records budget syncs and reports a fixed hold.
type budgetReconciler struct {
	fakeReconciler
	synced       []string
	syncedBudget map[string]float64
	holds        map[string][]*v1alpha1.ActorHold
	refreshed    int
}

func (b *budgetReconciler) SyncBudgetLabels(_ context.Context, task *v1alpha1.Task) (*v1alpha1.Task, error) {
	name := task.GetMetadata().GetName()
	b.synced = append(b.synced, name)
	if b.syncedBudget == nil {
		b.syncedBudget = map[string]float64{}
	}
	b.syncedBudget[name] = task.GetSpec().GetBudget().GetMaxUsd()
	task.Status.BudgetGroup = "default/root"
	return task, nil
}

func (b *budgetReconciler) RefreshActorState(_ context.Context, tasks []*v1alpha1.Task) {
	b.refreshed++
	for _, t := range tasks {
		t.Status.Holds = b.holds[t.GetMetadata().GetName()]
	}
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

func TestUpdateTaskBudget_SyncsTheSubtree(t *testing.T) {
	rec := &budgetReconciler{}
	st := memory.NewStore()
	srv := server.NewServer(st, server.Options{Reconciler: rec})
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
	if rec.syncedBudget["root"] != 10 {
		t.Errorf("the root was synced with budget %v, want the new one", rec.syncedBudget["root"])
	}
	synced := append([]string(nil), rec.synced...)
	sort.Strings(synced)
	if !reflect.DeepEqual(synced, []string{"child", "grand", "root"}) {
		t.Errorf("synced = %v, want root and its subtree only", synced)
	}
	stored, err := st.GetTask(context.Background(), "default", "root")
	if err != nil || stored.GetSpec().GetBudget().GetMaxUsd() != 10 {
		t.Errorf("stored budget = %v, %v", stored.GetSpec().GetBudget(), err)
	}

	// Clearing removes the budget.
	got, err = srv.UpdateTaskBudget(context.Background(), &v1alpha1.UpdateTaskBudgetRequest{Name: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSpec().GetBudget() != nil {
		t.Errorf("budget after clear = %v", got.GetSpec().GetBudget())
	}
}

func TestUpdateTaskBudget_Validates(t *testing.T) {
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: &budgetReconciler{}})
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
	hold := []*v1alpha1.ActorHold{{Name: "agent-cost/budget", Reason: "group default/root exhausted"}}
	rec := &budgetReconciler{holds: map[string][]*v1alpha1.ActorHold{"root": hold}}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: rec})
	createTree(t, srv, [2]string{"root", ""})

	task, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if len(task.GetStatus().GetHolds()) != 1 || task.GetStatus().GetHolds()[0].GetName() != "agent-cost/budget" {
		t.Errorf("GetTask holds = %v", task.GetStatus().GetHolds())
	}
	list, err := srv.ListTasks(context.Background(), &v1alpha1.ListTasksRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetTasks()) != 1 || len(list.GetTasks()[0].GetStatus().GetHolds()) != 1 {
		t.Errorf("ListTasks holds = %v", list.GetTasks())
	}
	if rec.refreshed != 2 {
		t.Errorf("refreshes = %d, want 2", rec.refreshed)
	}
}

func TestResumeTask_HeldIsFailedPrecondition(t *testing.T) {
	rec := &budgetReconciler{}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: rec})
	createTree(t, srv, [2]string{"root", ""})

	rec.reconcileErr = &substrate.HeldError{Hold: "agent-cost/budget", Reason: "exhausted"}
	rec.reconcilePhase = "Suspended"
	_, err := srv.ResumeTask(context.Background(), &v1alpha1.ResumeTaskRequest{Name: "root"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ResumeTask err = %v, want FailedPrecondition", err)
	}
	rec.reconcileErr = nil
	task, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: "root"})
	if err != nil {
		t.Fatal(err)
	}
	if task.GetStatus().GetPhase() != "Suspended" {
		t.Errorf("phase after a held resume = %q, want Suspended", task.GetStatus().GetPhase())
	}
}
