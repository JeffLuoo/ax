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

package budget

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

func task(name, parent string, maxUSD, warnAt float64, scope string) *v1alpha1.Task {
	t := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: "team"},
		Spec:     &v1alpha1.TaskSpec{Parent: parent},
		Status:   &v1alpha1.TaskStatus{Id: "id-" + name},
	}
	if parent != "" {
		t.Status.ParentId = "id-" + parent
	}
	if maxUSD > 0 || warnAt > 0 || scope != "" {
		t.Spec.Budget = &v1alpha1.Budget{MaxUsd: maxUSD, WarnAt: warnAt, Scope: scope}
	}
	return t
}

func lookupOf(tasks ...*v1alpha1.Task) Lookup {
	m := map[string]*v1alpha1.Task{}
	for _, t := range tasks {
		m[t.GetMetadata().GetName()] = t
	}
	return func(_ context.Context, _, name string) (*v1alpha1.Task, error) {
		if t, ok := m[name]; ok {
			return t, nil
		}
		return nil, errors.New("not found")
	}
}

func TestResolve(t *testing.T) {
	root := task("root", "", 5, 0.75, "")
	child := task("child", "root", 0, 0, "")
	grand := task("grand", "child", 1, 0, "")
	lookup := lookupOf(root, child, grand)

	tests := []struct {
		name string
		task *v1alpha1.Task
		want Labels
	}{
		{"root carries limit and warn-at", root, Labels{
			Set: map[string]string{LabelGroup: "root", LabelLimitUSD: "5", LabelWarnAt: "0.75"},
		}},
		{"child carries the group only", child, Labels{
			Set:    map[string]string{LabelGroup: "root"},
			Delete: []string{LabelLimitUSD, LabelWarnAt},
		}},
		{"outermost tree budget wins over a nested one", grand, Labels{
			Set:    map[string]string{LabelGroup: "root"},
			Delete: []string{LabelLimitUSD, LabelWarnAt},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Resolve(context.Background(), lookup, tc.task)
			if !reflect.DeepEqual(got.Set, tc.want.Set) || !reflect.DeepEqual(got.Delete, tc.want.Delete) {
				t.Errorf("Resolve() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestResolve_NoBudgetDeletesAll(t *testing.T) {
	root := task("root", "", 0, 0, "")
	got := Resolve(context.Background(), lookupOf(root), root)
	if len(got.Set) != 0 || !reflect.DeepEqual(got.Delete, Keys) {
		t.Errorf("Resolve() = %+v, want only deletes", got)
	}
}

func TestResolve_TaskScope(t *testing.T) {
	root := task("root", "", 3, 0, v1alpha1.BudgetScopeTask)
	child := task("child", "root", 0, 0, "")
	sub := task("sub", "root", 2, 0, v1alpha1.BudgetScopeTree)
	subChild := task("sub-child", "sub", 0, 0, "")
	lookup := lookupOf(root, child, sub, subChild)

	if g := Resolve(context.Background(), lookup, root).Group("team"); g != "team/root" {
		t.Errorf("root group = %q, want team/root", g)
	}
	if g := Resolve(context.Background(), lookup, child).GroupName(); g != "" {
		t.Errorf("a Task budget covered the child: group %q", g)
	}
	if g := Resolve(context.Background(), lookup, subChild).GroupName(); g != "sub" {
		t.Errorf("sub-child group = %q, want sub", g)
	}
	if l := Resolve(context.Background(), lookup, sub).Set[LabelLimitUSD]; l != "2" {
		t.Errorf("sub limit = %q, want 2", l)
	}
}

func TestAncestors_StopsAtRecreatedParent(t *testing.T) {
	root := task("root", "", 5, 0, "")
	child := task("child", "root", 0, 0, "")
	child.Status.ParentId = "id-old-root"
	if got := Ancestors(context.Background(), lookupOf(root, child), child); len(got) != 1 {
		t.Errorf("Ancestors() has %d tasks, want only the child", len(got))
	}
}

func TestAncestors_StopsOnCycle(t *testing.T) {
	a := task("a", "b", 0, 0, "")
	b := task("b", "a", 0, 0, "")
	if got := Ancestors(context.Background(), lookupOf(a, b), a); len(got) != 2 {
		t.Errorf("Ancestors() has %d tasks, want 2", len(got))
	}
}

func TestDiff(t *testing.T) {
	want := Labels{
		Set:    map[string]string{LabelGroup: "root", LabelLimitUSD: "10"},
		Delete: []string{LabelWarnAt},
	}
	set, del := want.Diff(map[string]string{LabelGroup: "root", LabelLimitUSD: "5", LabelWarnAt: "0.8", "other": "x"})
	if !reflect.DeepEqual(set, map[string]string{LabelLimitUSD: "10"}) {
		t.Errorf("set = %v", set)
	}
	if !reflect.DeepEqual(del, []string{LabelWarnAt}) {
		t.Errorf("del = %v", del)
	}
	set, del = want.Diff(map[string]string{LabelGroup: "root", LabelLimitUSD: "10"})
	if set != nil || del != nil {
		t.Errorf("up-to-date actor got set=%v del=%v", set, del)
	}
}

func TestDescendants(t *testing.T) {
	root := task("root", "", 5, 0, "")
	child := task("child", "root", 0, 0, "")
	grand := task("grand", "child", 0, 0, "")
	orphan := task("orphan", "root", 0, 0, "")
	orphan.Status.ParentId = "id-old-root"
	other := task("other", "", 0, 0, "")
	got := Descendants(root, []*v1alpha1.Task{root, child, grand, orphan, other})
	var names []string
	for _, t := range got {
		names = append(names, t.GetMetadata().GetName())
	}
	if !reflect.DeepEqual(names, []string{"child", "grand"}) {
		t.Errorf("Descendants() = %v, want [child grand]", names)
	}
}

func TestFormatUSD(t *testing.T) {
	for in, want := range map[float64]string{5: "5", 2.5: "2.5", 0.1: "0.1", 12.345: "12.345"} {
		if got := FormatUSD(in); got != want {
			t.Errorf("FormatUSD(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestGroupID(t *testing.T) {
	if got := GroupID("team", "root"); got != "team/root" {
		t.Errorf("GroupID = %q", got)
	}
	if got := GroupID("", "root"); got != "default/root" {
		t.Errorf("GroupID with no atespace = %q", got)
	}
	if got := GroupID("team", ""); got != "" {
		t.Errorf("GroupID with no name = %q", got)
	}
}
