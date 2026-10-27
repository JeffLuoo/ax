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

// Package budget turns task budgets into the agent-cost/budget-* actor labels
// that the Cost Service reads from Substrate. Ax computes no spend: the Cost
// Service sums it per budget group and holds the actors.
package budget

import (
	"context"
	"sort"
	"strconv"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// Actor label keys. Only Ax sets them; users never edit them.
const (
	// LabelGroup is on every actor that a budget covers: the name of the
	// task that owns the budget. Substrate label values cannot hold "/", and
	// a task tree never leaves its atespace, so the Cost Service keys the
	// group by the actor's atespace and this name (group:<atespace>/<task>).
	LabelGroup = "agent-cost/budget-group"
	// LabelLimitUSD is on the owner task's actor only: the limit in USD.
	LabelLimitUSD = "agent-cost/budget-limit-usd"
	// LabelWarnAt is on the owner task's actor only, when the budget sets
	// warnAt: the fraction of the limit at which the budget turns WARN.
	LabelWarnAt = "agent-cost/warn-at"
)

// Keys lists every label key that this package manages.
var Keys = []string{LabelGroup, LabelLimitUSD, LabelWarnAt}

// maxDepth bounds the walk up spec.parent, so a corrupt store with a cycle
// cannot loop.
const maxDepth = 64

// Lookup reads a task from the store. It returns an error when the task does
// not exist.
type Lookup func(ctx context.Context, atespace, name string) (*v1alpha1.Task, error)

// Ancestors returns the task followed by its ancestors, up to the root. It
// stops at a parent that is missing, or that was made again under the same
// name after the child was created (status.parentId no longer matches).
func Ancestors(ctx context.Context, lookup Lookup, task *v1alpha1.Task) []*v1alpha1.Task {
	chain := []*v1alpha1.Task{task}
	if lookup == nil {
		return chain
	}
	atespace := task.GetMetadata().GetAtespace()
	seen := map[string]bool{task.GetMetadata().GetName(): true}
	cur := task
	for len(chain) < maxDepth {
		parent := cur.GetSpec().GetParent()
		if parent == "" || seen[parent] {
			break
		}
		p, err := lookup(ctx, atespace, parent)
		if err != nil || p == nil {
			break
		}
		if want := cur.GetStatus().GetParentId(); want != "" && p.GetStatus().GetId() != want {
			break
		}
		seen[parent] = true
		chain = append(chain, p)
		cur = p
	}
	return chain
}

// HasLimit reports whether the task declares a budget with a limit.
func HasLimit(t *v1alpha1.Task) bool { return t.GetSpec().GetBudget().GetMaxUsd() > 0 }

func treeScoped(t *v1alpha1.Task) bool {
	s := t.GetSpec().GetBudget().GetScope()
	return s == "" || s == v1alpha1.BudgetScopeTree
}

// Owner returns the task whose budget covers the task, or nil. An actor
// carries one budget group, so the outermost Tree budget in the chain wins;
// a Task budget applies only when no Tree budget above covers the task.
// Admin-owned agent and tenant budgets in Cost Service config still apply on
// top of it.
func Owner(chain []*v1alpha1.Task) *v1alpha1.Task {
	for i := len(chain) - 1; i >= 0; i-- {
		if HasLimit(chain[i]) && treeScoped(chain[i]) {
			return chain[i]
		}
	}
	if len(chain) > 0 && HasLimit(chain[0]) {
		return chain[0]
	}
	return nil
}

// Labels is the budget label state that one actor should have.
type Labels struct {
	// Set holds the keys the actor must carry, with their values.
	Set map[string]string
	// Delete holds the managed keys the actor must not carry.
	Delete []string
}

// GroupName returns the value of LabelGroup, or "" when no budget covers the
// actor.
func (l Labels) GroupName() string { return l.Set[LabelGroup] }

// Group returns the budget group as "<atespace>/<owner task>", or "" when no
// budget covers the actor.
func (l Labels) Group(atespace string) string { return GroupID(atespace, l.GroupName()) }

// GroupID joins an atespace and a LabelGroup value into "<atespace>/<task>".
// It returns "" for an empty name.
func GroupID(atespace, name string) string {
	if name == "" {
		return ""
	}
	if atespace == "" {
		atespace = "default"
	}
	return atespace + "/" + name
}

// Desired returns the budget labels for task's actor, given its owner from
// Owner.
func Desired(task, owner *v1alpha1.Task) Labels {
	l := Labels{Set: map[string]string{}}
	if owner != nil {
		l.Set[LabelGroup] = owner.GetMetadata().GetName()
		if sameTask(owner, task) {
			b := owner.GetSpec().GetBudget()
			l.Set[LabelLimitUSD] = FormatUSD(b.GetMaxUsd())
			if b.GetWarnAt() > 0 {
				l.Set[LabelWarnAt] = strconv.FormatFloat(b.GetWarnAt(), 'f', -1, 64)
			}
		}
	}
	for _, k := range Keys {
		if _, ok := l.Set[k]; !ok {
			l.Delete = append(l.Delete, k)
		}
	}
	return l
}

func sameTask(a, b *v1alpha1.Task) bool {
	return a.GetMetadata().GetAtespace() == b.GetMetadata().GetAtespace() &&
		a.GetMetadata().GetName() == b.GetMetadata().GetName()
}

// FormatUSD renders a limit as the shortest decimal that parses back to it.
func FormatUSD(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

// Resolve walks the task's ancestors and returns its desired labels.
func Resolve(ctx context.Context, lookup Lookup, task *v1alpha1.Task) Labels {
	return Desired(task, Owner(Ancestors(ctx, lookup, task)))
}

// Diff returns the changes that bring current to the desired labels: the
// keys to set (missing or with another value) and the managed keys to delete
// (present on the actor). Both are empty when the actor is up to date.
func (l Labels) Diff(current map[string]string) (set map[string]string, del []string) {
	for k, v := range l.Set {
		if cur, ok := current[k]; !ok || cur != v {
			if set == nil {
				set = map[string]string{}
			}
			set[k] = v
		}
	}
	for _, k := range l.Delete {
		if _, ok := current[k]; ok {
			del = append(del, k)
		}
	}
	sort.Strings(del)
	return set, del
}

// Descendants returns every task under root in tasks, linked by spec.parent
// and status.parentId, in breadth-first order. tasks must all be in root's
// atespace.
func Descendants(root *v1alpha1.Task, tasks []*v1alpha1.Task) []*v1alpha1.Task {
	children := map[string][]*v1alpha1.Task{}
	for _, t := range tasks {
		if p := t.GetSpec().GetParent(); p != "" {
			children[p] = append(children[p], t)
		}
	}
	var out []*v1alpha1.Task
	seen := map[string]bool{root.GetMetadata().GetName(): true}
	queue := []*v1alpha1.Task{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, c := range children[cur.GetMetadata().GetName()] {
			name := c.GetMetadata().GetName()
			if seen[name] {
				continue
			}
			if want := c.GetStatus().GetParentId(); want != "" && want != cur.GetStatus().GetId() {
				continue
			}
			seen[name] = true
			out = append(out, c)
			queue = append(queue, c)
		}
	}
	return out
}
