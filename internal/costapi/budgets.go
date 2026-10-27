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

package costapi

import (
	"context"
	"strings"
)

// Budget states.
const (
	BudgetOK        = "OK"
	BudgetWarn      = "WARN"
	BudgetExhausted = "EXHAUSTED"
)

// Budget scopes.
const (
	BudgetScopeAgent  = "agent"
	BudgetScopeTenant = "tenant"
	BudgetScopeGroup  = "group"
)

// BudgetsReport is the response of GET /v1/budgets.
type BudgetsReport struct {
	// Mode is "enforce" or "observe-only". In observe-only the Cost Service
	// reports states but returns no 429 and adds no holds.
	Mode        string        `json:"mode" yaml:"mode"`
	GeneratedAt string        `json:"generatedAt,omitempty" yaml:"generatedAt,omitempty"`
	Budgets     []BudgetState `json:"budgets" yaml:"budgets"`
	Holds       []BudgetHold  `json:"holds,omitempty" yaml:"holds,omitempty"`
}

// BudgetState is one budget and its spend in the current period.
type BudgetState struct {
	// ID is "agent:<atespace>/<name>", "tenant:<tenant>", or
	// "group:<atespace>/<root task>".
	ID     string `json:"id" yaml:"id"`
	Scope  string `json:"scope" yaml:"scope"`
	Target string `json:"target" yaml:"target"`
	// Period is "monthly", "daily", or "tree" for a group, which lasts as
	// long as its task tree.
	Period             string   `json:"period,omitempty" yaml:"period,omitempty"`
	PeriodStart        string   `json:"periodStart,omitempty" yaml:"periodStart,omitempty"`
	State              string   `json:"state" yaml:"state"`
	LimitUSD           float64  `json:"limitUsd" yaml:"limitUsd"`
	WarnAt             float64  `json:"warnAt,omitempty" yaml:"warnAt,omitempty"`
	ClosedSpendUSD     float64  `json:"closedSpendUsd" yaml:"closedSpendUsd"`
	InFlightComputeUSD float64  `json:"inFlightComputeUsd" yaml:"inFlightComputeUsd"`
	TotalSpendUSD      float64  `json:"totalSpendUsd" yaml:"totalSpendUsd"`
	Estimated          bool     `json:"estimated" yaml:"estimated"`
	HeldActors         []string `json:"heldActors,omitempty" yaml:"heldActors,omitempty"`
}

// BudgetHold is a hold that the Cost Service placed on an actor.
type BudgetHold struct {
	// Actor is "<atespace>/<name>".
	Actor     string `json:"actor" yaml:"actor"`
	Name      string `json:"name" yaml:"name"`
	Reason    string `json:"reason,omitempty" yaml:"reason,omitempty"`
	CreatedBy string `json:"createdBy,omitempty" yaml:"createdBy,omitempty"`
	CreatedAt string `json:"createdAt,omitempty" yaml:"createdAt,omitempty"`
}

// GetBudgets fetches GET /v1/budgets. A non-empty atespace asks the server
// for that atespace only. It returns the decoded report and the body as the
// server sent it.
func (c *Client) GetBudgets(ctx context.Context, atespace string) (*BudgetsReport, []byte, error) {
	var report BudgetsReport
	raw, err := c.getJSON(ctx, "/v1/budgets", atespaceQuery(atespace), &report)
	if err != nil {
		return nil, nil, err
	}
	return &report, raw, nil
}

// GroupBudgetID returns the ID of the group budget that Ax stamps with
// agent-cost/budget-group: <atespace>/<task>.
func GroupBudgetID(group string) string { return BudgetScopeGroup + ":" + group }

// AgentBudgetID returns the ID of an admin-owned agent budget.
func AgentBudgetID(atespace, name string) string {
	return BudgetScopeAgent + ":" + atespace + "/" + name
}

// BudgetsFor returns the budgets that cover an actor: its group budget, when
// group is set, and its agent budget. Tenant budgets are not matched: the
// tenant of an actor is known only to the Cost Service.
func (r *BudgetsReport) BudgetsFor(atespace, name, group string) []BudgetState {
	if r == nil {
		return nil
	}
	var out []BudgetState
	actor := atespace + "/" + name
	for _, b := range r.Budgets {
		switch {
		case group != "" && b.ID == GroupBudgetID(group):
			out = append(out, b)
		case b.ID == AgentBudgetID(atespace, name):
			out = append(out, b)
		default:
			for _, h := range b.HeldActors {
				if h == actor && b.Scope == BudgetScopeTenant {
					out = append(out, b)
					break
				}
			}
		}
	}
	return out
}

// HoldsFor returns the Cost Service holds on an actor.
func (r *BudgetsReport) HoldsFor(atespace, name string) []BudgetHold {
	if r == nil {
		return nil
	}
	var out []BudgetHold
	actor := atespace + "/" + name
	for _, h := range r.Holds {
		if strings.EqualFold(h.Actor, actor) {
			out = append(out, h)
		}
	}
	return out
}
