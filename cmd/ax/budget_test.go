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

package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/ax/internal/costapi"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// budgetsBody is a GET /v1/budgets response in the shape the Cost Service
// serves.
const budgetsBody = `{
  "mode": "enforce",
  "generatedAt": "2026-10-27T19:00:00Z",
  "budgets": [
    {"id": "group:team/root", "scope": "group", "target": "team/root", "period": "tree",
     "state": "EXHAUSTED", "limitUsd": 5, "warnAt": 0.8, "closedSpendUsd": 4.9,
     "inFlightComputeUsd": 0.2, "totalSpendUsd": 5.1, "estimated": true,
     "heldActors": ["team/root", "team/child"]},
    {"id": "agent:team/solo", "scope": "agent", "target": "team/solo", "period": "monthly",
     "periodStart": "2026-10-01T00:00:00Z", "state": "WARN", "limitUsd": 10,
     "closedSpendUsd": 8.5, "inFlightComputeUsd": 0, "totalSpendUsd": 8.5, "estimated": true}
  ],
  "holds": [
    {"actor": "team/root", "name": "agent-cost/budget", "reason": "group:team/root exhausted",
     "createdBy": "system:serviceaccount:ate-cost:ate-cost", "createdAt": "2026-10-27T18:59:00Z"},
    {"actor": "team/child", "name": "agent-cost/budget", "reason": "group:team/root exhausted"}
  ]
}`

func budgetsServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/budgets" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(budgetsBody))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestParseBudgetArgs(t *testing.T) {
	opts, err := parseBudgetArgs([]string{"set", "task", "planner", "--max-usd", "$5", "--warn-at=0.8", "--scope", "task"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.action != "set" || opts.name != "planner" || opts.budget.GetMaxUsd() != 5 || opts.budget.GetWarnAt() != 0.8 || opts.budget.GetScope() != v1alpha1.BudgetScopeTask {
		t.Errorf("parseBudgetArgs = %+v", opts)
	}
	opts, err = parseBudgetArgs([]string{"clear", "planner"})
	if err != nil || opts.action != "clear" || opts.budget != nil {
		t.Errorf("clear = %+v, %v", opts, err)
	}
	for _, bad := range [][]string{
		{"set", "planner"},
		{"set", "planner", "--max-usd", "0"},
		{"set", "planner", "--max-usd", "5", "--warn-at", "2"},
		{"set", "planner", "--max-usd", "5", "--scope", "forest"},
		{"clear", "planner", "--max-usd", "5"},
		{"raise", "planner"},
		{"set"},
	} {
		if _, err := parseBudgetArgs(bad); err == nil {
			t.Errorf("parseBudgetArgs(%q) did not fail", bad)
		}
	}
}

func TestDescribeBudget(t *testing.T) {
	if got := describeBudget(&v1alpha1.Budget{MaxUsd: 5, WarnAt: 0.8}); got != "$5 (warn at 80%, scope Tree)" {
		t.Errorf("describeBudget = %q", got)
	}
	if got := describeBudget(nil); got != "none" {
		t.Errorf("describeBudget(nil) = %q", got)
	}
}

func TestCostBudgets_Table(t *testing.T) {
	srv := budgetsServer(t)
	out, err := captureStdout(t, func() error {
		return runCost(srv.URL, "", "default", false, []string{"budgets"})
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Mode: enforce",
		"BUDGET", "STATE", "group:team/root", "EXHAUSTED", "$5.100000", "102.0%",
		"agent:team/solo", "WARN", "monthly from 2026-10-01T00:00:00Z",
		"Holds:", "team/child", "agent-cost/budget",
		"ESTIMATE",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("ax cost budgets output lacks %q:\n%s", want, out)
		}
	}
	raw, err := captureStdout(t, func() error {
		return runCost(srv.URL, "", "default", false, []string{"budgets", "-o", "json"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw, `"heldActors"`) {
		t.Errorf("-o json is not the server's body:\n%s", raw)
	}
}

func TestPrintBudgetState(t *testing.T) {
	client := costapi.NewClient(budgetsServer(t).URL, nil)
	r := fetchBudgets(context.Background(), client, "team")
	if r == nil {
		t.Fatal("fetchBudgets returned nil")
	}
	var buf bytes.Buffer
	printBudgetState(&buf, r, "team", "child", "team/root")
	for _, want := range []string{"Budget State:", "EXHAUSTED", "of $5.000000 (102.0%)", "group:team/root", "Cost Service Hold:", "agent-cost/budget: group:team/root exhausted"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("printBudgetState lacks %q:\n%s", want, buf.String())
		}
	}
	buf.Reset()
	printBudgetState(&buf, r, "team", "unbudgeted", "")
	if buf.Len() != 0 {
		t.Errorf("an actor with no budget printed:\n%s", buf.String())
	}
	// A Cost Service without /v1/budgets gives no report and no output.
	notFound := httptest.NewServer(http.NotFoundHandler())
	defer notFound.Close()
	if fetchBudgets(context.Background(), costapi.NewClient(notFound.URL, nil), "") != nil {
		t.Error("fetchBudgets on a 404 returned a report")
	}
}

func TestPrintHolds(t *testing.T) {
	var buf bytes.Buffer
	printHolds(&buf, []*v1alpha1.ActorHold{{Name: "agent-cost/budget", Reason: "exhausted", CreatedBy: "agent-cost", CreatedAt: timestamppb.Now()}})
	for _, want := range []string{"Holds", "agent-cost/budget", "exhausted", "agent-cost"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("printHolds lacks %q:\n%s", want, buf.String())
		}
	}
	if holdNames(nil) != "<none>" || holdNames([]*v1alpha1.ActorHold{{Name: "a/b"}, {Name: "c/d"}}) != "a/b,c/d" {
		t.Error("holdNames")
	}
}

func TestParseGlobalArgs_CostServerFlags(t *testing.T) {
	for _, args := range [][]string{
		{"cost", "--cost-server", "http://c:9090", "--cost-namespace", "ns"},
		{"cost", "--cost-server=http://c:9090", "--cost-namespace=ns"},
		{"cost", "--finops-server", "http://c:9090", "--finops-namespace=ns"},
	} {
		g := parseGlobalArgs(args)
		if g.costServer != "http://c:9090" || g.costNamespace != "ns" || g.cmd != "cost" {
			t.Errorf("parseGlobalArgs(%q) = %+v", args, g)
		}
	}
}
