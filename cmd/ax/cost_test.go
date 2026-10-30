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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/google/ax/internal/costapi"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func TestParseCostArgs(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    costOptions
		wantErr bool
	}{
		{
			name: "default list",
			args: nil,
			want: costOptions{subcmd: "list", output: "table"},
		},
		{
			name: "tasks list with all atespaces and wide output",
			args: []string{"tasks", "-A", "-o", "wide"},
			want: costOptions{subcmd: "list", allAtespaces: true, output: "wide"},
		},
		{
			name: "task detail by kind and name",
			args: []string{"task", "coding-parent-1", "--output=json"},
			want: costOptions{subcmd: "get", target: "coding-parent-1", output: "json"},
		},
		{
			name: "direct actor name",
			args: []string{"coding-parent-1"},
			want: costOptions{subcmd: "get", target: "coding-parent-1", output: "table"},
		},
		{
			name: "curve subcommand",
			args: []string{"curve"},
			want: costOptions{subcmd: "curve", output: "table"},
		},
		{
			name: "focus csv",
			args: []string{"focus", "-o", "csv"},
			want: costOptions{subcmd: "focus", output: "csv"},
		},
		{
			name: "help flag",
			args: []string{"--help"},
			want: costOptions{subcmd: "list", output: "table", help: true},
		},
		{
			name:    "missing task name",
			args:    []string{"task"},
			wantErr: true,
		},
		{
			name:    "invalid output format",
			args:    []string{"-o", "xml"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCostArgs(tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseCostArgs(%v) error = %v, wantErr %v", tc.args, err, tc.wantErr)
			}
			if !tc.wantErr && !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parseCostArgs(%v) = %+v, want %+v", tc.args, got, tc.want)
			}
		})
	}
}

func newTestCostHTTPServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/costs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeCostReport(w, r, costapi.CostReport{
			Since:       "2026-10-17T00:00:00Z",
			GeneratedAt: "2026-10-17T01:00:00Z",
			Totals:      costapi.Totals{Calls: 2, InputTokens: 2800, OutputTokens: 600, Cost: costapi.MoneyMap{"USD": 0.015525}},
			Compute:     costapi.ComputeSummary{Leases: 2, LeaseSeconds: 20, VCPUSeconds: 10, GiBSeconds: 20, Cost: costapi.MoneyMap{"USD": 0.000171}},
			TotalCost:   costapi.MoneyMap{"USD": 0.016046},
			Actors: []costapi.ActorCost{
				{
					Atespace:              "otel-bench",
					Name:                  "coding-parent-1",
					UID:                   "f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
					Tenant:                "otel-bench",
					Template:              "agent-template",
					Class:                 "agent",
					IsAgent:               true,
					Calls:                 1,
					InputTokens:           2000,
					OutputTokens:          400,
					CacheReadInputTokens:  500,
					CacheWriteInputTokens: 250,
					Cost:                  costapi.MoneyMap{"USD": 0.013088},
					Compute:               costapi.ComputeSummary{Leases: 1, LeaseSeconds: 12, VCPUSeconds: 6, GiBSeconds: 12, Cost: costapi.MoneyMap{"USD": 0.000103}, MeasuredCPUSeconds: 3.12},
					TotalCost:             costapi.MoneyMap{"USD": 0.013540},
					MeasuredComputeCost:   costapi.MoneyMap{"USD": 0.000042},
					StorageCost:           costapi.MoneyMap{"USD": 0.000350},
					HostingCost:           costapi.MoneyMap{"USD": 0.000453},
					MeasuredHostingCost:   costapi.MoneyMap{"USD": 0.000392},
					Models: []costapi.ModelCost{
						{
							Provider:              "anthropic",
							Model:                 "claude-sonnet-4-5",
							Calls:                 1,
							InputTokens:           2000,
							OutputTokens:          400,
							CacheReadInputTokens:  500,
							CacheWriteInputTokens: 250,
							Cost:                  costapi.MoneyMap{"USD": 0.013088},
						},
					},
				},
				{
					Atespace:     "otel-bench",
					Name:         "coding-sub-1",
					UID:          "a5cbff7e-3268-42c3-ab88-65d9c6c8a66e",
					Tenant:       "otel-bench",
					Template:     "agent-template",
					Class:        "agent",
					IsAgent:      true,
					Calls:        1,
					InputTokens:  800,
					OutputTokens: 200,
					Cost:         costapi.MoneyMap{"USD": 0.002438},
					Compute:      costapi.ComputeSummary{Leases: 1, LeaseSeconds: 8, VCPUSeconds: 4, GiBSeconds: 8, Cost: costapi.MoneyMap{"USD": 0.000068}},
					TotalCost:    costapi.MoneyMap{"USD": 0.002856},
					HostingCost:  costapi.MoneyMap{"USD": 0.000068},
				},
			},
			Sessions: []costapi.SessionCost{
				{
					SessionID:     "conv-e2e-demo-1",
					FromHeader:    true,
					Atespace:      "otel-bench",
					Actor:         "otel-bench/coding-parent-1",
					ActorUID:      "f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
					Epoch:         "1",
					Calls:         1,
					InputTokens:   2000,
					OutputTokens:  400,
					ActiveSeconds: 12,
					ActiveShare:   1.0,
					LLMCost:       costapi.MoneyMap{"USD": 0.013088},
					ComputeCost:   costapi.MoneyMap{"USD": 0.000103},
					TotalCost:     costapi.MoneyMap{"USD": 0.013191},
				},
			},
		})
	})
	mux.HandleFunc("/v1/costs/curve", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(costapi.CurveReport{
			TargetUSD:          1.0,
			StoragePerAgentUSD: 0.0115,
			Tiers: []costapi.TierCurve{
				{
					Tier:           "on-demand",
					PerActiveHour:  0.015393,
					Fixed:          0.0115,
					BreakEvenHours: 64.2175,
					AlwaysOn:       11.2484,
					Points: []costapi.CurvePoint{
						{ActiveHours: 10, ActiveCost: 0.1654, UnderTarget: true},
						{ActiveHours: 15, ActiveCost: 0.2424, UnderTarget: true},
					},
				},
			},
		})
	})
	mux.HandleFunc("/v1/costs/focus", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") == "csv" {
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write([]byte("ResourceId,AllocatedMethodId,AllocatedResourceId\ngke-worker-pool/otel-bench/pool-1,booked-share,f45d4ab9\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(costapi.FocusReport{
			Kind:              costapi.FocusReportKind,
			ChargePeriodStart: "2026-10-16T12:00:00Z",
			ChargePeriodEnd:   "2026-10-16T14:00:00Z",
			AllocationKeys: []costapi.AllocationKey{{
				ServiceName:       "Kubernetes Engine",
				ResourceID:        "gke-worker-pool/otel-bench/pool-1",
				AllocatedMethodID: "booked-share",
				AllocatedMethodDetails: costapi.FocusMethodDetails{Elements: []costapi.FocusElement{
					{UsageQuantity: 0.0033, UsageUnit: costapi.UnitSlotHours},
				}},
				AllocatedResourceID:   "f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
				AllocatedResourceName: "otel-bench/coding-parent-1",
				AllocatedTags:         map[string]string{"agent-cost/tenant": "otel-bench", "agent-cost/class": "agent"},
			}},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// writeCostReport serves report the way the Cost Service does: filtered to the
// ?atespace= in the request, with fields that the CLI's structs do not have.
func writeCostReport(w http.ResponseWriter, r *http.Request, report costapi.CostReport) {
	if scope := r.URL.Query().Get("atespace"); scope != "" {
		report.Atespace = scope
		var rows []costapi.ActorCost
		for _, a := range report.Actors {
			if a.Atespace == scope {
				rows = append(rows, a)
			}
		}
		report.Actors = rows
	}
	data, _ := json.Marshal(report)
	var doc map[string]any
	_ = json.Unmarshal(data, &doc)
	doc["futureField"] = map[string]any{"kept": true}
	doc["costBasis"] = "estimated"
	_ = json.NewEncoder(w).Encode(doc)
}

func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	output, err := os.CreateTemp(t.TempDir(), "stdout")
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = output
	defer func() {
		os.Stdout = stdout
		_ = output.Close()
	}()
	runErr := fn()
	data, readErr := os.ReadFile(output.Name())
	if readErr != nil {
		t.Fatal(readErr)
	}
	return string(data), runErr
}

func TestRunCostCommands(t *testing.T) {
	costSrv := newTestCostHTTPServer(t)

	// Start an in-memory AX gRPC server with coding-parent-1 and coding-sub-1.
	s := memory.NewStore()
	if err := s.SaveTask(context.Background(), &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "coding-parent-1", Atespace: "otel-bench"},
		Status:   &v1alpha1.TaskStatus{Id: "task-parent-1", Phase: "Running", Actor: "coding-parent-1"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveTask(context.Background(), &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "coding-sub-1", Atespace: "otel-bench"},
		Spec:     &v1alpha1.TaskSpec{Parent: "coding-parent-1"},
		Status:   &v1alpha1.TaskStatus{Id: "task-sub-1", ParentId: "task-parent-1", Phase: "Running", Actor: "coding-sub-1"},
	}); err != nil {
		t.Fatal(err)
	}
	axAddr := serveAX(t, s)

	t.Run("list table joined with ax-server", func(t *testing.T) {
		out, err := captureStdout(t, func() error {
			return runCost(costSrv.URL, axAddr, "otel-bench", true, nil)
		})
		if err != nil {
			t.Fatalf("runCost list failed: %v", err)
		}
		for _, want := range []string{"NAME", "PHASE", "coding-parent-1", "Running", "$0.013540", "$0.016396", "Summary (atespace otel-bench, 2 rows)", "total=$0.016046", "Cost basis: ESTIMATE — NOT RECONCILED"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected output to contain %q, got:\n%s", want, out)
			}
		}
	})

	t.Run("task detail report", func(t *testing.T) {
		out, err := captureStdout(t, func() error {
			return runCost(costSrv.URL, axAddr, "otel-bench", true, []string{"task", "coding-parent-1"})
		})
		if err != nil {
			t.Fatalf("runCost task detail failed: %v", err)
		}
		for _, want := range []string{
			"Actor UID:          f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
			"Task Phase:         Running",
			"1. Active Hosting Cost",
			"claude-sonnet-4-5",
			"Tree (Own + 1 Sub-Agents)",
			"coding-sub-1",
			"conv-e2e-demo-1",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected detail output to contain %q, got:\n%s", want, out)
			}
		}
	})

	t.Run("curve report", func(t *testing.T) {
		out, err := captureStdout(t, func() error {
			return runCost(costSrv.URL, "", "default", false, []string{"curve"})
		})
		if err != nil {
			t.Fatalf("runCost curve failed: %v", err)
		}
		for _, want := range []string{"Hosting Cost Sensitivity Curve", "on-demand", "64.22 h/mo", "$0.1654"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected curve output to contain %q, got:\n%s", want, out)
			}
		}
	})

	t.Run("focus table and csv", func(t *testing.T) {
		out, err := captureStdout(t, func() error {
			return runCost(costSrv.URL, "", "default", false, []string{"focus"})
		})
		if err != nil {
			t.Fatalf("runCost focus failed: %v", err)
		}
		for _, want := range []string{"FOCUS 1.3 Allocation Keys", "gke-worker-pool/otel-bench/pool-1", "otel-bench/coding-parent-1", "0.0033 Slot-Hours", "otel-bench", "agent"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected focus output to contain %q, got:\n%s", want, out)
			}
		}

		csvOut, err := captureStdout(t, func() error {
			return runCost(costSrv.URL, "", "default", false, []string{"focus", "-o", "csv"})
		})
		if err != nil {
			t.Fatalf("runCost focus csv failed: %v", err)
		}
		if !strings.Contains(csvOut, "gke-worker-pool/otel-bench/pool-1,booked-share,f45d4ab9") {
			t.Fatalf("unexpected focus CSV output:\n%s", csvOut)
		}
	})
}

func TestLocalTaskTreeRollup(t *testing.T) {
	// The Cost Service reports only per-actor costs (no parentAgentId, subAgents, or treeTotalCost).
	// Also put bogus treeTotalCost=99.0 and parentAgentId="bogus" on rows to prove
	// the CLI ignores the Cost Service's tree fields and computes rollups from Ax tasks alone.
	costBody := `{
  "costBasis": "estimated",
  "atespace": "team-a",
  "totals": {"calls": 3, "cost": {"USD": 0.35}},
  "compute": {"leases": 3, "cost": {"USD": 0.07}},
  "totalCost": {"USD": 0.42},
  "actors": [
    {
      "atespace": "team-a",
      "name": "root",
      "uid": "uid-root",
      "isAgent": true,
      "calls": 1,
      "cost": {"USD": 0.10},
      "compute": {"leases": 1, "cost": {"USD": 0.02}},
      "totalCost": {"USD": 0.12},
      "subAgents": 99,
      "treeTotalCost": {"USD": 99.0}
    },
    {
      "atespace": "team-a",
      "name": "grandchild",
      "uid": "uid-grandchild",
      "isAgent": true,
      "calls": 1,
      "cost": {"USD": 0.20},
      "compute": {"leases": 1, "cost": {"USD": 0.04}},
      "totalCost": {"USD": 0.24},
      "parentAgentId": "bogus"
    },
    {
      "atespace": "team-a",
      "name": "stale-child",
      "uid": "uid-stale",
      "isAgent": true,
      "calls": 1,
      "cost": {"USD": 0.05},
      "compute": {"leases": 1, "cost": {"USD": 0.01}},
      "totalCost": {"USD": 0.06},
      "parentAgentId": "team-a/root"
    },
    {
      "atespace": "team-b",
      "name": "other-child",
      "uid": "uid-other",
      "isAgent": true,
      "calls": 1,
      "cost": {"USD": 0.50},
      "compute": {"leases": 1, "cost": {"USD": 0.10}},
      "totalCost": {"USD": 0.60}
    },
    {
      "atespace": "team-a",
      "name": "cycle-a",
      "uid": "uid-cycle-a",
      "isAgent": true,
      "cost": {"USD": 0.01},
      "compute": {"cost": {"USD": 0.01}},
      "totalCost": {"USD": 0.02}
    },
    {
      "atespace": "team-a",
      "name": "cycle-b",
      "uid": "uid-cycle-b",
      "isAgent": true,
      "cost": {"USD": 0.02},
      "compute": {"cost": {"USD": 0.01}},
      "totalCost": {"USD": 0.03}
    }
  ]
}`
	srv := rawServer(t, map[string]string{"/v1/costs": costBody})

	// Task tree in ax-server:
	// - root (id: id-root-v2) -> mid-unpriced (id: id-mid, parent_id: id-root-v2, NO Cost Service actor)
	//   -> grandchild (id: id-gc, parent_id: id-mid)
	// - stale-child has spec.parent="root" but status.parent_id="id-root-v1" (recreated parent guard!)
	// - other-child is in team-b with spec.parent="root" and parent_id="id-root-v2" (same-atespace guard!)
	// - cycle-a <-> cycle-b form a cycle in stored tasks (cycle safety!)
	tasks := []*v1alpha1.Task{
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "root", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Budget: &v1alpha1.Budget{MaxUsd: 0.30}},
			Status:   &v1alpha1.TaskStatus{Id: "id-root-v2", Phase: "Running"},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "mid-unpriced", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Parent: "root"},
			Status:   &v1alpha1.TaskStatus{Id: "id-mid", ParentId: "id-root-v2", Phase: "Running"},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "grandchild", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Parent: "mid-unpriced"},
			Status:   &v1alpha1.TaskStatus{Id: "id-gc", ParentId: "id-mid", Phase: "Completed"},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "stale-child", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Parent: "root"},
			Status:   &v1alpha1.TaskStatus{Id: "id-stale", ParentId: "id-root-v1", Phase: "Completed"},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "other-child", Atespace: "team-b"},
			Spec:     &v1alpha1.TaskSpec{Parent: "root"},
			Status:   &v1alpha1.TaskStatus{Id: "id-other", ParentId: "id-root-v2", Phase: "Running"},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "cycle-a", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Parent: "cycle-b"},
			Status:   &v1alpha1.TaskStatus{Id: "id-ca", ParentId: "id-cb", Phase: "Running"},
		},
		{
			Metadata: &v1alpha1.ObjectMeta{Name: "cycle-b", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Parent: "cycle-a"},
			Status:   &v1alpha1.TaskStatus{Id: "id-cb", ParentId: "id-ca", Phase: "Running"},
		},
	}
	ax := axServer(t, tasks...)

	// 1. `ax cost task root`:
	// Own = $0.12 (LLM $0.10 + compute $0.02).
	// Valid descendants = mid-unpriced ($0) + grandchild ($0.24 = LLM $0.20 + compute $0.04).
	// stale-child ($0.06) and other-child ($0.60) must be excluded.
	// Total tree = $0.36 (LLM $0.30 + compute $0.06), 2 sub-agents, OVER BUDGET (with sub-agents) against $0.30.
	out, err := captureStdout(t, func() error {
		return runCost(srv.URL, ax, "team-a", true, []string{"task", "root"})
	})
	if err != nil {
		t.Fatalf("ax cost task root failed: %v", err)
	}
	for _, want := range []string{
		"Sub-Agents:         2",
		"Spend (LLM+compute): own $0.120000 (40.0% of budget), tree $0.360000 (120.0%)  OVER BUDGET (with sub-agents)",
		"Tree (Own + 2 Sub-Agents)",
		"$0.300000",
		"$0.060000",
		"$0.360000",
		"mid-unpriced",
		"grandchild",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("root detail output lacks %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"stale-child", "other-child", "$99.000000", "Parent Agent:"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("root detail output unexpectedly contains %q:\n%s", unwanted, out)
		}
	}

	// 2. `ax cost task mid-unpriced`:
	// mid-unpriced has NO Cost Service actor record, but exists in ax-server and has
	// child `grandchild` ($0.24) in the Cost Service. It falls back gracefully to $0 own
	// cost and $0.24 tree cost.
	out, err = captureStdout(t, func() error {
		return runCost(srv.URL, ax, "team-a", true, []string{"task", "mid-unpriced"})
	})
	if err != nil {
		t.Fatalf("ax cost task mid-unpriced (missing Cost Service actor) failed: %v", err)
	}
	for _, want := range []string{
		"Name:               mid-unpriced",
		"Task Phase:         Running",
		"Sub-Agents:         1",
		"Parent Task:        root",
		"Spend (LLM+compute): own $0.000000, tree $0.240000",
		"Tree (Own + 1 Sub-Agents)",
		"grandchild",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("mid-unpriced detail output lacks %q:\n%s", want, out)
		}
	}

	// 3. `ax cost task cycle-a`:
	// Cycle cycle-a <-> cycle-b terminates and counts cycle-b once ($0.02 + $0.03 = $0.05).
	out, err = captureStdout(t, func() error {
		return runCost(srv.URL, ax, "team-a", true, []string{"task", "cycle-a"})
	})
	if err != nil {
		t.Fatalf("ax cost task cycle-a failed: %v", err)
	}
	for _, want := range []string{
		"Sub-Agents:         1",
		"Spend (LLM+compute): own $0.020000, tree $0.050000",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("cycle-a detail output lacks %q:\n%s", want, out)
		}
	}

	// 4. `ax cost -a team-a -o wide` list table shows local sub-agent count (2) and tree total ($0.360000) for root,
	// and ignores the Cost Service's bogus 99 subAgents / $99.000000 treeTotalCost.
	out, err = captureStdout(t, func() error {
		return runCost(srv.URL, ax, "team-a", true, []string{"-o", "wide"})
	})
	if err != nil {
		t.Fatalf("ax cost -o wide failed: %v", err)
	}
	if !strings.Contains(out, "$0.360000") || strings.Contains(out, "$99.000000") {
		t.Errorf("wide list did not use local tree rollup:\n%s", out)
	}

	// 5. `ax describe task` (via tryPrintTaskDescribeCost) computes local tree rollup,
	// including when the described parent task (`mid-unpriced`) has no Cost Service actor of its own.
	taskMap := map[string]*v1alpha1.Task{}
	for _, tk := range tasks {
		if tk.GetMetadata().GetAtespace() == "team-a" {
			taskMap[taskKey("team-a", tk.GetMetadata().GetName())] = tk
		}
	}
	var descBuf bytes.Buffer
	tryPrintTaskDescribeCost(&descBuf, srv.URL, tasks[0], taskMap)
	if !strings.Contains(descBuf.String(), "Tree (Own + 2 Sub-Agents)") || !strings.Contains(descBuf.String(), "$0.360000") {
		t.Errorf("describe root lacks local tree rollup:\n%s", descBuf.String())
	}

	descBuf.Reset()
	tryPrintTaskDescribeCost(&descBuf, srv.URL, tasks[1], taskMap)
	if !strings.Contains(descBuf.String(), "Tree (Own + 1 Sub-Agents)") || !strings.Contains(descBuf.String(), "$0.240000") {
		t.Errorf("describe mid-unpriced lacks local tree rollup:\n%s", descBuf.String())
	}
}
