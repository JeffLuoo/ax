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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/ax/internal/costapi"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// The files testdata/costs-*.json, curve.json, focus.json, and focus.csv are the responses of
// Cost Service HTTP handlers (github.com/JeffLuoo/agent-cost, main at b5c4eae, when the repository was named finops)
// for the actors team-a/root (task completed, one lease still open) and
// team-a/child (task failed over its budget), plus team-b/solo outside the
// atespace. Regenerate them whenever the Cost Service HTTP API changes, so
// that this test checks the CLI against the server that ships.

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// strictDecode fails on any field in data that v does not declare, so a field
// that the server adds or renames shows up here and not as a silent zero.
func strictDecode(t *testing.T, name string, v any) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(readGolden(t, name)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("%s does not match the client's types: %v", name, err)
	}
}

func TestCostContract_GoldenDecodesStrictly(t *testing.T) {
	var report costapi.CostReport
	strictDecode(t, "costs-team-a.json", &report)
	strictDecode(t, "costs-all.json", &costapi.CostReport{})
	var curve costapi.CurveReport
	strictDecode(t, "curve.json", &curve)
	var focus costapi.FocusReport
	strictDecode(t, "focus.json", &focus)

	// The keys that the CLI reads are the keys that the server sends.
	if report.Atespace != "team-a" || report.CostBasis != costapi.CostBasisEstimated {
		t.Errorf("atespace %q, costBasis %q", report.Atespace, report.CostBasis)
	}
	if len(report.Curve) == 0 || report.OutcomeCosts == nil {
		t.Errorf("curve %d rows, outcomeCosts %v", len(report.Curve), report.OutcomeCosts)
	}
	// The open lease's accrued cost is reported apart and is already in the
	// compute total.
	if !report.EstimatedFlag || !report.Estimated() || report.InFlightComputeCost.USD() <= 0 ||
		report.Compute.InFlightComputeCost.USD() != report.InFlightComputeCost.USD() ||
		report.Compute.Cost.USD() < report.InFlightComputeCost.USD() {
		t.Errorf("estimated %t, inFlightComputeCost %v, compute %+v", report.EstimatedFlag, report.InFlightComputeCost, report.Compute)
	}
	byName := map[string]costapi.ActorCost{}
	for _, a := range report.Actors {
		byName[a.Name] = a
	}
	child := byName["child"]
	if child.Task == nil || child.Task.Outcome != "Failed" || child.Task.ExitCode == nil || *child.Task.ExitCode != 3 || !child.Task.OverBudget ||
		child.Task.OutcomeSource != "sandbox" || report.OutcomeCosts.OutcomeSource != "sandbox" {
		t.Errorf("child row = %+v, task %+v", child, child.Task)
	}
	if child.Compute.Cost.USD() <= 0 || child.TotalCost.USD() <= 0 {
		t.Errorf("child has no compute cost: %+v", child.Compute)
	}
	if len(curve.Tiers) == 0 || len(curve.Tiers[0].Points) == 0 {
		t.Errorf("curve = %+v", curve)
	}
	if focus.Kind != costapi.FocusReportKind || focus.ChargePeriodStart == "" || len(focus.AllocationKeys) == 0 {
		t.Fatalf("focus kind %q, period %q, %d keys", focus.Kind, focus.ChargePeriodStart, len(focus.AllocationKeys))
	}
	methods := map[string]bool{}
	for _, k := range focus.AllocationKeys {
		methods[k.AllocatedMethodID] = true
		if k.ResourceID == "" || k.AllocatedResourceID == "" || len(k.AllocatedMethodDetails.Elements) == 0 {
			t.Errorf("allocation key without origin, agent, or usage: %+v", k)
		}
		for name := range k.AllocatedTags {
			if strings.HasPrefix(name, "finops.") || strings.HasPrefix(name, "ate.finops") {
				t.Errorf("allocation key has legacy tag %q", name)
			}
		}
		if k.AllocatedMethodID == "booked-share" {
			if q, ok := k.Quantity(costapi.UnitSlotHours); !ok || q <= 0 {
				t.Errorf("compute key without Slot-Hours: %+v", k)
			}
		}
	}
	if !methods["booked-share"] || !methods["token-usage"] {
		t.Errorf("allocation key methods = %v, want booked-share and token-usage", methods)
	}
}

func TestCostContract_CLIRendersTheGolden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := map[string]string{"/v1/costs": "costs-all.json", "/v1/costs/curve": "curve.json", "/v1/costs/focus": "focus.json"}[r.URL.Path]
		switch {
		case name == "":
			http.NotFound(w, r)
			return
		case r.URL.Path == "/v1/costs/focus" && r.URL.Query().Get("format") == "csv":
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write(readGolden(t, "focus.csv"))
			return
		case r.URL.Path == "/v1/costs" && r.URL.Query().Get("atespace") == "team-a":
			name = "costs-team-a.json"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(readGolden(t, name))
	}))
	defer srv.Close()

	ax := axServer(t,
		&v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "root", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Budget: &v1alpha1.Budget{MaxUsd: 5}},
			Status:   &v1alpha1.TaskStatus{Id: "task-root-1", Phase: "Running", Outcome: v1alpha1.OutcomeCompleted, ExitCode: 0},
		},
		&v1alpha1.Task{
			Metadata: &v1alpha1.ObjectMeta{Name: "child", Atespace: "team-a"},
			Spec:     &v1alpha1.TaskSpec{Parent: "root", Budget: &v1alpha1.Budget{MaxUsd: 0.01}},
			Status:   &v1alpha1.TaskStatus{Id: "task-child-1", ParentId: "task-root-1", Phase: "Running", Outcome: v1alpha1.OutcomeFailed, ExitCode: 3},
		},
	)

	out, err := captureStdout(t, func() error { return runCost(srv.URL, ax, "team-a", true, nil) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"child", "root", "OUTCOME", "Failed", "Completed", "OVER", "Summary (atespace team-a, 2 rows)", "In-flight compute: $0.013333 from 1 open leases", "Cost basis: ESTIMATE — NOT RECONCILED", "By task outcome (self-reported): "} {
		if !strings.Contains(out, want) {
			t.Errorf("list output lacks %q:\n%s", want, out)
		}
	}

	out, err = captureStdout(t, func() error { return runCost(srv.URL, ax, "team-a", true, []string{"task", "child"}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Parent Task:        root", "Outcome:            Failed (self-reported: exit code 3)", "Budget:             $0.010000", "OVER BUDGET"} {
		if !strings.Contains(out, want) {
			t.Errorf("detail output lacks %q:\n%s", want, out)
		}
	}
	out, err = captureStdout(t, func() error { return runCost(srv.URL, ax, "team-a", true, []string{"task", "root"}) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "child") || !strings.Contains(out, "Sub-Agents:") {
		t.Errorf("root detail does not list its sub-agent:\n%s", out)
	}
	if !strings.Contains(out, "of which in flight") || !strings.Contains(out, "open leases=1") {
		t.Errorf("root detail does not show its open lease's accrued cost:\n%s", out)
	}

	for _, args := range [][]string{{"curve"}, {"focus"}, {"focus", "-o", "csv"}, {"-o", "yaml"}} {
		if _, err := captureStdout(t, func() error { return runCost(srv.URL, "", "team-a", true, args) }); err != nil {
			t.Errorf("ax cost %v: %v", args, err)
		}
	}
}
