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

	"github.com/google/ax/internal/finops"
)

// The files testdata/finops-*.json and finops-focus.csv are the responses of
// a real finops server (github.com/JeffLuoo/finops, branch fix/top5 at
// dd7d851) after it took two ax.task.state_changed events from this repo's
// internal/events emitter and nine Substrate lifecycle events for the actors
// team-a/root and team-a/child. Regenerate them whenever the finops HTTP API
// changes, and at least once after the finops top-five changes merge, so that
// this test checks the CLI against the server that ships.

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

func TestFinOpsContract_GoldenDecodesStrictly(t *testing.T) {
	var report finops.CostReport
	strictDecode(t, "finops-costs-team-a.json", &report)
	strictDecode(t, "finops-costs-all.json", &finops.CostReport{})
	var curve finops.CurveReport
	strictDecode(t, "finops-curve.json", &curve)
	var focus finops.FocusReport
	strictDecode(t, "finops-focus.json", &focus)

	// The keys that the CLI reads are the keys that the server sends.
	if report.Atespace != "team-a" || report.CostBasis != finops.CostBasisEstimated {
		t.Errorf("atespace %q, costBasis %q", report.Atespace, report.CostBasis)
	}
	if len(report.Curve) == 0 || len(report.Focus) == 0 || report.FocusReconciliation == nil || report.OutcomeCosts == nil {
		t.Errorf("curve %d rows, focus %d rows, reconciliation %v, outcomeCosts %v", len(report.Curve), len(report.Focus), report.FocusReconciliation, report.OutcomeCosts)
	}
	byName := map[string]finops.ActorCost{}
	for _, a := range report.Actors {
		byName[a.Name] = a
	}
	child := byName["child"]
	if child.ParentAgentID != "team-a/root" || child.Task == nil || child.Task.Outcome != "Failed" || child.Task.ExitCode == nil || *child.Task.ExitCode != 3 || !child.Task.OverBudget {
		t.Errorf("child row = %+v, task %+v", child, child.Task)
	}
	if child.Compute.Cost.USD() <= 0 || child.TotalCost.USD() <= 0 {
		t.Errorf("child has no compute cost: %+v", child.Compute)
	}
	if len(curve.Tiers) == 0 || len(curve.Tiers[0].Points) == 0 {
		t.Errorf("curve = %+v", curve)
	}
	if focus.Reconciliation == nil || !focus.Estimated() || len(focus.Charges) == 0 {
		t.Errorf("focus reconciliation %+v, estimated %t, %d charges", focus.Reconciliation, focus.Estimated(), len(focus.Charges))
	}
}

func TestFinOpsContract_CLIRendersTheGolden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := map[string]string{"/v1/costs": "finops-costs-all.json", "/v1/costs/curve": "finops-curve.json", "/v1/costs/focus": "finops-focus.json"}[r.URL.Path]
		switch {
		case name == "":
			http.NotFound(w, r)
			return
		case r.URL.Path == "/v1/costs/focus" && r.URL.Query().Get("format") == "csv":
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write(readGolden(t, "finops-focus.csv"))
			return
		case r.URL.Path == "/v1/costs" && r.URL.Query().Get("atespace") == "team-a":
			name = "finops-costs-team-a.json"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(readGolden(t, name))
	}))
	defer srv.Close()

	out, err := captureStdout(t, func() error { return runCost(srv.URL, "", "team-a", true, nil) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"child", "root", "OUTCOME", "Failed", "Completed", "OVER", "Summary (atespace team-a, 2 rows)", "Cost basis: ESTIMATE — NOT RECONCILED", "By task outcome"} {
		if !strings.Contains(out, want) {
			t.Errorf("list output lacks %q:\n%s", want, out)
		}
	}

	out, err = captureStdout(t, func() error { return runCost(srv.URL, "", "team-a", true, []string{"task", "child"}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Parent Task:        root", "Outcome:            Failed (exit code 3)", "Budget:             $0.010000", "OVER BUDGET", "Parent Agent:       team-a/root"} {
		if !strings.Contains(out, want) {
			t.Errorf("detail output lacks %q:\n%s", want, out)
		}
	}
	out, err = captureStdout(t, func() error { return runCost(srv.URL, "", "team-a", true, []string{"task", "root"}) })
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "child") || !strings.Contains(out, "Sub-Agents (gen_ai.agent.parent_id names this actor)") {
		t.Errorf("root detail does not list its sub-agent:\n%s", out)
	}

	for _, args := range [][]string{{"curve"}, {"focus"}, {"focus", "-o", "csv"}, {"-o", "yaml"}} {
		if _, err := captureStdout(t, func() error { return runCost(srv.URL, "", "team-a", true, args) }); err != nil {
			t.Errorf("ax cost %v: %v", args, err)
		}
	}
}
