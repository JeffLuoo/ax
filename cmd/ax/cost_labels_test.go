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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/finops"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/tunnel"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

// rawServer serves fixed JSON bodies by path, ignoring the query.
func rawServer(t *testing.T, bodies map[string]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := bodies[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

const warningReport = `{
  "since": "2026-10-18T00:00:00Z",
  "generatedAt": "2026-10-18T01:00:00Z",
  "costBasis": "estimated",
  "totals": {"calls": 9, "cost": {"USD": 0.5}, "unpricedCalls": 2, "unknownCostCalls": 3, "usageStatus": {"ok": 4, "missing": 2, "truncated": 1}},
  "compute": {"leases": 2, "cost": {"USD": 0.25}, "unpricedLeases": 1},
  "totalCost": {"USD": 0.75},
  "fullyLoadedCost": {"USD": 0.9},
  "actors": [
    {"atespace": "team-a", "name": "agent-1", "uid": "u1", "isAgent": true, "calls": 5, "cost": {"USD": 0.4},
     "totalCost": {"USD": 0.6}, "fullyLoadedCost": {"USD": 0.7},
     "task": {"name": "agent-1", "budgetUsd": 0.5, "spendUsd": 0.6, "treeSpendUsd": 0.6, "overBudget": true, "outcome": "Failed", "exitCode": 3}},
    {"atespace": "", "name": "", "note": "unknown actor", "calls": 4, "cost": {"USD": 0.1}, "totalCost": {"USD": 0.15}}
  ],
  "outcomeCosts": {"completed": {"tasks": 0, "totalCost": {}}, "failed": {"tasks": 1, "totalCost": {"USD": 0.6}},
    "unfinished": {"tasks": 0, "totalCost": {}}, "noTask": {"actors": 1, "totalCost": {"USD": 0.15}}},
  "futureField": {"when": "2026-10-18", "flag": "true", "n": 1}
}`

func TestCostList_LabelsEstimatesAndWarnsAboutLeftOutSpend(t *testing.T) {
	srv := rawServer(t, map[string]string{"/v1/costs": warningReport})
	out, err := captureStdout(t, func() error { return runCost(srv.URL, "", "default", false, []string{"-A"}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"(unknown actor)",
		"OUTCOME",
		"Failed",
		"$0.600000/$0.500000 OVER",
		"Summary (all atespaces, 2 rows): calls=9",
		"total=$0.750000 fully_loaded=$0.900000",
		// A server with no outcomeSource predates it: its outcomes are the exit code.
		"By task outcome (self-reported): completed=$0.000000 (0 tasks) failed=$0.600000 (1 tasks)",
		"Cost basis: ESTIMATE — NOT RECONCILED",
		"Warning: these numbers leave out spend: 3 calls with unknown cost (no usage report); 2 calls with no price; 1 leases with no price; usage status missing=2 truncated=1; unknown actor: 4 calls, $0.150000",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestCostOutput_JSONAndYAMLPassTheServerBodyThrough(t *testing.T) {
	srv := rawServer(t, map[string]string{"/v1/costs": warningReport})
	var want any
	if err := json.Unmarshal([]byte(warningReport), &want); err != nil {
		t.Fatal(err)
	}

	out, err := captureStdout(t, func() error { return runCost(srv.URL, "", "default", false, []string{"-A", "-o", "json"}) })
	if err != nil {
		t.Fatal(err)
	}
	var gotJSON any
	if err := json.Unmarshal([]byte(out), &gotJSON); err != nil {
		t.Fatalf("-o json is not JSON: %v\n%s", err, out)
	}
	if !reflect.DeepEqual(gotJSON, want) {
		t.Errorf("-o json changed the server body:\n%s", out)
	}

	out, err = captureStdout(t, func() error { return runCost(srv.URL, "", "default", false, []string{"-A", "-o", "yaml"}) })
	if err != nil {
		t.Fatal(err)
	}
	// An empty map has only the flow form "{}"; any other brace is flow style.
	if strings.Contains(strings.ReplaceAll(out, "{}", ""), "{") {
		t.Errorf("-o yaml kept flow style:\n%s", out)
	}
	var gotYAML any
	if err := yaml.Unmarshal([]byte(out), &gotYAML); err != nil {
		t.Fatalf("-o yaml is not YAML: %v\n%s", err, out)
	}
	// Round-trip through JSON so that both sides have the same number types.
	b, _ := json.Marshal(gotYAML)
	var normalized any
	_ = json.Unmarshal(b, &normalized)
	if !reflect.DeepEqual(normalized, want) {
		t.Errorf("-o yaml changed the server body:\n%s", out)
	}
}

func TestJSONToYAML_KeepsStringsThatLookLikeOtherTypes(t *testing.T) {
	y, err := jsonToYAML([]byte(`{"b":"true","n":"1","d":"2026-10-18","e":"","z":null,"list":[1,"x"],"a":1.5}`))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := yaml.Unmarshal(y, &got); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]any{"b": "true", "n": "1", "d": "2026-10-18", "e": "", "a": 1.5} {
		if got[k] != want {
			t.Errorf("%s = %#v, want %#v\n%s", k, got[k], want, y)
		}
	}
	if got["z"] != nil {
		t.Errorf("z = %#v, want nil", got["z"])
	}
	// The keys keep the server's order.
	if !strings.HasPrefix(string(y), "b:") || strings.Index(string(y), "list:") > strings.Index(string(y), "a:") {
		t.Errorf("key order changed:\n%s", y)
	}
}

// axServer starts an in-memory AX gRPC server with the given tasks.
func axServer(t *testing.T, tasks ...*v1alpha1.Task) string {
	t.Helper()
	s := memory.NewStore()
	for _, task := range tasks {
		if err := s.SaveTask(context.Background(), task); err != nil {
			t.Fatal(err)
		}
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := server.NewServer(s).GRPCServer()
	t.Cleanup(gs.Stop)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String()
}

func TestCostTask_ShowsBudgetSpendAndOutcome(t *testing.T) {
	srv := rawServer(t, map[string]string{"/v1/costs": warningReport})
	finished := time.Date(2026, 10, 18, 8, 30, 0, 0, time.UTC)
	ax := axServer(t, &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "agent-1", Atespace: "team-a"},
		Spec:     &v1alpha1.TaskSpec{Parent: "planner", Budget: &v1alpha1.Budget{MaxUsd: 0.5}},
		Status:   &v1alpha1.TaskStatus{Phase: "Running", Outcome: v1alpha1.OutcomeFailed, ExitCode: 3, FinishedAt: timestamppb.New(finished)},
	})

	out, err := captureStdout(t, func() error { return runCost(srv.URL, ax, "team-a", true, []string{"task", "agent-1"}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Task Phase:         Running",
		"Parent Task:        planner",
		"Outcome:            Failed (self-reported: exit code 3, finished 2026-10-18T08:30:00Z)",
		"Budget:             $0.500000",
		"Spend (LLM+compute): own $0.600000 (120.0% of budget), tree $0.000000 (0.0%)  OVER BUDGET",
		"Cost basis: ESTIMATE — NOT RECONCILED",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}

	// Without ax-server, the task fields come from FinOps.
	out, err = captureStdout(t, func() error { return runCost(srv.URL, "", "team-a", true, []string{"task", "agent-1"}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Outcome:            Failed (self-reported: exit code 3)", "Budget:             $0.500000", "OVER BUDGET"} {
		if !strings.Contains(out, want) {
			t.Errorf("fallback output lacks %q:\n%s", want, out)
		}
	}

	// -o json carries the FinOps row as it came and the AX task.
	out, err = captureStdout(t, func() error { return runCost(srv.URL, ax, "team-a", true, []string{"task", "agent-1", "-o", "json"}) })
	if err != nil {
		t.Fatal(err)
	}
	var exp map[string]any
	if err := json.Unmarshal([]byte(out), &exp); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, out)
	}
	actor, _ := exp["actor"].(map[string]any)
	axTask, _ := exp["axTask"].(map[string]any)
	if actor["task"] == nil || axTask["status"] == nil || exp["costBasis"] != "estimated" {
		t.Errorf("detail JSON = %s", out)
	}
}

func TestDescribeTaskCost_UsesTheTasksAtespaceAndReportsAmbiguity(t *testing.T) {
	srv := rawServer(t, map[string]string{"/v1/costs": warningReport})
	var buf bytes.Buffer
	tryPrintTaskDescribeCost(&buf, srv.URL, &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "agent-1", Atespace: "team-a"},
		Spec:     &v1alpha1.TaskSpec{Budget: &v1alpha1.Budget{MaxUsd: 1}},
	})
	for _, want := range []string{"FinOps Cost:", "Budget:             $1.000000", "Outcome:            - (the command has not exited)", "Hosting Cost Breakdown"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("describe output lacks %q:\n%s", want, buf.String())
		}
	}
	// A task in another atespace has no FinOps record there: nothing is shown.
	buf.Reset()
	tryPrintTaskDescribeCost(&buf, srv.URL, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "agent-1", Atespace: "team-b"}})
	if buf.Len() != 0 {
		t.Errorf("describe in another atespace printed:\n%s", buf.String())
	}

	recreated := rawServer(t, map[string]string{"/v1/costs": `{"actors":[
		{"atespace":"team-a","name":"agent-1","uid":"u1"},{"atespace":"team-a","name":"agent-1","uid":"u2"}]}`})
	buf.Reset()
	tryPrintTaskDescribeCost(&buf, recreated.URL, &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "agent-1", Atespace: "team-a"}})
	if !strings.Contains(buf.String(), "matches 2 FinOps cost records") {
		t.Errorf("a recreated actor did not report the ambiguity:\n%s", buf.String())
	}
}

func TestCostFocus_LabelsAnEstimatedOrigin(t *testing.T) {
	srv := rawServer(t, map[string]string{"/v1/costs/focus": `{
	  "originCharge": {"OriginChargeId": "charge-pool-all", "BillingCurrency": "USD", "ServiceName": "Kubernetes Engine", "ResourceName": "default",
	    "EffectiveCost": 0.5, "BilledCost": 0, "Tags": {"finops.agent-substrate.dev/estimated": "true"}},
	  "charges": [
	    {"OriginChargeId": "charge-pool-all", "EffectiveCost": 0.2, "BilledCost": 0, "AllocatedMethodId": "booked-share", "AllocatedResourceId": "u1",
	     "AllocatedResourceName": "team-a/agent-1", "AllocatedTags": {"tenant": "team-a", "is_agent": "true"}},
	    {"OriginChargeId": "charge-pool-all", "EffectiveCost": 0.3, "BilledCost": 0, "AllocatedMethodId": "booked-share", "AllocatedResourceId": "_unused_capacity"}
	  ],
	  "auxiliaryCharges": [
	    {"OriginChargeId": "tokens", "EffectiveCost": 0.4, "AllocatedMethodId": "token-usage", "AllocatedResourceId": "u1", "AllocatedResourceName": "team-a/agent-1"}
	  ],
	  "reconciliation": {"resourceId": "gke-worker-pool/all", "pool": "ate-workers/default", "estimated": true, "originCost": 0.5, "allocated": 0.2,
	    "unallocated": 0.3, "agents": 1, "unsizedLeases": 0, "expectedIdle": 0.29, "priceGap": 0.01,
	    "coverageLeaseHours": 2, "coverageActivationHours": 2.5, "coverageRatio": 0.8, "estimatedLeases": 1},
	  "reconciliations": [
	    {"pool": "ate-workers/default", "estimated": true, "originCost": 0.3, "allocated": 0.1, "unallocated": 0.2, "agents": 1},
	    {"pool": "ate-workers/spot", "estimated": true, "originCost": 0.2, "allocated": 0.1, "unallocated": 0.1, "agents": 1, "problems": ["leases booked more than the pool's capacity"]}
	  ]
	}`})
	out, err := captureStdout(t, func() error { return runCost(srv.URL, "", "default", false, []string{"focus"}) })
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Cost Basis:      ESTIMATE — NOT RECONCILED",
		"effective=$0.500000, billed=$0.000000 USD",
		"expected_idle=$0.290000",
		"price_gap=$0.010000 (bill vs. capacity at rate card prices; does not depend on the leases)",
		"coverage=2.0000 lease-h / 2.5000 activation-h (ratio 0.8000)",
		"Status:          NOT CHECKED AGAINST A BILL",
		"EFFECTIVE ($)",
		"Auxiliary Charges (tokens, storage, platform overhead):",
		"token-usage",
		"Estimated Leases: 1",
		"ate-workers/spot origin=$0.200000 allocated=$0.100000 unallocated=$0.100000 agents=1 problems: leases booked more than the pool's capacity",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("focus output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "status=OK") || strings.Contains(out, "Status:          OK") {
		t.Errorf("an estimated origin shows as checked:\n%s", out)
	}
}

func TestCostFocus_OlderServerGapStillShows(t *testing.T) {
	r := finops.FocusReconciliation{Gap: new(float64)}
	*r.Gap = 0.02
	if g := r.PriceGapValue(); g == nil || *g != 0.02 {
		t.Errorf("PriceGapValue with only gap = %v", g)
	}
}

func TestStopContextTunnels_StopsBothTunnels(t *testing.T) {
	t.Setenv("AX_HOME", t.TempDir())
	for _, key := range []string{"prod", tunnel.FinOpsTunnelKey("prod")} {
		if err := tunnel.SaveTunnel(&tunnel.TunnelInfo{Context: key, Port: 1}); err != nil {
			t.Fatal(err)
		}
	}
	var buf bytes.Buffer
	if err := stopContextTunnels(&buf, "prod"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"prod", "prod-finops"} {
		if _, err := tunnel.GetTunnel(key); !os.IsNotExist(err) {
			t.Errorf("tunnel %q still saved (err %v)", key, err)
		}
		if !strings.Contains(buf.String(), `Tunnel "`+key+`" stopped`) {
			t.Errorf("output lacks the stop of %q:\n%s", key, buf.String())
		}
	}
	if err := stopContextTunnels(&buf, "prod"); err == nil {
		t.Error("stopping with no tunnels did not fail")
	}
}

func TestResolveOptionalFinOpsURL_UsesOnlyTheContextsTunnel(t *testing.T) {
	t.Setenv("AX_HOME", t.TempDir())
	t.Setenv("AX_FINOPS_SERVER", "")
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			_, _ = io.WriteString(w, "ok\n")
			return
		}
		http.NotFound(w, r)
	}))
	defer healthy.Close()
	_, portStr, _ := net.SplitHostPort(strings.TrimPrefix(healthy.URL, "http://"))
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	// A healthy tunnel saved for another context is not used.
	if err := tunnel.SaveTunnel(&tunnel.TunnelInfo{Context: tunnel.FinOpsTunnelKey("staging"), Port: port}); err != nil {
		t.Fatal(err)
	}
	if got := resolveOptionalFinOpsURL("", "prod"); got != "" {
		t.Errorf("resolveOptionalFinOpsURL for prod = %q, want none (the tunnel belongs to staging)", got)
	}
	if got := resolveOptionalFinOpsURL("", "staging"); got != "http://127.0.0.1:"+portStr {
		t.Errorf("resolveOptionalFinOpsURL for staging = %q", got)
	}
	if got := resolveOptionalFinOpsURL("http://explicit:9090", "prod"); got != "http://explicit:9090" {
		t.Errorf("an explicit --finops-server was not used: %q", got)
	}
}

// An outcome that something outside the sandbox checked names its evidence;
// one the sandbox reported is labeled self-reported.
func TestCost_OutcomeEvidence(t *testing.T) {
	external := strings.Replace(warningReport, `"outcome": "Failed", "exitCode": 3}`, `"outcome": "Failed", "exitCode": 3, "outcomeSource": "external"}`, 1)
	external = strings.Replace(external, `"noTask": {"actors": 1, "totalCost": {"USD": 0.15}}}`, `"noTask": {"actors": 1, "totalCost": {"USD": 0.15}}, "outcomeSource": "external"}`, 1)
	sandbox := strings.ReplaceAll(external, `"outcomeSource": "external"`, `"outcomeSource": "sandbox"`)
	if external == warningReport {
		t.Fatal("fixture did not change")
	}
	for _, tc := range []struct {
		name, report, list, detail string
	}{
		{"external", external, "By task outcome (evidence: external): ", "Outcome:            Failed (evidence: external: exit code 3)"},
		{"sandbox", sandbox, "By task outcome (self-reported): ", "Outcome:            Failed (self-reported: exit code 3)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := rawServer(t, map[string]string{"/v1/costs": tc.report})
			out, err := captureStdout(t, func() error { return runCost(srv.URL, "", "default", false, []string{"-A"}) })
			if err != nil || !strings.Contains(out, tc.list) {
				t.Errorf("list output lacks %q (%v):\n%s", tc.list, err, out)
			}
			out, err = captureStdout(t, func() error { return runCost(srv.URL, "", "team-a", true, []string{"task", "agent-1"}) })
			if err != nil || !strings.Contains(out, tc.detail) {
				t.Errorf("detail output lacks %q (%v):\n%s", tc.detail, err, out)
			}
		})
	}
}
