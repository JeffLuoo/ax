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

package finops

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const sampleCostsJSON = `{
  "since": "2026-10-17T01:43:49Z",
  "generatedAt": "2026-10-17T01:47:32Z",
  "totals": {
    "calls": 16,
    "inputTokens": 40800,
    "outputTokens": 9760,
    "cacheReadInputTokens": 22600,
    "cacheWriteInputTokens": 1000,
    "reasoningOutputTokens": 2760,
    "cost": {"USD": 0.131825},
    "usageStatus": {"ok": 16}
  },
  "compute": {
    "leases": 3,
    "openLeases": 0,
    "leaseSeconds": 123.76,
    "vcpuSeconds": 50.11,
    "gibSeconds": 100.22,
    "cost": {"USD": 0.000558},
    "measuredCost": {"USD": 0.000573},
    "measuredCpuSeconds": 51.52
  },
  "totalCost": {"USD": 0.132383},
  "fullyLoadedCost": {"USD": 0.132660},
  "actors": [
    {
      "atespace": "otel-bench",
      "name": "coding-parent-1",
      "uid": "6dadd6f6-2288-4dad-89fc-2134253d9b43",
      "tenant": "tenant-alpha",
      "subAgents": 1,
      "template": "otel-bench/finops-agent-parent",
      "class": "agent",
      "isAgent": true,
      "calls": 8,
      "inputTokens": 20400,
      "outputTokens": 4880,
      "cacheReadInputTokens": 11300,
      "cacheWriteInputTokens": 500,
      "reasoningOutputTokens": 1380,
      "cost": {"USD": 0.0659125},
      "usageStatus": {"ok": 8},
      "compute": {
        "leases": 1,
        "leaseSeconds": 76.68,
        "vcpuSeconds": 38.34,
        "gibSeconds": 76.68,
        "cost": {"USD": 0.000427},
        "measuredCost": {"USD": 0.000355},
        "measuredCpuSeconds": 31.92
      },
      "totalCost": {"USD": 0.066339},
      "measuredComputeCost": {"USD": 0.000355},
      "idleShareCost": {"USD": 0.000210},
      "storageCost": {"USD": 0.000002},
      "hostingCost": {"USD": 0.000639},
      "measuredHostingCost": {"USD": 0.000567},
      "fullyLoadedCost": {"USD": 0.066551},
      "measuredFullyLoadedCost": {"USD": 0.066480},
      "treeLlmCost": {"USD": 0.131825},
      "treeComputeCost": {"USD": 0.000558},
      "treeTotalCost": {"USD": 0.132383},
      "treeFullyLoadedCost": {"USD": 0.132660},
      "models": [
        {
          "provider": "anthropic",
          "model": "claude-sonnet-4-5-20250929",
          "calls": 2,
          "inputTokens": 8600,
          "outputTokens": 950,
          "cacheReadInputTokens": 5400,
          "cacheWriteInputTokens": 500,
          "cost": {"USD": 0.025845}
        }
      ]
    },
    {
      "atespace": "otel-bench",
      "name": "coding-sub-1",
      "uid": "2a93284b-6cf5-4b40-a5bf-c2df5d0a9367",
      "tenant": "tenant-alpha",
      "parentAgentId": "6dadd6f6-2288-4dad-89fc-2134253d9b43",
      "template": "otel-bench/finops-agent-sub",
      "class": "agent",
      "isAgent": true,
      "calls": 8,
      "inputTokens": 20400,
      "outputTokens": 4880,
      "cost": {"USD": 0.0659125},
      "compute": {
        "leases": 2,
        "leaseSeconds": 47.08,
        "vcpuSeconds": 11.77,
        "gibSeconds": 23.54,
        "cost": {"USD": 0.000131}
      },
      "totalCost": {"USD": 0.066044},
      "fullyLoadedCost": {"USD": 0.066109},
      "treeTotalCost": {"USD": 0.066044},
      "treeFullyLoadedCost": {"USD": 0.066109}
    }
  ],
  "sessions": [
    {
      "sessionId": "conv-live-e2e-001",
      "fromHeader": true,
      "atespace": "otel-bench",
      "actor": "otel-bench/coding-parent-1",
      "actorUid": "6dadd6f6-2288-4dad-89fc-2134253d9b43",
      "epoch": "1792201452129038207",
      "calls": 8,
      "inputTokens": 20400,
      "outputTokens": 4880,
      "activeSeconds": 0.074,
      "activeShare": 1.0,
      "llmCost": {"USD": 0.0659125},
      "computeCost": {"USD": 0.000427},
      "totalCost": {"USD": 0.066339}
    }
  ]
}`

func TestClientGetActorCost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/costs" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sampleCostsJSON))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, srv.Client())
	ctx := context.Background()

	m, err := c.GetActorCost(ctx, "otel-bench", true, "coding-parent-1")
	if err != nil {
		t.Fatalf("GetActorCost by name failed: %v", err)
	}
	actor := m.Actor
	if actor.UID != "6dadd6f6-2288-4dad-89fc-2134253d9b43" {
		t.Fatalf("unexpected UID: %s", actor.UID)
	}
	if len(m.SessionCosts) != 1 || m.SessionCosts[0].SessionID != "conv-live-e2e-001" || len(m.Sessions) != 1 {
		t.Fatalf("unexpected sessions: %+v", m.SessionCosts)
	}
	if len(m.Children) != 1 || m.Children[0].Name != "coding-sub-1" || len(m.SubAgents) != 1 {
		t.Fatalf("unexpected children: %+v", m.Children)
	}
	if !strings.Contains(string(m.Raw), `"measuredFullyLoadedCost"`) {
		t.Errorf("raw actor row lost fields: %s", m.Raw)
	}
	if got := actor.VariableHostingUSD(); got <= 0 {
		t.Fatalf("expected positive variable hosting cost, got %f", got)
	}

	// A unique UID prefix of at least MinUIDPrefix characters works.
	byUID, err := c.GetActorCost(ctx, "default", false, "2a93284b")
	if err != nil {
		t.Fatalf("GetActorCost by UID prefix failed: %v", err)
	}
	if byUID.Actor.Name != "coding-sub-1" {
		t.Fatalf("expected coding-sub-1, got %s", byUID.Actor.Name)
	}
	// A shorter prefix is not a match.
	if _, err := c.GetActorCost(ctx, "default", false, "2a93"); err == nil {
		t.Error("a 4-character UID prefix matched")
	}
}

// lookupServer serves a report with actors in several atespaces and records
// the atespace each request asked for.
func lookupServer(t *testing.T, asked *[]string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		scope := r.URL.Query().Get("atespace")
		*asked = append(*asked, scope)
		actors := []map[string]any{
			{"atespace": "team-a", "name": "test", "uid": "aaaaaaaa-1111-0000-0000-000000000000"},
			{"atespace": "team-b", "name": "test", "uid": "bbbbbbbb-2222-0000-0000-000000000000"},
			{"atespace": "team-b", "name": "only-b", "uid": "cccccccc-3333-0000-0000-000000000000"},
			{"atespace": "team-c", "name": "again", "uid": "dddddddd-4444-0000-0000-000000000000"},
			{"atespace": "team-c", "name": "again", "uid": "dddddddd-5555-0000-0000-000000000000"},
			{"atespace": "", "name": "", "note": "unknown actor"},
		}
		var rows []map[string]any
		for _, a := range actors {
			if scope == "" || a["atespace"] == scope {
				rows = append(rows, a)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"atespace": scope, "actors": rows})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestClientGetActorCost_LookupRules(t *testing.T) {
	var asked []string
	c := NewClient(lookupServer(t, &asked).URL, nil)
	ctx := context.Background()

	// An explicit atespace never matches outside it, and the server filters.
	if _, err := c.GetActorCost(ctx, "team-a", true, "only-b"); err == nil {
		t.Error("an explicit -a team-a matched team-b/only-b")
	}
	if asked[len(asked)-1] != "team-a" {
		t.Errorf("request asked for atespace %q, want team-a", asked[len(asked)-1])
	}
	m, err := c.GetActorCost(ctx, "team-a", true, "test")
	if err != nil || m.Actor.Atespace != "team-a" {
		t.Fatalf("team-a/test = %+v, %v", m, err)
	}

	// Without -a, the given atespace comes first, then all atespaces.
	if m, err := c.GetActorCost(ctx, "default", false, "only-b"); err != nil || m.Actor.Atespace != "team-b" {
		t.Errorf("only-b without -a = %+v, %v", m, err)
	}
	var amb *AmbiguousError
	if _, err := c.GetActorCost(ctx, "default", false, "test"); !errors.As(err, &amb) || len(amb.Candidates) != 2 {
		t.Errorf("a name in two atespaces: got %v, want an AmbiguousError with 2 candidates", err)
	}
	if m, err := c.GetActorCost(ctx, "team-b", false, "test"); err != nil || m.Actor.Atespace != "team-b" {
		t.Errorf("the default atespace wins: %+v, %v", m, err)
	}

	// "<atespace>/<name>" names the atespace.
	if m, err := c.GetActorCost(ctx, "default", false, "team-b/test"); err != nil || m.Actor.UID != "bbbbbbbb-2222-0000-0000-000000000000" {
		t.Errorf("team-b/test = %+v, %v", m, err)
	}

	// An actor made again under the same name has two records: ambiguous.
	if _, err := c.GetActorCost(ctx, "team-c", true, "again"); !errors.As(err, &amb) {
		t.Errorf("a recreated actor: got %v, want an AmbiguousError", err)
	}
	if m, err := c.GetActorCost(ctx, "team-c", true, "dddddddd-5555-0000-0000-000000000000"); err != nil || m.Actor.UID != "dddddddd-5555-0000-0000-000000000000" {
		t.Errorf("exact UID = %+v, %v", m, err)
	}
	// A UID prefix that two actors share is ambiguous.
	if _, err := c.GetActorCost(ctx, "default", false, "dddddddd"); !errors.As(err, &amb) {
		t.Errorf("a shared UID prefix: got %v, want an AmbiguousError", err)
	}
	// A note row has no name and never matches.
	if _, err := c.GetActorCost(ctx, "default", false, ""); err == nil {
		t.Error("an empty name matched a note row")
	}
}

func TestClientSendsAtespaceAndKeepsRawBody(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"atespace":"team-a","costBasis":"estimated","futureField":{"x":1},"actors":[],"totals":{"unknownCostCalls":2}}`))
	}))
	defer srv.Close()
	report, raw, err := NewClient(srv.URL, nil).GetCosts(context.Background(), "team-a")
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != "atespace=team-a" {
		t.Errorf("query = %q, want atespace=team-a", gotQuery)
	}
	if !strings.Contains(string(raw), "futureField") {
		t.Errorf("raw body lost a field the client does not know: %s", raw)
	}
	if !report.Estimated() || report.Totals.UnknownCostCalls != 2 || report.Atespace != "team-a" {
		t.Errorf("report = %+v", report)
	}
	if (CostReport{CostBasis: "reconciled"}).Estimated() {
		t.Error("a reconciled report counted as an estimate")
	}
	if !(CostReport{}).Estimated() {
		t.Error("a report with no cost basis did not count as an estimate")
	}
}

func TestClientGetActorCost_RecreatedParentSubAgentsMatchCanonicalOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "actors": [
    {
      "atespace": "otel-bench",
      "name": "ax-parent-r2",
      "uid": "e32d3de1-4053-4c32-8068-0fa736b02d69",
      "tenant": "tenant-alpha",
      "subAgents": 0,
      "compute": {"lastLease": "2026-10-18T16:23:19Z"}
    },
    {
      "atespace": "otel-bench",
      "name": "ax-parent-r2",
      "uid": "b1cb5cc0-5acc-4c1f-8657-1703688f5883",
      "tenant": "tenant-alpha",
      "subAgents": 1,
      "compute": {"lastLease": "2026-10-18T16:28:12Z"}
    },
    {
      "atespace": "otel-bench",
      "name": "ax-child-r2",
      "uid": "e946925b-9f51-4ef4-b9d8-18fd0d9ca8a1",
      "tenant": "tenant-alpha",
      "parentAgentId": "otel-bench/ax-parent-r2"
    }
  ]
}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, srv.Client())
	ctx := context.Background()

	oldParent, err := c.GetActorCost(ctx, "otel-bench", true, "e32d3de1")
	if err != nil {
		t.Fatalf("old parent lookup failed: %v", err)
	}
	if len(oldParent.Children) != 0 {
		t.Errorf("old superseded parent got %d children (%+v), want 0", len(oldParent.Children), oldParent.Children)
	}

	newParent, err := c.GetActorCost(ctx, "otel-bench", true, "b1cb5cc0")
	if err != nil {
		t.Fatalf("new parent lookup failed: %v", err)
	}
	if len(newParent.Children) != 1 || newParent.Children[0].UID != "e946925b-9f51-4ef4-b9d8-18fd0d9ca8a1" {
		t.Errorf("new canonical parent got children %+v, want [e946925b]", newParent.Children)
	}
}
