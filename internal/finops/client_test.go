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
	"net/http"
	"net/http/httptest"
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

	actor, sessions, children, _, err := c.GetActorCost(ctx, "otel-bench", "coding-parent-1")
	if err != nil {
		t.Fatalf("GetActorCost by name failed: %v", err)
	}
	if actor.UID != "6dadd6f6-2288-4dad-89fc-2134253d9b43" {
		t.Fatalf("unexpected UID: %s", actor.UID)
	}
	if len(sessions) != 1 || sessions[0].SessionID != "conv-live-e2e-001" {
		t.Fatalf("unexpected sessions: %+v", sessions)
	}
	if len(children) != 1 || children[0].Name != "coding-sub-1" {
		t.Fatalf("unexpected children: %+v", children)
	}
	if got := actor.VariableHostingUSD(); got <= 0 {
		t.Fatalf("expected positive variable hosting cost, got %f", got)
	}
	if got := actor.SharedOverheadUSD(); got <= 0 {
		t.Fatalf("expected positive shared overhead cost, got %f", got)
	}

	// Lookup by UID prefix should also work.
	byUID, _, _, _, err := c.GetActorCost(ctx, "", "2a93284b")
	if err != nil {
		t.Fatalf("GetActorCost by UID prefix failed: %v", err)
	}
	if byUID.Name != "coding-sub-1" {
		t.Fatalf("expected coding-sub-1, got %s", byUID.Name)
	}
}
