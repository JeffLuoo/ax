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
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/google/ax/internal/finops"
	"github.com/google/ax/internal/server"
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

func newTestFinOpsHTTPServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/costs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(finops.CostReport{
			Since:           "2026-10-17T00:00:00Z",
			GeneratedAt:     "2026-10-17T01:00:00Z",
			Totals:          finops.Totals{Calls: 2, InputTokens: 2800, OutputTokens: 600, Cost: finops.MoneyMap{"USD": 0.015525}},
			Compute:         finops.ComputeSummary{Leases: 2, LeaseSeconds: 20, VCPUSeconds: 10, GiBSeconds: 20, Cost: finops.MoneyMap{"USD": 0.000171}},
			TotalCost:       finops.MoneyMap{"USD": 0.016046},
			FullyLoadedCost: finops.MoneyMap{"USD": 0.308161},
			Actors: []finops.ActorCost{
				{
					Atespace:                "otel-bench",
					Name:                    "coding-parent-1",
					UID:                     "f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
					Tenant:                  "otel-bench",
					SubAgents:               1,
					Template:                "agent-template",
					Class:                   "agent",
					IsAgent:                 true,
					Calls:                   1,
					InputTokens:             2000,
					OutputTokens:            400,
					CacheReadInputTokens:    500,
					CacheWriteInputTokens:   250,
					Cost:                    finops.MoneyMap{"USD": 0.013088},
					Compute:                 finops.ComputeSummary{Leases: 1, LeaseSeconds: 12, VCPUSeconds: 6, GiBSeconds: 12, Cost: finops.MoneyMap{"USD": 0.000103}, MeasuredCPUSeconds: 3.12},
					TotalCost:               finops.MoneyMap{"USD": 0.013540},
					MeasuredComputeCost:     finops.MoneyMap{"USD": 0.000042},
					IdleShareCost:           finops.MoneyMap{"USD": 0.145633},
					StorageCost:             finops.MoneyMap{"USD": 0.000350},
					PlatformShareCost:       finops.MoneyMap{"USD": 0.000258},
					HostingCost:             finops.MoneyMap{"USD": 0.146344},
					MeasuredHostingCost:     finops.MoneyMap{"USD": 0.146283},
					FullyLoadedCost:         finops.MoneyMap{"USD": 0.159432},
					MeasuredFullyLoadedCost: finops.MoneyMap{"USD": 0.159371},
					TreeLLMCost:             finops.MoneyMap{"USD": 0.015525},
					TreeComputeCost:         finops.MoneyMap{"USD": 0.000171},
					TreeTotalCost:           finops.MoneyMap{"USD": 0.016046},
					TreeFullyLoadedCost:     finops.MoneyMap{"USD": 0.308161},
					Models: []finops.ModelCost{
						{
							Provider:              "anthropic",
							Model:                 "claude-sonnet-4-5",
							Calls:                 1,
							InputTokens:           2000,
							OutputTokens:          400,
							CacheReadInputTokens:  500,
							CacheWriteInputTokens: 250,
							Cost:                  finops.MoneyMap{"USD": 0.013088},
						},
					},
				},
				{
					Atespace:            "otel-bench",
					Name:                "coding-sub-1",
					UID:                 "a5cbff7e-3268-42c3-ab88-65d9c6c8a66e",
					Tenant:              "otel-bench",
					ParentAgentID:       "f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
					Template:            "agent-template",
					Class:               "agent",
					IsAgent:             true,
					Calls:               1,
					InputTokens:         800,
					OutputTokens:        200,
					Cost:                finops.MoneyMap{"USD": 0.002438},
					Compute:             finops.ComputeSummary{Leases: 1, LeaseSeconds: 8, VCPUSeconds: 4, GiBSeconds: 8, Cost: finops.MoneyMap{"USD": 0.000068}},
					TotalCost:           finops.MoneyMap{"USD": 0.002856},
					HostingCost:         finops.MoneyMap{"USD": 0.146291},
					FullyLoadedCost:     finops.MoneyMap{"USD": 0.148729},
					TreeFullyLoadedCost: finops.MoneyMap{"USD": 0.148729},
				},
			},
			Sessions: []finops.SessionCost{
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
					LLMCost:       finops.MoneyMap{"USD": 0.013088},
					ComputeCost:   finops.MoneyMap{"USD": 0.000103},
					TotalCost:     finops.MoneyMap{"USD": 0.013191},
				},
			},
		})
	})
	mux.HandleFunc("/v1/costs/curve", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(finops.CurveReport{
			TargetUSD:           1.0,
			PoolUtilization:     0.65,
			StoragePerAgentUSD:  0.0115,
			PlatformPerAgentUSD: 0.72,
			Tiers: []finops.TierCurve{
				{
					Tier:                "on-demand",
					PerActiveHour:       0.015393,
					LoadedPerActiveHour: 0.023681,
					Fixed:               0.7315,
					BreakEvenHours:      11.3383,
					AlwaysOn:            18.0185,
					Points: []finops.CurvePoint{
						{ActiveHours: 10, FullyLoaded: 0.9683, UnderTarget: true},
						{ActiveHours: 15, FullyLoaded: 1.0867, UnderTarget: false},
					},
				},
			},
		})
	})
	mux.HandleFunc("/v1/costs/focus", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("format") == "csv" {
			w.Header().Set("Content-Type", "text/csv")
			_, _ = w.Write([]byte("OriginChargeId,AllocatedResourceId,BilledCost\nchg-1,f45d4ab9,0.000103\nchg-1,_unused_capacity,0.436910\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(finops.FocusReport{
			OriginCharge: finops.FocusCharge{
				OriginChargeID:  "chg-otel-bench-pool-1",
				BillingCurrency: "USD",
				ServiceName:     "Compute Engine",
				ResourceName:    "otel-bench-pool-1",
				BilledCost:      0.437595,
			},
			Charges: []finops.FocusCharge{
				{
					OriginChargeID:        "chg-otel-bench-pool-1",
					BillingCurrency:       "USD",
					ServiceName:           "Compute Engine",
					ConsumedQuantity:      12,
					ConsumedUnit:          "Seconds",
					BilledCost:            0.000103,
					AllocatedMethodID:     "booked-share",
					AllocatedResourceID:   "f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
					AllocatedResourceName: "otel-bench/coding-parent-1",
					AllocatedTags:         map[string]string{"ate.finops.tenant": "otel-bench", "ate.template.class": "agent"},
				},
				{
					OriginChargeID:        "chg-otel-bench-pool-1",
					BillingCurrency:       "USD",
					ServiceName:           "Compute Engine",
					ConsumedQuantity:      3560,
					ConsumedUnit:          "Seconds",
					BilledCost:            0.436910,
					AllocatedMethodID:     "booked-share",
					AllocatedResourceID:   "_unused_capacity",
					AllocatedResourceName: "_unused_capacity",
				},
			},
			Reconciliation: &finops.FocusReconciliation{
				OriginCost:  0.437595,
				Allocated:   0.000685,
				Unallocated: 0.436910,
				Agents:      2,
			},
		})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
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
	finopsSrv := newTestFinOpsHTTPServer(t)

	// Start an in-memory AX gRPC server with coding-parent-1 in Running phase.
	s := memory.NewStore()
	if err := s.SaveTask(context.Background(), &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "coding-parent-1", Atespace: "otel-bench"},
		Status:   &v1alpha1.TaskStatus{Phase: "Running", Actor: "coding-parent-1"},
	}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	axSrv := server.NewServer(s).GRPCServer()
	t.Cleanup(axSrv.Stop)
	go func() { _ = axSrv.Serve(listener) }()

	t.Run("list table joined with ax-server", func(t *testing.T) {
		out, err := captureStdout(t, func() error {
			return runCost(finopsSrv.URL, listener.Addr().String(), "otel-bench", true, nil)
		})
		if err != nil {
			t.Fatalf("runCost list failed: %v", err)
		}
		for _, want := range []string{"NAME", "PHASE", "coding-parent-1", "Running", "$0.159432", "$0.308161", "Summary (atespace otel-bench, 2 actors)"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected output to contain %q, got:\n%s", want, out)
			}
		}
	})

	t.Run("task detail report", func(t *testing.T) {
		out, err := captureStdout(t, func() error {
			return runCost(finopsSrv.URL, listener.Addr().String(), "otel-bench", true, []string{"task", "coding-parent-1"})
		})
		if err != nil {
			t.Fatalf("runCost task detail failed: %v", err)
		}
		for _, want := range []string{
			"Actor UID:          f45d4ab9-7dd2-4e78-b8bc-59d6ff76cd29",
			"Task Phase:         Running",
			"1. Variable Hosting Cost",
			"2. Shared Overhead",
			"3. Fully Loaded Hosting Cost",
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
			return runCost(finopsSrv.URL, "", "default", false, []string{"curve"})
		})
		if err != nil {
			t.Fatalf("runCost curve failed: %v", err)
		}
		for _, want := range []string{"Hosting Cost Sensitivity Curve", "on-demand", "11.34 h/mo", "$0.9683"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected curve output to contain %q, got:\n%s", want, out)
			}
		}
	})

	t.Run("focus table and csv", func(t *testing.T) {
		out, err := captureStdout(t, func() error {
			return runCost(finopsSrv.URL, "", "default", false, []string{"focus"})
		})
		if err != nil {
			t.Fatalf("runCost focus failed: %v", err)
		}
		for _, want := range []string{"FOCUS 1.3 Split Cost Allocation Report", "chg-otel-bench-pool-1", "_unused_capacity", "$0.436910"} {
			if !strings.Contains(out, want) {
				t.Fatalf("expected focus output to contain %q, got:\n%s", want, out)
			}
		}

		csvOut, err := captureStdout(t, func() error {
			return runCost(finopsSrv.URL, "", "default", false, []string{"focus", "-o", "csv"})
		})
		if err != nil {
			t.Fatalf("runCost focus csv failed: %v", err)
		}
		if !strings.Contains(csvOut, "_unused_capacity,0.436910") {
			t.Fatalf("unexpected focus CSV output:\n%s", csvOut)
		}
	})
}
