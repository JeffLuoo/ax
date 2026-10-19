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

package events_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"

	"github.com/google/ax/internal/events"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

func task() *v1alpha1.Task {
	return &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "child", Atespace: "team-a"},
		Spec:     &v1alpha1.TaskSpec{Parent: "root", Budget: &v1alpha1.Budget{MaxUsd: 2.5}},
		Status: &v1alpha1.TaskStatus{Phase: "Running", Outcome: v1alpha1.OutcomeFailed, ExitCode: 3,
			Id: "task-child-1-ab", ParentId: "task-root-1-cd", CreatedBy: "alice@example.com"},
	}
}

// collector is an OTLP/gRPC logs receiver that keeps what it receives.
type collector struct {
	collogspb.UnimplementedLogsServiceServer
	mu   sync.Mutex
	reqs []*collogspb.ExportLogsServiceRequest
}

func (c *collector) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs = append(c.reqs, req)
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func value(v *commonpb.AnyValue) any {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonpb.AnyValue_IntValue:
		return x.IntValue
	}
	return nil
}

func TestNewFromEnv_Disabled(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	e, err := events.NewFromEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(events.Nop); !ok {
		t.Fatalf("NewFromEnv without an endpoint = %T, want events.Nop", e)
	}
}

// TestNewFromEnv_SendsEventOverOTLP sends one event through the real SDK and
// exporter to a local OTLP receiver, and checks the record on the wire.
func TestNewFromEnv_SendsEventOverOTLP(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{}
	srv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(srv, c)
	go srv.Serve(lis)
	defer srv.Stop()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+lis.Addr().String())
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	e, err := events.NewFromEnv(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*events.OTLP); !ok {
		t.Fatalf("NewFromEnv with an endpoint = %T, want *events.OTLP", e)
	}

	ctx, cancel := context.WithCancel(context.Background())
	e.TaskChanged(ctx, task())
	cancel() // the record must still go out after the caller's context ends
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if err := e.Shutdown(sctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	var records []map[string]any
	var names []string
	var service string
	for _, req := range c.reqs {
		for _, rl := range req.GetResourceLogs() {
			for _, kv := range rl.GetResource().GetAttributes() {
				if kv.GetKey() == "service.name" {
					service = kv.GetValue().GetStringValue()
				}
			}
			for _, sl := range rl.GetScopeLogs() {
				for _, lr := range sl.GetLogRecords() {
					names = append(names, lr.GetEventName())
					attrs := map[string]any{}
					for _, kv := range lr.GetAttributes() {
						attrs[kv.GetKey()] = value(kv.GetValue())
					}
					records = append(records, attrs)
				}
			}
		}
	}
	if len(records) != 1 || names[0] != events.EventName {
		t.Fatalf("received %d records with names %v, want one %s", len(records), names, events.EventName)
	}
	if service != "ax-server" {
		t.Errorf("service.name = %q, want ax-server", service)
	}
	want := map[string]any{
		"ax.atespace":        "team-a",
		"ax.task.name":       "child",
		"ax.task.parent":     "root",
		"ax.task.id":         "task-child-1-ab",
		"ax.task.parent_id":  "task-root-1-cd",
		"ax.task.created_by": "alice@example.com",
		"ax.task.budget_usd": 2.5,
		"ax.task.phase":      "Running",
		"ax.task.outcome":    "Failed",
		"ax.task.exit_code":  int64(3),
		"ate.atespace":       "team-a",
		"ate.actor.name":     "child",
		// The outcome is the runner's exit code, from inside the sandbox.
		"ax.task.outcome_source": "sandbox",
	}
	got := records[0]
	if len(got) != len(want) {
		t.Errorf("attributes = %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("attribute %s = %#v, want %#v", k, got[k], v)
		}
	}
}

func TestAttributes_RootTaskWithoutBudget(t *testing.T) {
	attrs := map[string]any{}
	for _, kv := range events.Attributes(&v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "solo"}}) {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	if attrs[events.AttrAtespace] != "default" || attrs[events.AttrAteSpace] != "default" {
		t.Errorf("atespace attributes = %v, want default", attrs)
	}
	if attrs[events.AttrParent] != "" || attrs[events.AttrBudgetUSD] != "0" || attrs[events.AttrOutcome] != "" {
		t.Errorf("root task attributes = %v", attrs)
	}
	if _, ok := attrs[events.AttrOutcomeSource]; ok {
		t.Errorf("outcome source without an outcome: %v", attrs)
	}
}
