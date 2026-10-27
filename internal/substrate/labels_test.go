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

package substrate

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func heldStatus(t *testing.T, hold, reason string) error {
	t.Helper()
	st, err := status.New(codes.FailedPrecondition, "actor is held").WithDetails(&errdetails.ErrorInfo{
		Reason: ActorHeldReason, Domain: "ate.dev", Metadata: map[string]string{"hold": hold, "reason": reason},
	})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

func TestAsHeld(t *testing.T) {
	wrapped := fmt.Errorf("resuming actor team/a: %w", heldStatus(t, "agent-cost/budget", "exhausted"))
	held, ok := AsHeld(wrapped)
	if !ok || held.Hold != "agent-cost/budget" || held.Reason != "exhausted" {
		t.Fatalf("AsHeld = %+v, %v", held, ok)
	}
	if held.Error() != "actor held by agent-cost/budget: exhausted" {
		t.Errorf("Error() = %q", held.Error())
	}
	if again, ok := AsHeld(fmt.Errorf("x: %w", held)); !ok || again != held {
		t.Error("AsHeld does not find a wrapped *HeldError")
	}
	for _, err := range []error{
		nil,
		status.Error(codes.FailedPrecondition, "not ready"),
		status.Error(codes.Unavailable, "down"),
	} {
		if _, ok := AsHeld(err); ok {
			t.Errorf("AsHeld(%v) reported a hold", err)
		}
	}
	if IsTransientError(wrapped) {
		t.Error("a hold is reported as transient")
	}
}

type labelMockClient struct {
	ateapipb.ControlClient
	labelReqs []*ateapipb.SetActorLabelsRequest
	listReqs  []*ateapipb.ListActorsRequest
	pages     [][]*ateapipb.Actor
	createReq *ateapipb.CreateActorRequest
}

func (m *labelMockClient) SetActorLabels(_ context.Context, req *ateapipb.SetActorLabelsRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.labelReqs = append(m.labelReqs, req)
	return &ateapipb.Actor{Labels: req.GetLabels()}, nil
}

func (m *labelMockClient) ListActors(_ context.Context, req *ateapipb.ListActorsRequest, _ ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	m.listReqs = append(m.listReqs, req)
	i := len(m.listReqs) - 1
	resp := &ateapipb.ListActorsResponse{Actors: m.pages[i]}
	if i+1 < len(m.pages) {
		resp.NextPageToken = fmt.Sprintf("page-%d", i+1)
	}
	return resp, nil
}

func (m *labelMockClient) CreateActor(_ context.Context, req *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	m.createReq = req
	return req.GetActor(), nil
}

func TestClient_SetActorLabels(t *testing.T) {
	m := &labelMockClient{}
	c := &Client{control: m}
	if _, err := c.SetActorLabels(context.Background(), "team", "a", map[string]string{"agent-cost/budget-group": "team/root"}, []string{"agent-cost/warn-at"}); err != nil {
		t.Fatal(err)
	}
	req := m.labelReqs[0]
	if req.GetActor().GetAtespace() != "team" || req.GetActor().GetName() != "a" ||
		req.GetLabels()["agent-cost/budget-group"] != "team/root" || !reflect.DeepEqual(req.GetDeleteKeys(), []string{"agent-cost/warn-at"}) {
		t.Errorf("SetActorLabels request = %v", req)
	}
}

func TestClient_ListActorsFollowsPages(t *testing.T) {
	a := func(n string) *ateapipb.Actor { return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: n}} }
	m := &labelMockClient{pages: [][]*ateapipb.Actor{{a("x"), a("y")}, {a("z")}}}
	c := &Client{control: m}
	actors, err := c.ListActors(context.Background(), "team")
	if err != nil {
		t.Fatal(err)
	}
	if len(actors) != 3 || len(m.listReqs) != 2 || m.listReqs[1].GetOptions().GetPageToken() != "page-1" {
		t.Errorf("ListActors = %d actors over %d calls", len(actors), len(m.listReqs))
	}
}

func TestClient_EnsureActorWithLabels(t *testing.T) {
	m := &labelMockClient{}
	c := &Client{control: m}
	labels := map[string]string{"agent-cost/budget-group": "team/root"}
	if _, err := c.EnsureActorWithLabels(context.Background(), "team", "a", "sys", "tmpl", labels); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.createReq.GetActor().GetLabels(), labels) {
		t.Errorf("CreateActor labels = %v", m.createReq.GetActor().GetLabels())
	}
}
