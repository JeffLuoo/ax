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

package server_test

import (
	"context"
	"testing"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newForkTestServer(t *testing.T) (*server.Server, *substratetest.Server) {
	t.Helper()
	sub := substratetest.NewServer(t)
	srv := server.NewServer(memory.NewStore(), newTestReconciler(sub))
	ctx := context.Background()
	if _, err := srv.CreateTask(ctx, &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "coder", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Commands: []string{"sleep", "infinity"}},
	}}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := srv.ResumeTask(ctx, &v1alpha1.ResumeTaskRequest{Atespace: "default", Name: "coder"}); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	return srv, sub
}

func TestForkTaskCreatesForksFromOneTag(t *testing.T) {
	srv, sub := newForkTestServer(t)
	ctx := context.Background()

	resp, err := srv.ForkTask(ctx, &v1alpha1.ForkTaskRequest{
		Atespace:  "default",
		Name:      "coder",
		Tag:       "impl-done",
		ForkNames: []string{"verify-ui", "verify-chaos"},
	})
	if err != nil {
		t.Fatalf("ForkTask: %v", err)
	}
	if resp.GetTag() != "impl-done" || len(resp.GetForks()) != 2 {
		t.Fatalf("ForkTask = tag %q, %d forks; want impl-done and 2 forks", resp.GetTag(), len(resp.GetForks()))
	}
	if _, ok := sub.Tag("default", "impl-done"); !ok {
		t.Fatal("ForkTask did not create the tag")
	}
	source, ok := sub.Actor("default", "coder")
	if !ok {
		t.Fatal("source actor is missing")
	}
	if source.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("source actor state = %v, want suspended", source.GetStatus().GetState())
	}
	for _, name := range []string{"verify-ui", "verify-chaos"} {
		got, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "default", Name: name})
		if err != nil {
			t.Fatalf("GetTask(%s): %v", name, err)
		}
		fs := got.GetSpec().GetForkSource()
		if fs.GetTask() != "coder" || fs.GetTag() != "impl-done" {
			t.Errorf("%s forkSource = %v, want coder/impl-done", name, fs)
		}
		if got.GetSpec().GetSessionId() == "" || got.GetSpec().GetSessionId() == got.GetMetadata().GetName() {
			t.Errorf("%s session_id = %q, want a new ID", name, got.GetSpec().GetSessionId())
		}
		actor, ok := sub.Actor("default", name)
		if !ok {
			t.Fatalf("actor %s is missing", name)
		}
		if actor.GetSourceTag().GetName() != "impl-done" {
			t.Errorf("actor %s source_tag = %v, want impl-done", name, actor.GetSourceTag())
		}
		if actor.GetActorTemplate().GetName() != source.GetActorTemplate().GetName() {
			t.Errorf("actor %s template = %s, want the source template %s", name, actor.GetActorTemplate().GetName(), source.GetActorTemplate().GetName())
		}
	}
}

func TestForkTaskResumesForks(t *testing.T) {
	srv, sub := newForkTestServer(t)
	if _, err := srv.ForkTask(context.Background(), &v1alpha1.ForkTaskRequest{
		Atespace:  "default",
		Name:      "coder",
		ForkNames: []string{"verify-fuzz"},
		Resume:    true,
	}); err != nil {
		t.Fatalf("ForkTask: %v", err)
	}
	actor, ok := sub.Actor("default", "verify-fuzz")
	if !ok {
		t.Fatal("fork actor is missing")
	}
	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("fork actor state = %v, want running", actor.GetStatus().GetState())
	}
}

func TestForkTaskRejectsBadRequests(t *testing.T) {
	srv, _ := newForkTestServer(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		req  *v1alpha1.ForkTaskRequest
		want codes.Code
	}{
		{"no forks", &v1alpha1.ForkTaskRequest{Name: "coder"}, codes.InvalidArgument},
		{"fork named after the source", &v1alpha1.ForkTaskRequest{Name: "coder", ForkNames: []string{"coder"}}, codes.InvalidArgument},
		{"duplicate fork name", &v1alpha1.ForkTaskRequest{Name: "coder", ForkNames: []string{"a", "a"}}, codes.InvalidArgument},
		{"missing source", &v1alpha1.ForkTaskRequest{Name: "nope", ForkNames: []string{"a"}}, codes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := srv.ForkTask(ctx, tc.req); status.Code(err) != tc.want {
				t.Errorf("ForkTask = %v, want %v", err, tc.want)
			}
		})
	}
	if _, err := srv.ForkTask(ctx, &v1alpha1.ForkTaskRequest{Name: "coder", ForkNames: []string{"f1"}}); err != nil {
		t.Fatalf("ForkTask: %v", err)
	}
	if _, err := srv.ForkTask(ctx, &v1alpha1.ForkTaskRequest{Name: "coder", ForkNames: []string{"f1"}}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("ForkTask onto an existing task = %v, want AlreadyExists", err)
	}
}

func TestCreateTaskRejectsForkSource(t *testing.T) {
	srv, _ := newForkTestServer(t)
	_, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "sneaky", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{ForkSource: &v1alpha1.ForkSource{Task: "coder", Tag: "t"}},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateTask with forkSource = %v, want InvalidArgument", err)
	}
}
