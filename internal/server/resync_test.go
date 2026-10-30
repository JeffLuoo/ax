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
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
)

func TestResyncTasks_SendsTheCurrentStateOfEveryStoredTask(t *testing.T) {
	ms := memory.NewStore()
	sub := substratetest.NewServer(t)
	ctx := context.Background()
	// The store keeps no actor phase; the resync reads it from the actor.
	storeTask(t, ms, &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "root", Atespace: "team-a"},
		Status:   &v1alpha1.TaskStatus{Id: "task-root-1"},
	})
	setActorState(sub, "team-a", "root", ateapipb.ActorState_ACTOR_STATE_RUNNING)
	storeTask(t, ms, &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "child", Atespace: "team-a"},
		Spec:     &v1alpha1.TaskSpec{Parent: "root", Budget: &v1alpha1.Budget{MaxUsd: 3}},
		Status:   &v1alpha1.TaskStatus{Id: "task-child-1", ParentId: "task-root-1"},
	})
	setActorState(sub, "team-a", "child", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	saveTask(t, ms, sub, "team-b", "going", v1alpha1.PhaseTerminating, "")
	for i := range 7 {
		saveTask(t, ms, sub, "team-c", fmt.Sprintf("t%d", i), "Running", "")
	}

	rec := &recordingEmitter{}
	srv := server.NewServer(ms, newTestReconciler(sub), server.Options{Events: rec})
	if n := srv.ResyncTasks(ctx, server.TaskResyncOptions{PageSize: 3}); n != 10 {
		t.Fatalf("ResyncTasks sent %d events, want 10", n)
	}

	var names []string
	phases := map[string]string{}
	for _, e := range rec.events {
		names = append(names, e.GetMetadata().GetAtespace()+"/"+e.GetMetadata().GetName())
		phases[e.GetMetadata().GetAtespace()+"/"+e.GetMetadata().GetName()] = e.GetStatus().GetPhase()
		if e.GetMetadata().GetName() == "child" {
			if e.GetSpec().GetParent() != "root" || e.GetStatus().GetParentId() != "task-root-1" || e.GetSpec().GetBudget().GetMaxUsd() != 3 || e.GetStatus().GetPhase() != "Suspended" {
				t.Errorf("resync event for child = %v", e)
			}
		}
	}
	// Each event has the phase that GetTask reports.
	for name, want := range map[string]string{"team-a/root": "Running", "team-a/child": "Suspended", "team-b/going": v1alpha1.PhaseTerminating, "team-c/t0": "Running"} {
		if phases[name] != want {
			t.Errorf("resync event phase for %s = %q, want %q", name, phases[name], want)
		}
	}
	sort.Strings(names)
	if len(names) != 10 || names[0] != "team-a/child" || names[2] != "team-b/going" {
		t.Errorf("resync events for %v, want each stored task once", names)
	}
}

func TestResyncTasks_SkipsATaskThatAnRPCHolds(t *testing.T) {
	ms := memory.NewStore()
	sub := substratetest.NewServer(t)
	saveTask(t, ms, sub, "default", "busy", "Running", "")
	saveTask(t, ms, sub, "default", "idle", "Running", "")
	locker := lock.NewMemoryLocker()
	rec := &recordingEmitter{}
	srv := server.NewServer(ms, newTestReconciler(sub), server.Options{Locker: locker, Events: rec})

	ctx := context.Background()
	unlock, err := locker.Lock(ctx, "task", "default", "busy")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	start := time.Now()
	if n := srv.ResyncTasks(ctx, server.TaskResyncOptions{LockTimeout: 50 * time.Millisecond}); n != 1 {
		t.Errorf("ResyncTasks sent %d events, want 1", n)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("ResyncTasks took %v with one task locked", d)
	}
	if len(rec.events) != 1 || rec.events[0].GetMetadata().GetName() != "idle" {
		t.Errorf("events = %v, want only idle", rec.phases())
	}
}

func TestRunTaskResync_RepeatsAndStopsWithContext(t *testing.T) {
	ms := memory.NewStore()
	sub := substratetest.NewServer(t)
	saveTask(t, ms, sub, "default", "long", "Running", "")
	rec := &recordingEmitter{}
	srv := server.NewServer(ms, newTestReconciler(sub), server.Options{Events: rec})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.RunTaskResync(ctx, server.TaskResyncOptions{Interval: 10 * time.Millisecond})
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for len(rec.phases()) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunTaskResync did not stop")
	}
	if got := rec.phases(); len(got) < 2 || strings.Join(got[:2], ",") != "Running,Running" {
		t.Errorf("event phases = %v, want at least two resyncs", got)
	}
	// A zero interval turns the resync off: it returns at once.
	srv.RunTaskResync(context.Background(), server.TaskResyncOptions{})
}
