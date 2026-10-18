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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// fakeRouter stands in for the atenet router: it answers GET /statusz for the
// actor in the ate-target-actor header.
type fakeRouter struct {
	mu       sync.Mutex
	states   map[string]string // actor -> JSON body
	asked    []string
	inFlight int
	maxIn    int
	delay    time.Duration
}

func (f *fakeRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	actor := r.Header.Get("ate-target-actor")
	f.mu.Lock()
	f.asked = append(f.asked, actor)
	f.inFlight++
	f.maxIn = max(f.maxIn, f.inFlight)
	body, ok := f.states[actor]
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()
	time.Sleep(f.delay)
	if r.URL.Path != "/statusz" || r.Method != http.MethodGet || !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, body)
}

func (f *fakeRouter) askedFor(actor string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, a := range f.asked {
		if a == actor {
			return true
		}
	}
	return false
}

func saveTask(t *testing.T, ms *memory.MemoryStore, atespace, name, phase, outcome string) {
	t.Helper()
	if err := ms.SaveTask(context.Background(), &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: atespace},
		Spec:     &v1alpha1.TaskSpec{Parent: "", Budget: &v1alpha1.Budget{MaxUsd: 1}},
		Status:   &v1alpha1.TaskStatus{Phase: phase, Outcome: outcome},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPollOutcomes_SetsOutcomeOfExitedRunningTasks(t *testing.T) {
	ms := memory.NewStore()
	saveTask(t, ms, "team-a", "done", "Running", "")
	saveTask(t, ms, "team-a", "broken", "Running", "")
	saveTask(t, ms, "team-a", "busy", "Running", "")
	saveTask(t, ms, "team-a", "asleep", "Suspended", "")
	saveTask(t, ms, "team-a", "settled", "Running", v1alpha1.OutcomeCompleted)

	router := &fakeRouter{states: map[string]string{
		"team-a/done":    `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:30:00Z"}`,
		"team-a/broken":  `{"state":"exited","exitCode":7,"finishedAt":"2026-10-18T08:31:00Z"}`,
		"team-a/busy":    `{"state":"running"}`,
		"team-a/asleep":  `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:32:00Z"}`,
		"team-a/settled": `{"state":"exited","exitCode":9,"finishedAt":"2026-10-18T08:33:00Z"}`,
	}}
	hs := httptest.NewServer(router)
	defer hs.Close()

	rec := &recordingEmitter{}
	srv := server.NewServer(ms, server.Options{Events: rec})
	ctx := context.Background()
	watch, closer, err := ms.WatchTask(ctx, "team-a", "done")
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()

	opts := server.OutcomePollerOptions{RouterAddr: strings.TrimPrefix(hs.URL, "http://")}
	if n := srv.PollOutcomes(ctx, opts); n != 2 {
		t.Fatalf("PollOutcomes set %d outcomes, want 2", n)
	}

	for name, want := range map[string]struct {
		outcome string
		code    int32
		at      string
	}{
		"done":   {v1alpha1.OutcomeCompleted, 0, "2026-10-18T08:30:00Z"},
		"broken": {v1alpha1.OutcomeFailed, 7, "2026-10-18T08:31:00Z"},
	} {
		got, err := ms.GetTask(ctx, "team-a", name)
		if err != nil {
			t.Fatal(err)
		}
		st := got.GetStatus()
		if st.GetOutcome() != want.outcome || st.GetExitCode() != want.code || st.GetFinishedAt().AsTime().Format(time.RFC3339) != want.at || st.GetPhase() != "Running" {
			t.Errorf("%s status = %v, want outcome %s exit %d at %s", name, st, want.outcome, want.code, want.at)
		}
	}
	if got, _ := ms.GetTask(ctx, "team-a", "busy"); got.GetStatus().GetOutcome() != "" {
		t.Errorf("a running command got outcome %q", got.GetStatus().GetOutcome())
	}
	if got, _ := ms.GetTask(ctx, "team-a", "settled"); got.GetStatus().GetOutcome() != v1alpha1.OutcomeCompleted || got.GetStatus().GetExitCode() != 0 {
		t.Errorf("a task with an outcome was changed: %v", got.GetStatus())
	}
	// The router resumes a suspended actor to answer, so the poller must not ask.
	if router.askedFor("team-a/asleep") {
		t.Error("the poller asked the router about a suspended task")
	}
	if router.askedFor("team-a/settled") {
		t.Error("the poller asked about a task that already has an outcome")
	}

	// Watchers see the update, and FinOps gets an event with the outcome.
	select {
	case w := <-watch:
		if w.GetStatus().GetOutcome() != v1alpha1.OutcomeCompleted {
			t.Errorf("watch event = %v", w.GetStatus())
		}
	case <-time.After(2 * time.Second):
		t.Error("no watch event for the outcome")
	}
	outcomes := map[string]string{}
	for _, e := range rec.events {
		outcomes[e.GetMetadata().GetName()] = e.GetStatus().GetOutcome()
	}
	if len(rec.events) != 2 || outcomes["done"] != v1alpha1.OutcomeCompleted || outcomes["broken"] != v1alpha1.OutcomeFailed {
		t.Errorf("events = %v, want one for each outcome", outcomes)
	}

	// A second pass changes nothing.
	if n := srv.PollOutcomes(ctx, opts); n != 0 {
		t.Errorf("second PollOutcomes set %d outcomes, want 0", n)
	}
}

func TestPollOutcomes_WaitsForTheTaskLockAndRechecksThePhase(t *testing.T) {
	ms := memory.NewStore()
	saveTask(t, ms, "default", "racer", "Running", "")
	router := &fakeRouter{states: map[string]string{
		"default/racer": `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:30:00Z"}`,
	}}
	hs := httptest.NewServer(router)
	defer hs.Close()
	locker := lock.NewMemoryLocker()
	srv := server.NewServer(ms, server.Options{Locker: locker})

	// A suspend holds the lock while the poller starts.
	ctx := context.Background()
	unlock, err := locker.Lock(ctx, "task", "default", "racer")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan int)
	go func() {
		done <- srv.PollOutcomes(ctx, server.OutcomePollerOptions{RouterAddr: strings.TrimPrefix(hs.URL, "http://")})
	}()
	time.Sleep(100 * time.Millisecond)
	saveTask(t, ms, "default", "racer", "Suspended", "")
	unlock()

	if n := <-done; n != 0 {
		t.Errorf("PollOutcomes set %d outcomes for a task that was suspended meanwhile", n)
	}
	if router.askedFor("default/racer") {
		t.Error("the poller asked the router about a task that was suspended while it waited for the lock")
	}
}

func TestPollOutcomes_BoundsConcurrencyAndIgnoresBadAnswers(t *testing.T) {
	ms := memory.NewStore()
	states := map[string]string{}
	for i := range 6 {
		name := fmt.Sprintf("t%d", i)
		saveTask(t, ms, "default", name, "Running", "")
		states["default/"+name] = `{"state":"exited","exitCode":0}`
	}
	saveTask(t, ms, "default", "garbage", "Running", "")
	states["default/garbage"] = `{not json`
	router := &fakeRouter{states: states, delay: 50 * time.Millisecond}
	hs := httptest.NewServer(router)
	defer hs.Close()
	srv := server.NewServer(ms)

	n := srv.PollOutcomes(context.Background(), server.OutcomePollerOptions{RouterAddr: strings.TrimPrefix(hs.URL, "http://"), Concurrency: 2})
	if n != 6 {
		t.Errorf("PollOutcomes set %d outcomes, want 6", n)
	}
	if router.maxIn > 2 {
		t.Errorf("%d requests in flight, want at most 2", router.maxIn)
	}
	if got, _ := ms.GetTask(context.Background(), "default", "garbage"); got.GetStatus().GetOutcome() != "" {
		t.Errorf("an undecodable answer set outcome %q", got.GetStatus().GetOutcome())
	}
	// No finishedAt in the answer: the poller uses the time it saw the exit.
	if got, _ := ms.GetTask(context.Background(), "default", "t0"); got.GetStatus().GetFinishedAt() == nil {
		t.Error("finishedAt not set")
	}
}

func TestRunOutcomePoller_StopsWithContext(t *testing.T) {
	srv := server.NewServer(memory.NewStore())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.RunOutcomePoller(ctx, server.OutcomePollerOptions{RouterAddr: "127.0.0.1:1", Interval: 10 * time.Millisecond})
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunOutcomePoller did not stop")
	}
	// Without a router address it returns at once.
	srv.RunOutcomePoller(context.Background(), server.OutcomePollerOptions{})
}
