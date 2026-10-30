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

	"github.com/google/ax/internal/events"
	"github.com/google/ax/internal/lock"
	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/internal/substrate/substratetest"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/proto"
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

// saveTask stores a task that GetTask reports with phase. The store keeps
// only the phases of AX's own (Pending, Failed, Terminating); for the others,
// the task's actor on sub gets the matching state.
func saveTask(t *testing.T, ms *memory.MemoryStore, sub *substratetest.Server, atespace, name, phase, outcome string) {
	t.Helper()
	stored := ""
	state := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	switch phase {
	case v1alpha1.PhaseRunning:
		state = ateapipb.ActorState_ACTOR_STATE_RUNNING
	case v1alpha1.PhaseSuspended:
	default:
		stored = phase
	}
	storeTask(t, ms, &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name, Atespace: atespace},
		Spec:     &v1alpha1.TaskSpec{Parent: "", Budget: &v1alpha1.Budget{MaxUsd: 1}},
		Status:   &v1alpha1.TaskStatus{Phase: stored, Outcome: outcome},
	})
	setActorState(sub, atespace, name, state)
}

// storeTask stores task with its status as given. The store saves an empty
// phase as Pending, so storeTask writes the status again.
func storeTask(t *testing.T, ms *memory.MemoryStore, task *v1alpha1.Task) {
	t.Helper()
	ctx := context.Background()
	st := proto.Clone(task.GetStatus()).(*v1alpha1.TaskStatus)
	if err := ms.SaveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := ms.UpdateTaskStatus(ctx, task.GetMetadata().GetAtespace(), task.GetMetadata().GetName(), st); err != nil {
		t.Fatal(err)
	}
}

// setActorState gives the named actor on sub the given state, as Substrate
// can change an actor without AX.
func setActorState(sub *substratetest.Server, atespace, name string, state ateapipb.ActorState) {
	sub.SetActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: atespace, Name: name},
		Status:   &ateapipb.ActorStatus{State: state},
	})
}

func TestPollOutcomes_SetsOutcomeOfExitedRunningTasks(t *testing.T) {
	ms := memory.NewStore()
	sub := substratetest.NewServer(t)
	saveTask(t, ms, sub, "team-a", "done", "Running", "")
	saveTask(t, ms, sub, "team-a", "broken", "Running", "")
	saveTask(t, ms, sub, "team-a", "busy", "Running", "")
	saveTask(t, ms, sub, "team-a", "asleep", "Suspended", "")
	saveTask(t, ms, sub, "team-a", "settled", "Running", v1alpha1.OutcomeCompleted)
	saveTask(t, ms, sub, "team-a", "failing", v1alpha1.PhaseFailed, "")
	// The actor runs, but the stored phase Failed is what GetTask reports.
	setActorState(sub, "team-a", "failing", ateapipb.ActorState_ACTOR_STATE_RUNNING)

	router := &fakeRouter{states: map[string]string{
		"team-a/done":    `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:30:00Z"}`,
		"team-a/broken":  `{"state":"exited","exitCode":7,"finishedAt":"2026-10-18T08:31:00Z"}`,
		"team-a/busy":    `{"state":"running"}`,
		"team-a/asleep":  `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:32:00Z"}`,
		"team-a/settled": `{"state":"exited","exitCode":9,"finishedAt":"2026-10-18T08:33:00Z"}`,
		"team-a/failing": `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:34:00Z"}`,
	}}
	hs := httptest.NewServer(router)
	defer hs.Close()

	rec := &recordingEmitter{}
	srv := server.NewServer(ms, newTestReconciler(sub), server.Options{Events: rec})
	ctx := context.Background()

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
		got, err := srv.GetTask(ctx, &v1alpha1.GetTaskRequest{Atespace: "team-a", Name: name})
		if err != nil {
			t.Fatal(err)
		}
		st := got.GetStatus()
		if st.GetOutcome() != want.outcome || st.GetExitCode() != want.code || st.GetFinishedAt().AsTime().Format(time.RFC3339) != want.at || st.GetPhase() != "Running" {
			t.Errorf("%s status = %v, want outcome %s exit %d at %s", name, st, want.outcome, want.code, want.at)
		}
		stored, err := ms.GetTask(ctx, "team-a", name)
		if err != nil || stored.GetStatus().GetOutcome() != want.outcome || stored.GetStatus().GetPhase() != "" {
			t.Errorf("%s stored status = %v, %v, want outcome %s and no phase", name, stored.GetStatus(), err, want.outcome)
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
	if router.askedFor("team-a/failing") {
		t.Error("the poller asked about a task whose stored phase is Failed")
	}

	// A task event is emitted with the outcome and the phase that GetTask
	// reports.
	outcomes := map[string]string{}
	for _, e := range rec.events {
		outcomes[e.GetMetadata().GetName()] = e.GetStatus().GetOutcome()
		if e.GetStatus().GetPhase() != "Running" {
			t.Errorf("event for %s has phase %q, want Running", e.GetMetadata().GetName(), e.GetStatus().GetPhase())
		}
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
	sub := substratetest.NewServer(t)
	saveTask(t, ms, sub, "default", "racer", "Running", "")
	router := &fakeRouter{states: map[string]string{
		"default/racer": `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:30:00Z"}`,
	}}
	hs := httptest.NewServer(router)
	defer hs.Close()
	locker := lock.NewMemoryLocker()
	srv := server.NewServer(ms, newTestReconciler(sub), server.Options{Locker: locker})

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
	setActorState(sub, "default", "racer", ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
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
	sub := substratetest.NewServer(t)
	states := map[string]string{}
	for i := range 6 {
		name := fmt.Sprintf("t%d", i)
		saveTask(t, ms, sub, "default", name, "Running", "")
		states["default/"+name] = `{"state":"exited","exitCode":0}`
	}
	saveTask(t, ms, sub, "default", "garbage", "Running", "")
	states["default/garbage"] = `{not json`
	router := &fakeRouter{states: states, delay: 50 * time.Millisecond}
	hs := httptest.NewServer(router)
	defer hs.Close()
	srv := server.NewServer(ms, newTestReconciler(sub))

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
	srv := server.NewServer(memory.NewStore(), newTestReconciler(substratetest.NewServer(t)))
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

func TestSuspendAndDelete_RecordTheOutcomeBeforeTheyAct(t *testing.T) {
	ms := memory.NewStore()
	sub := substratetest.NewServer(t)
	saveTask(t, ms, sub, "team-a", "quick", "Running", "")
	saveTask(t, ms, sub, "team-a", "brief", "Running", "")
	saveTask(t, ms, sub, "team-a", "busy", "Running", "")
	saveTask(t, ms, sub, "team-a", "asleep", "Suspended", "")
	saveTask(t, ms, sub, "team-a", "unknown", "Running", "")
	router := &fakeRouter{states: map[string]string{
		"team-a/quick":  `{"state":"exited","exitCode":4,"finishedAt":"2026-10-18T08:30:00Z"}`,
		"team-a/brief":  `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:31:00Z"}`,
		"team-a/busy":   `{"state":"running"}`,
		"team-a/asleep": `{"state":"exited","exitCode":0,"finishedAt":"2026-10-18T08:32:00Z"}`,
	}}
	hs := httptest.NewServer(router)
	defer hs.Close()
	rec := &recordingEmitter{}
	srv := server.NewServer(ms, newTestReconciler(sub), server.Options{Events: rec, RouterAddr: strings.TrimPrefix(hs.URL, "http://")})
	ctx := context.Background()

	// A command that exited before the suspend keeps its outcome.
	got, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Atespace: "team-a", Name: "quick"})
	if err != nil {
		t.Fatal(err)
	}
	if st := got.GetStatus(); st.GetPhase() != "Suspended" || st.GetOutcome() != v1alpha1.OutcomeFailed || st.GetExitCode() != 4 || st.GetFinishedAt().AsTime().Format(time.RFC3339) != "2026-10-18T08:30:00Z" {
		t.Errorf("suspended task status = %v, want Suspended with outcome Failed exit 4", st)
	}
	if stored, _ := ms.GetTask(ctx, "team-a", "quick"); stored.GetStatus().GetOutcome() != v1alpha1.OutcomeFailed {
		t.Errorf("stored status = %v, want outcome Failed", stored.GetStatus())
	}

	// A command that exited before the delete: the delete events carry the
	// outcome.
	if _, err := srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "team-a", Name: "brief"}); err != nil {
		t.Fatal(err)
	}
	var deleted *v1alpha1.Task
	for _, e := range rec.events {
		if e.GetMetadata().GetName() == "brief" && e.GetStatus().GetPhase() == events.PhaseDeleted {
			deleted = e
		}
	}
	if deleted.GetStatus().GetOutcome() != v1alpha1.OutcomeCompleted || deleted.GetStatus().GetExitCode() != 0 {
		t.Errorf("delete event = %v, want outcome Completed", deleted.GetStatus())
	}
	// The outcome event, before the delete, has the phase that GetTask
	// reported: the actor was running.
	for _, e := range rec.events {
		if e.GetMetadata().GetName() == "brief" && e.GetStatus().GetPhase() != "Running" && e.GetStatus().GetPhase() != v1alpha1.PhaseTerminating && e.GetStatus().GetPhase() != events.PhaseDeleted {
			t.Errorf("event for brief has phase %q", e.GetStatus().GetPhase())
		}
	}

	// A command that still runs, and a runner that does not answer, do not
	// stop the suspend and set no outcome.
	for _, name := range []string{"busy", "unknown"} {
		got, err := srv.SuspendTask(ctx, &v1alpha1.SuspendTaskRequest{Atespace: "team-a", Name: name})
		if err != nil {
			t.Fatalf("suspend %s: %v", name, err)
		}
		if got.GetStatus().GetPhase() != "Suspended" || got.GetStatus().GetOutcome() != "" {
			t.Errorf("%s status = %v, want Suspended with no outcome", name, got.GetStatus())
		}
	}

	// The router resumes a suspended actor to answer, so a task that is not
	// Running is not asked.
	if _, err := srv.DeleteTask(ctx, &v1alpha1.DeleteTaskRequest{Atespace: "team-a", Name: "asleep"}); err != nil {
		t.Fatal(err)
	}
	if router.askedFor("team-a/asleep") {
		t.Error("the delete asked the router about a suspended task")
	}
}

func TestSuspend_WithoutARouterDoesNotAsk(t *testing.T) {
	ms := memory.NewStore()
	sub := substratetest.NewServer(t)
	saveTask(t, ms, sub, "default", "t", "Running", "")
	srv := server.NewServer(ms, newTestReconciler(sub))
	got, err := srv.SuspendTask(context.Background(), &v1alpha1.SuspendTaskRequest{Name: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetStatus().GetPhase() != "Suspended" || got.GetStatus().GetOutcome() != "" {
		t.Errorf("status = %v", got.GetStatus())
	}
}

func TestSuspend_DoesNotWaitLongForASlowRunner(t *testing.T) {
	ms := memory.NewStore()
	sub := substratetest.NewServer(t)
	saveTask(t, ms, sub, "default", "slow", "Running", "")
	router := &fakeRouter{states: map[string]string{
		"default/slow": `{"state":"exited","exitCode":0}`,
	}, delay: 3 * time.Second}
	hs := httptest.NewServer(router)
	defer hs.Close()
	// A client without its own timeout: only the check's bound applies.
	srv := server.NewServer(ms, newTestReconciler(sub), server.Options{RouterAddr: strings.TrimPrefix(hs.URL, "http://"), RouterClient: &http.Client{}})

	start := time.Now()
	got, err := srv.SuspendTask(context.Background(), &v1alpha1.SuspendTaskRequest{Name: "slow"})
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Errorf("SuspendTask took %v with a slow runner", d)
	}
	if got.GetStatus().GetPhase() != "Suspended" || got.GetStatus().GetOutcome() != "" {
		t.Errorf("status = %v, want Suspended with no outcome", got.GetStatus())
	}
}
