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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// DefaultOutcomePollInterval is the time between two outcome polls.
	DefaultOutcomePollInterval = 15 * time.Second
	// DefaultOutcomePollConcurrency bounds the /statusz requests in flight.
	DefaultOutcomePollConcurrency = 8

	// outcomeListPage is how many tasks one store read returns.
	outcomeListPage = 500
	// outcomeLockTimeout bounds the wait for a task's lock, for example while
	// a resume holds it.
	outcomeLockTimeout = 30 * time.Second
	// maxStatuszBytes bounds a /statusz response.
	maxStatuszBytes = 64 << 10
	// finalOutcomeTimeout bounds the /statusz request that a suspend or a
	// delete makes while it holds the task's lock.
	finalOutcomeTimeout = 2 * time.Second
)

// OutcomePollerOptions configures RunOutcomePoller.
type OutcomePollerOptions struct {
	// RouterAddr is the atenet router's address: a URL, or a host:port where
	// port 443 means https. The router sends a request to the actor that the
	// ate-target-actor header names.
	RouterAddr string
	// Interval is the time between polls; zero means DefaultOutcomePollInterval.
	Interval time.Duration
	// Concurrency bounds the requests in flight; zero means
	// DefaultOutcomePollConcurrency.
	Concurrency int
	// HTTPClient sends the requests; nil means controller.NewRouterHTTPClient,
	// which has a 2 s timeout and the router's mTLS credentials.
	HTTPClient *http.Client
}

// statusz is the runner's GET /statusz report.
type statusz struct {
	State      string `json:"state"`
	ExitCode   *int   `json:"exitCode"`
	FinishedAt string `json:"finishedAt"`
}

// RunOutcomePoller sets the outcome of tasks whose command has exited. It
// polls at once and then every interval until ctx is done. It does nothing
// when opts.RouterAddr is empty.
func (s *Server) RunOutcomePoller(ctx context.Context, opts OutcomePollerOptions) {
	if opts.RouterAddr == "" {
		slog.Warn("no atenet router address; task outcomes will not be recorded")
		return
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultOutcomePollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.PollOutcomes(ctx, opts)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// PollOutcomes makes one pass over the tasks that are Running and have no
// outcome. For each one it asks the runner for /statusz through the router,
// and sets the outcome when the command has exited. It returns how many
// outcomes it set.
//
// A stored task is never Running: the phase follows from the task's actor, as
// GetTask reports it. So PollOutcomes reads the actors to find the running
// tasks, and reads the actor again under the task's lock before it asks.
//
// It never asks about a task that is not Running: the router resumes a
// suspended actor to answer, and that would undo a suspend. It holds the
// task's lock from the check of the phase until the outcome is saved, so a
// suspend cannot start in between.
func (s *Server) PollOutcomes(ctx context.Context, opts OutcomePollerOptions) int {
	client := opts.HTTPClient
	if client == nil {
		client = controller.NewRouterHTTPClient()
	}
	workers := opts.Concurrency
	if workers <= 0 {
		workers = DefaultOutcomePollConcurrency
	}

	candidates := s.runningTasksWithoutOutcome(ctx)

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		set  int
		sema = make(chan struct{}, workers)
	)
	for _, t := range candidates {
		select {
		case <-ctx.Done():
			wg.Wait()
			return set
		case sema <- struct{}{}:
		}
		wg.Go(func() {
			defer func() { <-sema }()
			if s.checkOutcome(ctx, client, opts.RouterAddr, t.GetMetadata().GetAtespace(), t.GetMetadata().GetName()) {
				mu.Lock()
				set++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return set
}

// runningTasksWithoutOutcome returns the tasks that GetTask reports as
// Running and that have no outcome. It reads the stored tasks one page at a
// time, then the actors with one ListActors call for each atespace. GetTask
// reports a task whose stored phase is one of AX's own (Pending, Failed, or
// Terminating) with that phase, so such a task is not a candidate.
func (s *Server) runningTasksWithoutOutcome(ctx context.Context) []*v1alpha1.Task {
	byAtespace := map[string][]*v1alpha1.Task{}
	for offset := int64(0); ; offset += outcomeListPage {
		page, err := s.storedTasks(ctx, "", outcomeListPage, offset)
		if err != nil {
			slog.Warn("listing tasks for outcome poll", "error", err)
			return nil
		}
		for _, t := range page {
			if t.GetStatus().GetOutcome() != "" || v1alpha1.IsAXPhase(t.GetStatus().GetPhase()) {
				continue
			}
			ns := t.GetMetadata().GetAtespace()
			if ns == "" {
				ns = "default"
			}
			byAtespace[ns] = append(byAtespace[ns], t)
		}
		if len(page) < outcomeListPage {
			break
		}
	}

	var running []*v1alpha1.Task
	for ns, group := range byAtespace {
		actors, err := s.substrate.ListActors(ctx, ns)
		if err != nil {
			slog.Warn("listing actors for outcome poll", "atespace", ns, "error", err)
			continue
		}
		byName := make(map[string]*ateapipb.Actor, len(actors))
		for _, a := range actors {
			byName[a.GetMetadata().GetName()] = a
		}
		for _, t := range group {
			a, ok := byName[t.GetMetadata().GetName()]
			if ok && actorPhase(a.GetStatus().GetState()) == v1alpha1.PhaseRunning {
				running = append(running, t)
			}
		}
	}
	return running
}

// checkOutcome asks one task's runner for its command state and records an
// exit. It reports whether it set the outcome.
func (s *Server) checkOutcome(ctx context.Context, client *http.Client, routerAddr, atespace, name string) bool {
	if atespace == "" {
		atespace = "default"
	}
	lockCtx, cancel := context.WithTimeout(ctx, outcomeLockTimeout)
	defer cancel()
	unlock, err := s.locker.Lock(lockCtx, "task", atespace, name)
	if err != nil {
		slog.Warn("locking task for outcome poll", "task", atespace+"/"+name, "error", err)
		return false
	}
	defer unlock()

	// The phase may have changed since the list; only the task as GetTask
	// reports it now, with its actor read under the lock, counts.
	task, err := s.getTask(ctx, atespace, name)
	if err != nil || task.GetStatus().GetPhase() != v1alpha1.PhaseRunning || task.GetStatus().GetOutcome() != "" {
		return false
	}
	return s.recordOutcomeLocked(ctx, client, routerAddr, task, 0)
}

// recordFinalOutcome asks the runner of a Running task with no outcome for
// its command state once, and records an exit in task and in the store. task
// comes from getTask, so its phase follows from its actor.
// SuspendTask and DeleteTask call it while they hold the task's lock, before
// they act: after a suspend the poller does not ask, and after a delete
// there is no runner to ask. A command that exited after the last poll
// otherwise loses its outcome. It does nothing when no router is set, and a
// failed request does not stop the caller.
func (s *Server) recordFinalOutcome(ctx context.Context, task *v1alpha1.Task) {
	if s.routerAddr == "" || task.GetStatus().GetPhase() != v1alpha1.PhaseRunning || task.GetStatus().GetOutcome() != "" {
		return
	}
	s.recordOutcomeLocked(ctx, s.routerClient, s.routerAddr, task, finalOutcomeTimeout)
}

// recordOutcomeLocked asks task's runner for its command state and records
// an exit in task and in the store. The caller holds the task's lock and has
// checked, with the task's actor, that the task is Running and has no outcome. A timeout
// greater than zero bounds the request; the store write does not count
// against it. It reports whether it set the outcome.
func (s *Server) recordOutcomeLocked(ctx context.Context, client *http.Client, routerAddr string, task *v1alpha1.Task, timeout time.Duration) bool {
	atespace, name := task.GetMetadata().GetAtespace(), task.GetMetadata().GetName()
	if atespace == "" {
		atespace = "default"
	}
	fetchCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	st, err := fetchStatusz(fetchCtx, client, routerAddr, atespace, name)
	if err != nil {
		slog.Debug("reading task command state", "task", atespace+"/"+name, "error", err)
		return false
	}
	if st.State != "exited" {
		return false
	}

	code := int32(-1)
	if st.ExitCode != nil {
		code = int32(*st.ExitCode)
	}
	finished := time.Now()
	if t, err := time.Parse(time.RFC3339Nano, st.FinishedAt); err == nil {
		finished = t
	}
	task.Status.Outcome = v1alpha1.OutcomeForExitCode(code)
	task.Status.ExitCode = code
	task.Status.FinishedAt = timestamppb.New(finished)
	if err := s.updateTask(ctx, atespace, name, task.Status); err != nil {
		slog.Warn("saving task outcome", "task", atespace+"/"+name, "error", err)
		return false
	}
	slog.Info("task command exited", "task", atespace+"/"+name, "outcome", task.Status.Outcome, "exitCode", code)
	s.events.TaskChanged(ctx, task)
	return true
}

// fetchStatusz GETs /statusz from the task's runner through the atenet
// router, which routes on the ate-target-actor header. The actor is named
// after the task.
func fetchStatusz(ctx context.Context, client *http.Client, routerAddr, atespace, name string) (*statusz, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, controller.RouterURL(routerAddr, "/statusz"), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("ate-target-actor", atespace+"/"+name)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxStatuszBytes))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /statusz returned HTTP %d", resp.StatusCode)
	}
	var st statusz
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, fmt.Errorf("decoding /statusz: %w", err)
	}
	return &st, nil
}
