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
	"log/slog"
	"time"
)

const (
	// DefaultTaskResyncInterval is the time between two task resyncs.
	DefaultTaskResyncInterval = 10 * time.Minute

	// defaultResyncPageSize is how many tasks one store read returns, and so
	// how many events one page sends.
	defaultResyncPageSize = 500
	// defaultResyncPagePause is the wait after each page. The event exporter
	// sends up to 512 records each second, so one page each second does not
	// fill its queue.
	defaultResyncPagePause = time.Second
	// defaultResyncLockTimeout bounds the wait for a task's lock. An RPC that
	// holds the lock sends its own event.
	defaultResyncLockTimeout = 2 * time.Second
)

// TaskResyncOptions configures RunTaskResync and ResyncTasks.
type TaskResyncOptions struct {
	// Interval is the time between resyncs; zero or less turns the resync off.
	Interval time.Duration
	// PageSize is how many tasks one store read returns; zero means 500.
	PageSize int64
	// PagePause is the wait after each page; zero means 1 s in RunTaskResync
	// and no wait in ResyncTasks.
	PagePause time.Duration
	// LockTimeout bounds the wait for each task's lock; zero means 2 s.
	LockTimeout time.Duration
}

// RunTaskResync sends the current state of every stored task again, at once
// and then every opts.Interval until ctx is done. It does nothing when
// opts.Interval is zero or less.
//
// The task event exporter drops an event when its queue is full, and it does
// not send that event again. Agent FinOps keeps task state in memory only, so
// after a FinOps or collector restart it does not know a task until the task
// changes. The resync repairs both cases within one interval.
func (s *Server) RunTaskResync(ctx context.Context, opts TaskResyncOptions) {
	if opts.Interval <= 0 {
		return
	}
	if opts.PagePause == 0 {
		opts.PagePause = defaultResyncPagePause
	}
	ticker := time.NewTicker(opts.Interval)
	defer ticker.Stop()
	for {
		n := s.ResyncTasks(ctx, opts)
		slog.Debug("resent task state", "tasks", n)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ResyncTasks makes one pass over the stored tasks and sends a task event for
// each one. It returns how many events it sent.
//
// It reads the store one page at a time and waits opts.PagePause after each
// page, so a large store does not fill the exporter's queue. It reads each
// task again under the task's lock, so an event never carries a state older
// than the event of a concurrent RPC. When an RPC holds the lock longer than
// opts.LockTimeout, it skips the task: the RPC sends its own event. A task
// that is deleted during the pass can move other tasks to an earlier page, so
// the pass can miss a task; the next pass sends it.
func (s *Server) ResyncTasks(ctx context.Context, opts TaskResyncOptions) int {
	pageSize := opts.PageSize
	if pageSize <= 0 {
		pageSize = defaultResyncPageSize
	}
	lockTimeout := opts.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = defaultResyncLockTimeout
	}

	sent := 0
	for offset := int64(0); ; offset += pageSize {
		page, err := s.store.ListTasks(ctx, "", pageSize, offset)
		if err != nil {
			slog.Warn("listing tasks for resync", "error", err)
			return sent
		}
		for _, t := range page {
			if ctx.Err() != nil {
				return sent
			}
			if s.resyncTask(ctx, lockTimeout, t.GetMetadata().GetAtespace(), t.GetMetadata().GetName()) {
				sent++
			}
		}
		if int64(len(page)) < pageSize {
			return sent
		}
		if opts.PagePause > 0 {
			select {
			case <-ctx.Done():
				return sent
			case <-time.After(opts.PagePause):
			}
		}
	}
}

// resyncTask sends one task's stored state. It reports whether it sent it.
func (s *Server) resyncTask(ctx context.Context, lockTimeout time.Duration, atespace, name string) bool {
	if atespace == "" {
		atespace = "default"
	}
	lockCtx, cancel := context.WithTimeout(ctx, lockTimeout)
	defer cancel()
	unlock, err := s.locker.Lock(lockCtx, "task", atespace, name)
	if err != nil {
		slog.Debug("skipping a locked task in resync", "task", atespace+"/"+name, "error", err)
		return false
	}
	defer unlock()
	task, err := s.store.GetTask(ctx, atespace, name)
	if err != nil {
		// The task was deleted after the list; its delete event went out.
		return false
	}
	s.events.TaskChanged(ctx, task)
	return true
}
