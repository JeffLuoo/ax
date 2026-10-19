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

// Package events sends task lifecycle events from ax-server to Agent FinOps.
//
// Each event is an OTLP log record named ax.task.state_changed. It carries the
// task's place in the task tree, its budget, its phase, and its outcome, so
// that FinOps can roll sub-task cost up to the parent task, compare spend with
// the budget, and split cost by outcome. ax-server sends one when a task is
// created, when its phase changes, when it is deleted, and when its outcome is
// set. It also sends the current state of every task again at an interval
// (the task resync), because the exporter drops an event when its queue is
// full, and FinOps keeps task state in memory only.
package events

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/google/ax/pkg/apis/v1alpha1"
)

// EventName is the OTLP event name of a task lifecycle event.
const EventName = "ax.task.state_changed"

// OutcomeSourceSandbox is the outcome source of every outcome ax-server
// reports: the task command's exit code.
const OutcomeSourceSandbox = "sandbox"

// PhaseDeleted is the phase of the event sent after a task's record is
// removed. It is not a phase that a stored task can have.
const PhaseDeleted = "Deleted"

// Attribute names of a task lifecycle event. FinOps reads them by these exact
// names; ate.atespace and ate.actor.name repeat the atespace and the task name
// because Ax names a task's actor after the task.
const (
	AttrAtespace  = "ax.atespace"
	AttrTaskName  = "ax.task.name"
	AttrParent    = "ax.task.parent"
	AttrBudgetUSD = "ax.task.budget_usd"
	AttrPhase     = "ax.task.phase"
	AttrOutcome   = "ax.task.outcome"
	AttrExitCode  = "ax.task.exit_code"
	// AttrTaskID is the task's unique ID (status.id). Two tasks with the same
	// name have different IDs.
	AttrTaskID = "ax.task.id"
	// AttrParentID is the parent's ID at create time (status.parent_id).
	// Unlike AttrParent, it does not move to a new parent with the same name.
	AttrParentID = "ax.task.parent_id"
	// AttrCreatedBy is the verified principal that created the task
	// (status.created_by), or "" when ax-server runs without authentication.
	AttrCreatedBy = "ax.task.created_by"
	// AttrOutcomeSource says who decided the outcome. ax-server's outcome is
	// the runner's exit code, reported from inside the sandbox, so it is
	// always "sandbox".
	AttrOutcomeSource = "ax.task.outcome_source"
	AttrAteSpace      = "ate.atespace"
	AttrActorName     = "ate.actor.name"
)

// Emitter sends task lifecycle events. TaskChanged must not block on the
// network, so an API call never waits for an export.
type Emitter interface {
	TaskChanged(ctx context.Context, task *v1alpha1.Task)
	// Shutdown flushes the events not yet sent.
	Shutdown(ctx context.Context) error
}

// Nop is an Emitter that sends nothing.
type Nop struct{}

func (Nop) TaskChanged(context.Context, *v1alpha1.Task) {}
func (Nop) Shutdown(context.Context) error              { return nil }

// Enabled reports whether the environment names an OTLP endpoint, which is
// what turns task events on.
func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT") != ""
}

// NewFromEnv returns an OTLP/gRPC Emitter configured by the standard
// OTEL_EXPORTER_OTLP_* variables when Enabled, and a Nop otherwise. Records
// go through a batch processor, so TaskChanged only enqueues; a full queue
// drops records rather than blocking. The processor does not send a dropped
// record again: CountDrops logs and counts the drops, and the task resync of
// ax-server sends the current state of each task again later.
func NewFromEnv(ctx context.Context) (Emitter, error) {
	if !Enabled() {
		return Nop{}, nil
	}
	CountDrops()
	exp, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	return New(sdklog.NewBatchProcessor(exp)), nil
}

// OTLP is an Emitter that writes records to an OpenTelemetry log processor.
type OTLP struct {
	provider *sdklog.LoggerProvider
	logger   otellog.Logger
	now      func() time.Time
}

// New returns an Emitter that hands records to processor.
func New(processor sdklog.Processor) *OTLP {
	res := resource.NewSchemaless(attribute.String("service.name", "ax-server"))
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(processor), sdklog.WithResource(res))
	return &OTLP{
		provider: provider,
		logger:   provider.Logger("github.com/google/ax/internal/events"),
		now:      time.Now,
	}
}

// TaskChanged emits one ax.task.state_changed record for task.
func (e *OTLP) TaskChanged(ctx context.Context, task *v1alpha1.Task) {
	now := e.now()
	var rec otellog.Record
	rec.SetEventName(EventName)
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetSeverity(otellog.SeverityInfo)
	rec.SetBody(attribute.StringValue("Task state changed"))
	rec.AddAttributes(Attributes(task)...)
	// The record goes out after the call that caused it returns.
	e.logger.Emit(context.WithoutCancel(ctx), rec)
}

// Shutdown flushes and stops the processor.
func (e *OTLP) Shutdown(ctx context.Context) error {
	return e.provider.Shutdown(ctx)
}

// Attributes returns the attributes of the event for task.
func Attributes(task *v1alpha1.Task) []attribute.KeyValue {
	meta, spec, st := task.GetMetadata(), task.GetSpec(), task.GetStatus()
	atespace := meta.GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	attrs := []attribute.KeyValue{
		attribute.String(AttrAtespace, atespace),
		attribute.String(AttrTaskName, meta.GetName()),
		attribute.String(AttrParent, spec.GetParent()),
		attribute.String(AttrTaskID, st.GetId()),
		attribute.String(AttrParentID, st.GetParentId()),
		attribute.String(AttrCreatedBy, st.GetCreatedBy()),
		attribute.Float64(AttrBudgetUSD, spec.GetBudget().GetMaxUsd()),
		attribute.String(AttrPhase, st.GetPhase()),
		attribute.String(AttrOutcome, st.GetOutcome()),
		attribute.Int64(AttrExitCode, int64(st.GetExitCode())),
		attribute.String(AttrAteSpace, atespace),
		attribute.String(AttrActorName, meta.GetName()),
	}
	if st.GetOutcome() != "" {
		attrs = append(attrs, attribute.String(AttrOutcomeSource, OutcomeSourceSandbox))
	}
	return attrs
}
