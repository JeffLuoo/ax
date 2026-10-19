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
	"testing"

	sdklog "go.opentelemetry.io/otel/sdk/log"

	"github.com/google/ax/internal/events"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// blockingExporter holds every export until release is closed, so the batch
// processor's queue fills.
type blockingExporter struct{ release chan struct{} }

func (b *blockingExporter) Export(ctx context.Context, _ []sdklog.Record) error {
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return nil
}
func (b *blockingExporter) Shutdown(context.Context) error   { return nil }
func (b *blockingExporter) ForceFlush(context.Context) error { return nil }

func TestCountDrops_CountsRecordsThatAFullQueueDrops(t *testing.T) {
	events.CountDrops()
	before := events.Dropped()

	exp := &blockingExporter{release: make(chan struct{})}
	e := events.New(sdklog.NewBatchProcessor(exp, sdklog.WithMaxQueueSize(2), sdklog.WithExportMaxBatchSize(1)))
	task := &v1alpha1.Task{Metadata: &v1alpha1.ObjectMeta{Name: "t", Atespace: "default"}, Status: &v1alpha1.TaskStatus{Phase: "Running"}}
	for range 20 {
		e.TaskChanged(context.Background(), task)
	}
	close(exp.release)
	if err := e.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := events.Dropped() - before; got == 0 {
		t.Error("Dropped did not count the records that the full queue dropped")
	}
}
