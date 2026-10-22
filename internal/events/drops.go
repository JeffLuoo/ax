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

package events

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
)

// The batch processor of the OpenTelemetry log SDK has no API that returns
// the number of records it dropped. It reports a drop only as the warning
// "dropped log records" to the OpenTelemetry internal logger, and the default
// internal logger does not show warnings. CountDrops replaces that logger, so
// ax-server counts the drops and writes them to its own log.

var (
	dropped        atomic.Uint64
	countDropsOnce sync.Once
)

// CountDrops sends the internal messages of the OpenTelemetry SDK to slog,
// and counts the task events that the batch processor dropped. It changes a
// process-wide setting. A second call does nothing.
func CountDrops() {
	countDropsOnce.Do(func() { otel.SetLogger(logr.New(dropSink{})) })
}

// Dropped returns how many task events the export queue dropped since the
// process started. It counts only after CountDrops.
func Dropped() uint64 { return dropped.Load() }

// dropSink is a logr.LogSink that writes to slog. The SDK writes warnings at
// V(1), information at V(4), and debug messages at V(8). The sink keeps the
// warnings and the errors only.
type dropSink struct {
	values []any
}

func (dropSink) Init(logr.RuntimeInfo)  {}
func (dropSink) Enabled(level int) bool { return level <= 1 }

func (s dropSink) Info(_ int, msg string, kv ...any) {
	if msg == "dropped log records" {
		var n uint64
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i] == "dropped" {
				if d, ok := kv[i+1].(uint64); ok {
					n = d
				}
			}
		}
		total := dropped.Add(n)
		slog.Warn("the task event queue was full; some task events were dropped until the next task resync", "dropped", n, "droppedTotal", total)
		return
	}
	slog.Warn("opentelemetry: "+msg, append(append([]any{}, s.values...), kv...)...)
}

func (s dropSink) Error(err error, msg string, kv ...any) {
	slog.Error("opentelemetry: "+msg, append(append([]any{"error", err}, s.values...), kv...)...)
}

func (s dropSink) WithValues(kv ...any) logr.LogSink {
	return dropSink{values: append(append([]any{}, s.values...), kv...)}
}

func (s dropSink) WithName(string) logr.LogSink { return s }
