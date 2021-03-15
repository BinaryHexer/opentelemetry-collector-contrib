// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sampling

import (
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer/pdata"
)

type Signal int32

const (
	Unknown Signal = iota
	Trace
	Log
)

type SignalData struct {
	sync.Mutex
	// Decisions gives the current status of the sampling decision for each policy.
	Decisions []Decision
	// Arrival time the first span or log for the trace was received.
	ArrivalTime time.Time
	// Decisiontime time when sampling decision was taken.
	DecisionTime time.Time
	// Count track the number of spans or logs associated with the trace.
	Count int64
	// ReceivedBatches stores all the batches received for the trace.
	ReceivedBatches []SignalData2
}

type SignalData2 struct {
	// Type is the type of signal data, trace or log.
	Type Signal
	// Count track the number of spans or logs associated with the trace.
	Count int64
	// TraceData stores the data received for the traces.
	TraceData pdata.Traces
	// LogData stores the data received for the logs.
	LogData pdata.Logs
}

// TraceData stores the sampling related trace data.
type TraceData struct {
	sync.Mutex
	// Decisions gives the current status of the sampling decision for each policy.
	Decisions []Decision
	// Arrival time the first span for the trace was received.
	ArrivalTime time.Time
	// Decisiontime time when sampling decision was taken.
	DecisionTime time.Time
	// SpanCount track the number of spans on the trace.
	SpanCount int64
	// ReceivedBatches stores all the batches received for the trace.
	ReceivedBatches []pdata.Traces
}

// LogData stores the sampling related trace data.
type LogData struct {
	sync.Mutex
	// Decisions gives the current status of the sampling decision for each policy.
	Decisions []Decision
	// Arrival time the first log for the trace was received.
	ArrivalTime time.Time
	// Decisiontime time when sampling decision was taken.
	DecisionTime time.Time
	// LogCount track the number of logs on the trace.
	LogCount int64
	// ReceivedBatches stores all the batches received for the trace.
	ReceivedBatches []pdata.Logs
}

// Decision gives the status of sampling decision.
type Decision int32

const (
	// Unspecified indicates that the status of the decision was not set yet.
	Unspecified Decision = iota
	// Pending indicates that the policy was not evaluated yet.
	Pending
	// Sampled is used to indicate that the decision was already taken
	// to sample the data.
	Sampled
	// NotSampled is used to indicate that the decision was already taken
	// to not sample the data.
	NotSampled
	// Dropped is used when data needs to be purged before the sampling policy
	// had a chance to evaluate it.
	Dropped
)

// PolicyEvaluator implements a tail-based sampling policy evaluator,
// which makes a sampling decision for a given trace when requested.
type PolicyEvaluator interface {
	// OnLateArrivingSpans notifies the evaluator that the given list of spans arrived
	// after the sampling decision was already taken for the trace.
	// This gives the evaluator a chance to log any message/metrics and/or update any
	// related internal state.
	OnLateArrivingSpans(earlyDecision Decision, spans []*pdata.Span) error
	OnLateArrivingLogs(earlyDecision Decision, logs []*pdata.LogRecord) error

	// EvaluateTrace looks at the trace data and returns a corresponding SamplingDecision.
	EvaluateTrace(traceID pdata.TraceID, trace *TraceData) (Decision, error)
	// EvaluateLog looks at the log data and returns a corresponding SamplingDecision.
	EvaluateLog(traceID pdata.TraceID, log *LogData) (Decision, error)
}
