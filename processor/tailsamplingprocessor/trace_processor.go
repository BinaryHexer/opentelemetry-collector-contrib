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

package tailsamplingprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"sync"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/sampling"
)

// tailSamplingSpanProcessor handles the incoming trace data and uses the given sampling
// policy to sample traces.
type tailSamplingSpanProcessor struct {
	nextConsumer      consumer.Traces
	samplingProcessor samplingProcessor
}

// newTraceProcessor returns a processor.TraceProcessor that will perform tail sampling according to the given
// configuration.
func newTraceProcessor(logger *zap.Logger, nextConsumer consumer.Traces, cfg Config) (component.TracesProcessor, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	tsp := &tailSamplingSpanProcessor{
		nextConsumer: nextConsumer,
	}

	sp, err := newProcessor(logger, cfg, tsp.decisionHook(), tsp.samplingHook())
	if err != nil {
		return nil, err
	}
	tsp.samplingProcessor = sp

	return tsp, nil
}

func (tsp *tailSamplingSpanProcessor) decisionHook() DecisionHook {
	return func(policy *Policy, id pdata.TraceID, batches []sampling.SignalData2) (sampling.Decision, error) {
		trace := &sampling.TraceData{
			Mutex:           sync.Mutex{},
			Decisions:       nil,
			ArrivalTime:     time.Time{},
			DecisionTime:    time.Time{},
			SpanCount:       0,
			ReceivedBatches: nil,
		}

		for _, batch := range batches {
			trace.ReceivedBatches = append(trace.ReceivedBatches, batch.TraceData)
		}

		return policy.Evaluator.EvaluateTrace(id, trace)
	}
}

func (tsp *tailSamplingSpanProcessor) samplingHook() SamplingHook {
	return func(ctx context.Context, decision sampling.Decision, batches []sampling.SignalData2) {
		var traceBatches []pdata.Traces
		for _, batch := range batches {
			traceBatches = append(traceBatches, batch.TraceData)
		}

		td := combineTraces(traceBatches)
		_ = tsp.nextConsumer.ConsumeTraces(ctx, td)
	}
}

// Start is invoked during service startup.
func (tsp *tailSamplingSpanProcessor) Start(ctx context.Context, host component.Host) error {
	return tsp.samplingProcessor.Start(ctx, host)
}

// Shutdown is invoked during service shutdown.
func (tsp *tailSamplingSpanProcessor) Shutdown(ctx context.Context) error {
	return tsp.samplingProcessor.Shutdown(ctx)
}

func (tsp *tailSamplingSpanProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}

// ConsumeTraces is required by the SpanProcessor interface.
func (tsp *tailSamplingSpanProcessor) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	idToSignal := groupTraceByID(td)
	tsp.samplingProcessor.ProcessSignals(idToSignal)

	return nil
}

func groupTraceByID(td pdata.Traces) map[pdata.TraceID]sampling.SignalData2 {
	idToTraces := make(map[pdata.TraceID]sampling.SignalData2)
	for _, trace := range batchpersignal.SplitTraces(td) {
		id := traceIDFromTraces(trace)
		idToTraces[id] = sampling.SignalData2{
			Type:      sampling.Trace,
			Count:     int64(trace.SpanCount()),
			TraceData: trace,
		}
	}

	return idToTraces
}

func traceIDFromTraces(td pdata.Traces) pdata.TraceID {
	rs := td.ResourceSpans()
	if rs.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	ils := rs.At(0).InstrumentationLibrarySpans()
	if ils.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	spans := ils.At(0).Spans()
	if spans.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	return spans.At(0).TraceID()
}

func combineTraces(traceBatches []pdata.Traces) pdata.Traces {
	combinedTraces := pdata.NewTraces()
	for j := 0; j < len(traceBatches); j++ {
		batch := traceBatches[j]
		batch.ResourceSpans().MoveAndAppendTo(combinedTraces.ResourceSpans())
	}

	return combinedTraces
}
