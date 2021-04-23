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
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpersignal"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/sampling"
)

// tailSamplingLogProcessor handles the incoming trace data and uses the given sampling
// policy to sample traces.
type tailSamplingLogProcessor struct {
	nextConsumer      consumer.Logs
	samplingProcessor samplingProcessor
}

// newLogProcessor returns a processor.LogProcessor that will perform tail sampling according to the given
// configuration.
func newLogProcessor(logger *zap.Logger, nextConsumer consumer.Logs, cfg Config) (component.LogsProcessor, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	tsp := &tailSamplingLogProcessor{
		nextConsumer: nextConsumer,
	}

	sp, err := newProcessor(logger, cfg, tsp.decisionHook(), tsp.samplingHook())
	if err != nil {
		return nil, err
	}
	tsp.samplingProcessor = sp

	return tsp, nil
}

func (tsp *tailSamplingLogProcessor) decisionHook() DecisionHook {
	return func(policy *Policy, id pdata.TraceID, batches []sampling.SignalData2) (sampling.Decision, error) {
		log := &sampling.LogData{
			Mutex:           sync.Mutex{},
			Decisions:       nil,
			ArrivalTime:     time.Time{},
			DecisionTime:    time.Time{},
			LogCount:        0,
			ReceivedBatches: nil,
		}

		for _, batch := range batches {
			log.ReceivedBatches = append(log.ReceivedBatches, batch.LogData)
		}

		return policy.Evaluator.EvaluateLog(id, log)
	}
}

func (tsp *tailSamplingLogProcessor) samplingHook() SamplingHook {
	return func(ctx context.Context, decision sampling.Decision, batches []sampling.SignalData2) {
		var logBatches []pdata.Logs
		for _, batch := range batches {
			logBatches = append(logBatches, batch.LogData)
		}

		ld := combineLogs(logBatches)
		_ = tsp.nextConsumer.ConsumeLogs(ctx, ld)
	}
}

// Start is invoked during service startup.
func (tsp *tailSamplingLogProcessor) Start(ctx context.Context, host component.Host) error {
	return tsp.samplingProcessor.Start(ctx, host)
}

// Shutdown is invoked during service shutdown.
func (tsp *tailSamplingLogProcessor) Shutdown(ctx context.Context) error {
	return tsp.samplingProcessor.Shutdown(ctx)
}

func (tsp *tailSamplingLogProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}

func (tsp *tailSamplingLogProcessor) ConsumeLogs(ctx context.Context, ld pdata.Logs) error {
	idToSignal := groupLogByID(ld)
	tsp.samplingProcessor.ProcessSignals(idToSignal)

	return nil
}

func groupLogByID(ld pdata.Logs) map[pdata.TraceID]sampling.SignalData2 {
	idToTraces := make(map[pdata.TraceID]sampling.SignalData2)
	for _, log := range batchpersignal.SplitLogs(ld) {
		id := traceIDFromLogs(log)
		idToTraces[id] = sampling.SignalData2{
			Type:    sampling.Trace,
			Count:   int64(log.LogRecordCount()),
			LogData: log,
		}
	}

	return idToTraces
}

func traceIDFromLogs(ld pdata.Logs) pdata.TraceID {
	rl := ld.ResourceLogs()
	if rl.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	ill := rl.At(0).InstrumentationLibraryLogs()
	if ill.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	logs := ill.At(0).Logs()
	if logs.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	return logs.At(0).TraceID()
}

func combineLogs(logBatches []pdata.Logs) pdata.Logs {
	combinedLogs := pdata.NewLogs()
	for j := 0; j < len(logBatches); j++ {
		batch := logBatches[j]
		batch.ResourceLogs().MoveAndAppendTo(combinedLogs.ResourceLogs())
	}

	return combinedLogs
}
