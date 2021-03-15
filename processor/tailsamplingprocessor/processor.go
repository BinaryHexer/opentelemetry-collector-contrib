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
	"runtime"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"sync/atomic"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/idbatcher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/sampling"
)

const (
	sourceFormat = "tail_sampling"
)

var _ samplingProcessor = (*tailSamplingProcessor)(nil)

type samplingProcessor interface {
	component.Processor
	ProcessSignals(idToSignals map[pdata.TraceID]sampling.SignalData2)
}

// tailSamplingProcessor handles the incoming trace data and uses the given sampling
// policy to sample traces.
type tailSamplingProcessor struct {
	ctx              context.Context
	start            sync.Once
	maxNumTraces     uint64
	policies         []*Policy
	logger           *zap.Logger
	idToSignal       sync.Map
	policyTicker     tTicker
	decisionBatcher  idbatcher.Batcher
	deleteChan       chan pdata.TraceID
	numTracesOnMap   uint64
	decisionHook     DecisionHook
	postSamplingHook SamplingHook
}

type DecisionHook func(*Policy, pdata.TraceID, []sampling.SignalData2) (sampling.Decision, error)

type SamplingHook func(context.Context, sampling.Decision, []sampling.SignalData2)

// newLogProcessor returns a processor.LogProcessor that will perform tail sampling according to the given
// configuration.
func newProcessor(logger *zap.Logger, cfg Config, dhook DecisionHook, shook SamplingHook) (samplingProcessor, error) {
	numDecisionBatches := uint64(cfg.DecisionWait.Seconds())
	inBatcher, err := idbatcher.New(numDecisionBatches, cfg.ExpectedNewTracesPerSec, uint64(2*runtime.NumCPU()))
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	policies, err := buildPolicies(ctx, logger, cfg)
	if err != nil {
		return nil, err
	}

	tsp := &tailSamplingProcessor{
		ctx:              ctx,
		maxNumTraces:     cfg.NumTraces,
		logger:           logger,
		decisionBatcher:  inBatcher,
		policies:         policies,
		decisionHook:     dhook,
		postSamplingHook: shook,
	}

	tsp.policyTicker = &policyTicker{onTick: tsp.samplingPolicyOnTick}
	tsp.deleteChan = make(chan pdata.TraceID, cfg.NumTraces)

	return tsp, nil
}

func buildPolicies(ctx context.Context, logger *zap.Logger, cfg Config) ([]*Policy, error) {
	var policies []*Policy
	for i := range cfg.PolicyCfgs {
		policyCfg := &cfg.PolicyCfgs[i]
		policyCtx, err := tag.New(ctx, tag.Upsert(tagPolicyKey, policyCfg.Name), tag.Upsert(tagSourceFormat, sourceFormat))
		if err != nil {
			return nil, err
		}

		eval, err := getPolicyEvaluator(logger, policyCfg)
		if err != nil {
			return nil, err
		}

		policy := &Policy{
			Name:      policyCfg.Name,
			Evaluator: eval,
			ctx:       policyCtx,
		}
		policies = append(policies, policy)
	}

	return policies, nil
}

func (tsp *tailSamplingProcessor) samplingPolicyOnTick() {
	metrics := policyMetrics{}

	startTime := time.Now()
	batch, _ := tsp.decisionBatcher.CloseCurrentAndTakeFirstBatch()
	batchLen := len(batch)
	tsp.logger.Debug("Sampling Policy Evaluation ticked")
	for _, id := range batch {
		d, ok := tsp.idToSignal.Load(id)
		if !ok {
			metrics.idNotFoundOnMapCount++
			continue
		}
		trace := d.(*sampling.SignalData)
		trace.DecisionTime = time.Now()

		decision, policy := tsp.makeDecision(id, trace, &metrics)

		// Sampled or not, remove the batches
		trace.Lock()
		traceBatches := trace.ReceivedBatches
		trace.ReceivedBatches = nil
		trace.Unlock()

		if decision == sampling.Sampled {
			tsp.postSamplingHook(policy.ctx, sampling.Sampled, traceBatches)
		}
	}

	stats.Record(tsp.ctx,
		statOverallDecisionLatencyµs.M(int64(time.Since(startTime)/time.Microsecond)),
		statDroppedTooEarlyCount.M(metrics.idNotFoundOnMapCount),
		statPolicyEvaluationErrorCount.M(metrics.evaluateErrorCount),
		statTracesOnMemoryGauge.M(int64(atomic.LoadUint64(&tsp.numTracesOnMap))))

	tsp.logger.Debug("Sampling policy evaluation completed",
		zap.Int("batch.len", batchLen),
		zap.Int64("sampled", metrics.decisionSampled),
		zap.Int64("notSampled", metrics.decisionNotSampled),
		zap.Int64("droppedPriorToEvaluation", metrics.idNotFoundOnMapCount),
		zap.Int64("policyEvaluationErrors", metrics.evaluateErrorCount),
	)
}

func (tsp *tailSamplingProcessor) makeDecision(id pdata.TraceID, sd *sampling.SignalData, metrics *policyMetrics) (sampling.Decision, *Policy) {
	finalDecision := sampling.NotSampled
	var matchingPolicy *Policy = nil

	for i, policy := range tsp.policies {
		policyEvaluateStartTime := time.Now()
		sd.Lock()
		batches := sd.ReceivedBatches
		sd.Unlock()
		decision, err := tsp.decisionHook(policy, id, batches)

		stats.Record(
			policy.ctx,
			statDecisionLatencyMicroSec.M(int64(time.Since(policyEvaluateStartTime)/time.Microsecond)))

		if err != nil {
			sd.Decisions[i] = sampling.NotSampled
			metrics.evaluateErrorCount++
			tsp.logger.Debug("Sampling policy error", zap.Error(err))
		} else {
			sd.Decisions[i] = decision

			switch decision {
			case sampling.Sampled:
				// any single policy that decides to sample will cause the decision to be sampled
				// the nextConsumer will get the context from the first matching policy
				finalDecision = sampling.Sampled
				if matchingPolicy == nil {
					matchingPolicy = policy
				}

				_ = stats.RecordWithTags(
					policy.ctx,
					[]tag.Mutator{tag.Insert(tagSampledKey, "true")},
					statCountTracesSampled.M(int64(1)),
				)
				metrics.decisionSampled++

			case sampling.NotSampled:
				_ = stats.RecordWithTags(
					policy.ctx,
					[]tag.Mutator{tag.Insert(tagSampledKey, "false")},
					statCountTracesSampled.M(int64(1)),
				)
				metrics.decisionNotSampled++
			}
		}
	}

	return finalDecision, matchingPolicy
}

func (tsp *tailSamplingProcessor) Start(ctx context.Context, host component.Host) error {
	tsp.start.Do(func() {
		tsp.logger.Info("Starting tail_sampling timers")
		tsp.policyTicker.Start(1 * time.Second)
	})

	return nil
}

func (tsp *tailSamplingProcessor) Shutdown(ctx context.Context) error {
	return nil
}

func (tsp *tailSamplingProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}

func (tsp *tailSamplingProcessor) ProcessSignals(idToSignals map[pdata.TraceID]sampling.SignalData2) {
	var newTraceIDs int64
	for id, signal := range idToSignals {
		s := signal

		count := s.Count
		initialSignalData := tsp.buildSignalData(&s)
		actualData, loaded := tsp.loadOrStore(id, initialSignalData, count)
		tsp.checkSamplingDecision(actualData, &s)

		if loaded {
			newTraceIDs++
		}
	}

	stats.Record(tsp.ctx, statNewTraceIDReceivedCount.M(newTraceIDs))
}

func (tsp *tailSamplingProcessor) buildSignalData(sd *sampling.SignalData2) *sampling.SignalData {
	lenPolicies := len(tsp.policies)
	initialDecisions := make([]sampling.Decision, lenPolicies)
	for i := 0; i < lenPolicies; i++ {
		initialDecisions[i] = sampling.Pending
	}
	initialSignalData := &sampling.SignalData{
		Decisions:   initialDecisions,
		ArrivalTime: time.Now(),
		Count:       sd.Count,
	}

	return initialSignalData
}

func (tsp *tailSamplingProcessor) loadOrStore(id pdata.TraceID, initialSignalData *sampling.SignalData, count int64) (*sampling.SignalData, bool) {
	d, loaded := tsp.idToSignal.LoadOrStore(id, initialSignalData)
	actualData := d.(*sampling.SignalData)
	if loaded {
		atomic.AddInt64(&actualData.Count, count)
	} else {
		tsp.decisionBatcher.AddToCurrentBatch(id)
		atomic.AddUint64(&tsp.numTracesOnMap, 1)
		postDeletion := false
		currTime := time.Now()
		for !postDeletion {
			select {
			case tsp.deleteChan <- id:
				postDeletion = true
			default:
				traceKeyToDrop := <-tsp.deleteChan
				tsp.dropTrace(traceKeyToDrop, currTime)
			}
		}
	}

	return actualData, loaded
}

func (tsp *tailSamplingProcessor) dropTrace(traceID pdata.TraceID, deletionTime time.Time) {
	var sd *sampling.SignalData
	if d, ok := tsp.idToSignal.Load(traceID); ok {
		sd = d.(*sampling.SignalData)
		tsp.idToSignal.Delete(traceID)
		// Subtract one from numTracesOnMap per https://godoc.org/sync/atomic#AddUint64
		atomic.AddUint64(&tsp.numTracesOnMap, ^uint64(0))
	}
	if sd == nil {
		tsp.logger.Error("Attempt to delete traceID not on table")
		return
	}

	stats.Record(tsp.ctx, statTraceRemovalAgeSec.M(int64(deletionTime.Sub(sd.ArrivalTime)/time.Second)))
}

func (tsp *tailSamplingProcessor) checkSamplingDecision(actualData *sampling.SignalData, signal *sampling.SignalData2) {
	for i, policy := range tsp.policies {
		actualData.Lock()
		actualDecision := actualData.Decisions[i]
		// If decision is pending, we want to add the new spans still under the lock, so the decision doesn't happen
		// in between the transition from pending.
		if actualDecision == sampling.Pending {
			// Add the spans to the trace, but only once for all policy, otherwise same spans will
			// be duplicated in the final trace.
			actualData.ReceivedBatches = append(actualData.ReceivedBatches, *signal)
			actualData.Unlock()
			break
		}
		actualData.Unlock()

		tsp.postSamplingHook(policy.ctx, actualDecision, []sampling.SignalData2{*signal})

		// At this point the late arrival has been passed to nextConsumer. Need to break out of the policy loop
		// so that it isn't sent to nextConsumer more than once when multiple policies chose to sample
		if actualDecision == sampling.Sampled {
			break
		}
	}
}
