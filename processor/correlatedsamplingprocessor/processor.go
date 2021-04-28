package correlatedsamplingprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlatedsamplingprocessor/sampling"
)

type signalType int32

const (
	Unknown signalType = iota
	Trace
	Log
)

type signal struct {
	TraceID pdata.TraceID
	Data    interface{}
}

type combinatorHook func(batches []*signal) (batch interface{})

type decisionHook func(sampling.Filter, pdata.TraceID, interface{}) (sampling.Decision, error)

type samplingHook func(interface{})

var _ component.Processor = (*correlatedProcessor)(nil)

type correlatedProcessor struct {
	logger          *zap.Logger
	combinatorHook  combinatorHook
	decisionHook    decisionHook
	samplingHook    samplingHook
	nextConsumer    component.Processor
	filters         []sampling.Filter
	samplingTracker samplingTracker
	deleteChan      chan pdata.TraceID
	batcher         batcher
}

func newCProcessor(logger *zap.Logger, cfg Config, ch combinatorHook, dh decisionHook, sh samplingHook) (*correlatedProcessor, error) {
	filters, err := buildFilters(logger, cfg)
	if err != nil {
		return nil, err
	}

	p := &correlatedProcessor{
		logger:          logger,
		combinatorHook:  ch,
		decisionHook:    dh,
		samplingHook:    sh,
		filters:         filters,
		samplingTracker: newSamplingTracker(logger, cfg.ID),
		deleteChan:      make(chan pdata.TraceID, cfg.NumTraces),
	}

	b := newBatcher(logger, cfg.DecisionWait, p.makeDecision)
	p.batcher = b

	return p, nil
}

func buildFilters(logger *zap.Logger, cfg Config) ([]sampling.Filter, error) {
	var filters []sampling.Filter
	for i := range cfg.FilterCfgs {
		filterCfg := &cfg.FilterCfgs[i]

		filter, err := newFilter(logger, filterCfg)
		if err != nil {
			return nil, err
		}

		filters = append(filters, filter)
	}

	return filters, nil
}

func (c *correlatedProcessor) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (c *correlatedProcessor) Shutdown(ctx context.Context) error {
	c.batcher.FlushAll()
	return nil
}

func (c *correlatedProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}

// 1. Check if signal is late arriving.
//  1.1 If yes, then jump to step 4.  (m[TraceID] != nil)
//  1.2 If no, continue to step 2.
// 2. Batch signals by TraceID. (after this step all operations must be performed on the whole batch only)
//  2.1 Add to traceID-decision map, m[TraceID] = decision.Pending.
//  2.2 Wait DecisionWait time.
// 3. Apply sampling policies and update map with decision.
// 4. Apply sampling decision
//  4.1 If decision == sampled, then forward to next consumer.
//  4.2 If decision == not sampled, then drop.

func (c *correlatedProcessor) ConsumeSignal(s signal) {
	decision := c.samplingTracker.GetDecision(s.TraceID)

	// new trace
	if decision == sampling.Unspecified {
		c.newTrace(s.TraceID)
	}

	if decision == sampling.Unspecified || decision == sampling.Pending {
		c.addToBatch(s)
	} else {
		c.applyDecision(s)
	}
}

func (c *correlatedProcessor) newTrace(traceID pdata.TraceID) {
	full := true
	for full {
		select {
		case c.deleteChan <- traceID:
			// there is space to add new trace
			full = false
		default:
			// we need to delete a trace before adding a new one
			evictID := <-c.deleteChan
			c.batcher.Flush(evictID)
			c.samplingTracker.Delete(evictID)
		}
	}
}

func (c *correlatedProcessor) addToBatch(s signal) {
	err := c.batcher.Add(s)
	if err != nil {
		panic(err)
	}

	c.samplingTracker.SetDecision(s.TraceID, sampling.Pending)
}

func (c *correlatedProcessor) makeDecision(xs []*signal) {
	if len(xs) < 1 {
		return
	}

	var finalDecision sampling.Decision

	traceID := xs[0].TraceID
	batch := c.combinatorHook(xs)

	for _, policy := range c.filters {
		decision, err := c.decisionHook(policy, traceID, batch)
		if err != nil {
			decision = sampling.NotSampled
		}

		if finalDecision != sampling.Sampled {
			finalDecision = decision
		}
	}

	c.samplingTracker.SetDecision(traceID, finalDecision)
	c.applyDecision(signal{TraceID: traceID, Data: batch})
}

func (c *correlatedProcessor) applyDecision(s signal) {
	decision := c.samplingTracker.GetDecision(s.TraceID)
	if decision == sampling.Sampled {
		// send to next consumer
		c.samplingHook(s.Data)
	}
}
