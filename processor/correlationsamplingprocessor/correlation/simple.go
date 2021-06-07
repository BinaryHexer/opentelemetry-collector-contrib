package correlation

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"google.golang.org/api/support/bundler"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
	"go.opentelemetry.io/collector/consumer"
	"sync/atomic"
)

var _ Processor = (*correlatedProcessor)(nil)

type correlatedProcessor struct {
	logger          *zap.Logger
	combinatorHook  combinatorHook
	decisionHook    decisionHook
	samplingHook    samplingHook
	filters         []sampling.Filter
	samplingTracker sampling.Tracker
	deleteChan      chan pdata.TraceID
	batcher         batcher
	bundler         *bundler.Bundler
	// metrics
	traceCount uint64
	evictCount uint64
}

func NewProcessor(logger *zap.Logger, cfg Config, ch combinatorHook, dh decisionHook, sh samplingHook) (*correlatedProcessor, error) {
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
		samplingTracker: sampling.NewTracker(logger, cfg.CorrelationID),
		deleteChan:      make(chan pdata.TraceID, cfg.NumTraces),
	}

	b := newBatcher(logger, cfg.DecisionWait, p.makeDecision)
	p.batcher = b

	var e Signal
	bb := bundler.NewBundler(&e, func(i interface{}) {
		batch := i.([]*Signal)
		for _, x := range batch {
			p.applyDecision(*x)
			// subtract one from traceCount per https://godoc.org/sync/atomic#AddUint64
			atomic.AddUint64(&p.traceCount, ^uint64(0))
		}
	})
	bb.DelayThreshold = cfg.DecisionWait // TODO: use another cfg field
	bb.BundleCountThreshold = 1000000
	bb.BundleByteThreshold = 1e6 * 10 // 10M
	bb.BundleByteLimit = 0            // unlimited
	bb.BufferedByteLimit = 1e9        // 1G
	bb.HandlerLimit = 1
	p.bundler = bb

	return p, nil
}

func buildFilters(logger *zap.Logger, cfg Config) ([]sampling.Filter, error) {
	var filters []sampling.Filter
	for i := range cfg.FilterCfgs {
		filterCfg := &cfg.FilterCfgs[i]

		filter, err := sampling.NewFilter(logger, filterCfg)
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
	c.batcher.Close()
	c.bundler.Flush()

	return nil
}

func (c *correlatedProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
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

func (c *correlatedProcessor) ConsumeSignal(s Signal) error {
	decision := c.samplingTracker.GetDecision(s.TraceID)

	// new trace
	if decision == sampling.Unspecified {
		c.newTrace(s.TraceID)
	}

	if decision == sampling.Unspecified || decision == sampling.Pending || s.TraceID == pdata.InvalidTraceID() {
		err := c.addToBatch(s)
		if err != nil {
			return err
		}
	} else {
		c.applyDecision(s)
	}

	return nil
}

func (c *correlatedProcessor) newTrace(traceID pdata.TraceID) {
	full := true
	for full {
		select {
		case c.deleteChan <- traceID:
			// there is space to add new trace
			full = false
		default:
			// need to delete a trace before adding a new one
			evictID := <-c.deleteChan
			c.batcher.Flush(evictID)
			c.samplingTracker.Delete(evictID)
			atomic.AddUint64(&c.evictCount, 1)
		}
	}
}

func (c *correlatedProcessor) addToBatch(s Signal) error {
	err := c.batcher.Add(s)
	if err != nil {
		return err
	}

	c.samplingTracker.SetDecision(s.TraceID, sampling.Pending)
	atomic.AddUint64(&c.traceCount, 1)

	return nil
}

func (c *correlatedProcessor) makeDecision(xs []*Signal) {
	if len(xs) < 1 {
		return
	}

	traceID := xs[0].TraceID
	if traceID == pdata.InvalidTraceID() {
		// optimize is required due to logs without traceID, these logs cannot be combined based on traceID.
		c.optimize(xs)
		return
	}

	batch := c.combinatorHook(xs)
	finalDecision := c.applyFilters(traceID, batch)
	c.samplingTracker.SetDecision(traceID, finalDecision)

	// must add to bundler even if the decision is not sampled since the decision might change due to correlation.
	_ = c.bundler.Add(&Signal{TraceID: traceID, Data: batch}, 1)
}

func (c *correlatedProcessor) optimize(batch []*Signal) {
	for _, s := range batch {
		decision := c.applyFilters(s.TraceID, s.Data)
		if decision == sampling.Sampled {
			// send to next consumer
			c.samplingHook(s.Data)
		}
	}
}

func (c *correlatedProcessor) applyFilters(traceID pdata.TraceID, batch interface{}) sampling.Decision {
	var finalDecision sampling.Decision
	for _, policy := range c.filters {
		decision, err := c.decisionHook(policy, traceID, batch)
		if err != nil {
			decision = sampling.NotSampled
		}

		if finalDecision != sampling.Sampled {
			finalDecision = decision
		}
	}

	return finalDecision
}

func (c *correlatedProcessor) applyDecision(s Signal) {
	decision := c.samplingTracker.GetDecision(s.TraceID)
	if decision == sampling.Sampled {
		// send to next consumer
		c.samplingHook(s.Data)
	}
}

func (c *correlatedProcessor) Metrics() ProcessorMetrics {
	return ProcessorMetrics{
		TraceCount:            int64(atomic.LoadUint64(&c.traceCount)),
		SamplingDecisionCount: c.samplingTracker.Count(),
		EvictCount:            int64(atomic.LoadUint64(&c.evictCount)),
	}
}
