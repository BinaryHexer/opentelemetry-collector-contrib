package correlationsamplingprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/consumer/consumererror"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/correlation"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
	"go.opencensus.io/stats"
	"time"
)

type traceProcessor struct {
	ctx          context.Context
	nextConsumer consumer.Traces
	processor    correlation.Processor
	ticker       *time.Ticker
}

func newTracesProcessor(ctx context.Context, logger *zap.Logger, nextConsumer consumer.Traces, cfg Config) (component.TracesProcessor, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	tp := &traceProcessor{
		ctx:          ctx,
		nextConsumer: nextConsumer,
		ticker:       time.NewTicker(time.Second),
	}
	ch := func(batches []*correlation.Signal) (batch interface{}) {
		traces := make([]pdata.Traces, len(batches))
		for i, b := range batches {
			traces[i] = b.Data.(pdata.Traces)
		}

		return combineTraces(traces)
	}
	dh := func(filter sampling.Filter, traceID pdata.TraceID, td interface{}) (sampling.Decision, error) {
		return tp.decisionHook(filter, traceID, td.(pdata.Traces))
	}
	sh := func(td interface{}) {
		tp.samplingHook(td.(pdata.Traces))
	}

	p, err := correlation.NewShardedProcessor(logger, cfg, ch, dh, sh)
	if err != nil {
		return nil, err
	}
	tp.processor = p

	return tp, nil
}

func (tp *traceProcessor) decisionHook(filter sampling.Filter, traceID pdata.TraceID, td pdata.Traces) (sampling.Decision, error) {
	return filter.ApplyForTrace(traceID, td)
}

func combineTraces(traceBatches []pdata.Traces) pdata.Traces {
	combinedTraces := pdata.NewTraces()
	for j := 0; j < len(traceBatches); j++ {
		batch := traceBatches[j]
		batch.ResourceSpans().MoveAndAppendTo(combinedTraces.ResourceSpans())
	}

	return combinedTraces
}

func (tp *traceProcessor) samplingHook(td pdata.Traces) {
	_ = tp.nextConsumer.ConsumeTraces(tp.ctx, td)

	stats.Record(context.Background(), mReleasedSpans.M(int64(td.SpanCount())), mReleasedTraces.M(1))
}

func (tp *traceProcessor) Start(ctx context.Context, host component.Host) error {
	err := tp.processor.Start(ctx, host)
	if err != nil {
		return err
	}

	go tp.periodicMetrics()
	return nil
}

func (tp *traceProcessor) Shutdown(ctx context.Context) error {
	tp.ticker.Stop()
	return tp.processor.Shutdown(ctx)
}

func (tp *traceProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (tp *traceProcessor) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	var errors []error

	traces := groupSpansByTraceID(td)
	for traceID, trace := range traces {
		s := correlation.Signal{
			TraceID: traceID,
			Data:    trace,
		}
		err := tp.processor.ConsumeSignal(s)
		if err != nil {
			errors = append(errors, err)
		}
	}

	return consumererror.Combine(errors)
}

func (tp *traceProcessor) periodicMetrics() {
	for range tp.ticker.C {
		m := tp.processor.Metrics()

		stats.Record(context.Background(), mNumTracesInMemory.M(m.TraceCount))
		stats.Record(context.Background(), mNumSamplingDecisionInMemory.M(m.SamplingDecisionCount))
		stats.Record(context.Background(), mTracesEvicted.M(m.EvictCount))
	}
}

func groupSpansByTraceID(td pdata.Traces) map[pdata.TraceID]pdata.Traces {
	m := make(map[pdata.TraceID]pdata.Traces)

	rspans := td.ResourceSpans()
	for i := 0; i < rspans.Len(); i++ {
		rs := rspans.At(i)
		ilss := rs.InstrumentationLibrarySpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			for k := 0; k < ils.Spans().Len(); k++ {
				span := ils.Spans().At(k)
				traceID := span.TraceID()

				if _, ok := m[traceID]; !ok {
					trace := pdata.NewTraces()
					m[traceID] = trace
				}

				newRS := buildResourceSpan(rs, ils, span)
				m[traceID].ResourceSpans().Append(newRS)
			}
		}
	}

	return m
}

func buildResourceSpan(rs pdata.ResourceSpans, ils pdata.InstrumentationLibrarySpans, span pdata.Span) pdata.ResourceSpans {
	newRS := pdata.NewResourceSpans()
	newILS := pdata.NewInstrumentationLibrarySpans()

	rs.Resource().CopyTo(newRS.Resource())
	ils.InstrumentationLibrary().CopyTo(newILS.InstrumentationLibrary())

	newILS.Spans().Append(span)
	newRS.InstrumentationLibrarySpans().Append(newILS)

	return newRS
}
