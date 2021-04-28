package correlatedsamplingprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlatedsamplingprocessor/sampling"
)

type traceProcessor struct {
	ctx          context.Context
	nextConsumer consumer.Traces
	processor    *correlatedProcessor
}

func newTraceProcessor(ctx context.Context, logger *zap.Logger, nextConsumer consumer.Traces, cfg Config) (component.TracesProcessor, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	tp := &traceProcessor{
		ctx:          ctx,
		nextConsumer: nextConsumer,
	}
	ch := func(batches []*signal) (batch interface{}) {
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

	p, err := newCProcessor(logger, cfg, ch, dh, sh)
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
}

func (tp *traceProcessor) Start(ctx context.Context, host component.Host) error {
	return tp.processor.Start(ctx, host)
}

func (tp *traceProcessor) Shutdown(ctx context.Context) error {
	return tp.processor.Shutdown(ctx)
}

func (tp *traceProcessor) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}

func (tp *traceProcessor) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	traces := groupSpansByTraceID(td)
	for traceID, trace := range traces {
		s := signal{
			TraceID: traceID,
			Data:    trace,
		}
		tp.processor.ConsumeSignal(s)
	}

	return nil
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
