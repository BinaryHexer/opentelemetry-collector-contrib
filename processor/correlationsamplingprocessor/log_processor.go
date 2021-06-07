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

type logProcessor struct {
	ctx          context.Context
	nextConsumer consumer.Logs
	processor    correlation.Processor
	ticker       *time.Ticker
}

func newLogsProcessor(ctx context.Context, logger *zap.Logger, nextConsumer consumer.Logs, cfg Config) (component.LogsProcessor, error) {
	if nextConsumer == nil {
		return nil, componenterror.ErrNilNextConsumer
	}

	lp := &logProcessor{
		ctx:          ctx,
		nextConsumer: nextConsumer,
		ticker:       time.NewTicker(time.Second),
	}
	ch := func(batches []*correlation.Signal) (batch interface{}) {
		logs := make([]pdata.Logs, len(batches))
		for i, b := range batches {
			logs[i] = b.Data.(pdata.Logs)
		}

		return combineLogs(logs)
	}
	dh := func(filter sampling.Filter, traceID pdata.TraceID, ld interface{}) (sampling.Decision, error) {
		return lp.decisionHook(filter, traceID, ld.(pdata.Logs))
	}
	sh := func(ld interface{}) {
		lp.samplingHook(ld.(pdata.Logs))
	}

	p, err := correlation.NewShardedProcessor(logger, cfg, ch, dh, sh)
	if err != nil {
		return nil, err
	}
	lp.processor = p

	return lp, nil
}

func (lp *logProcessor) decisionHook(filter sampling.Filter, traceID pdata.TraceID, ld pdata.Logs) (sampling.Decision, error) {
	return filter.ApplyForLog(traceID, ld)
}

func combineLogs(logBatches []pdata.Logs) pdata.Logs {
	combinedLogs := pdata.NewLogs()
	for j := 0; j < len(logBatches); j++ {
		batch := logBatches[j]
		batch.ResourceLogs().MoveAndAppendTo(combinedLogs.ResourceLogs())
	}

	return combinedLogs
}

func (lp *logProcessor) samplingHook(ld pdata.Logs) {
	_ = lp.nextConsumer.ConsumeLogs(lp.ctx, ld)

	stats.Record(context.Background(), mReleasedLogs.M(int64(ld.LogRecordCount())), mReleasedTraces.M(1))
}

func (lp *logProcessor) Start(ctx context.Context, host component.Host) error {
	err := lp.processor.Start(ctx, host)
	if err != nil {
		return err
	}

	go lp.periodicMetrics()
	return nil
}

func (lp *logProcessor) Shutdown(ctx context.Context) error {
	lp.ticker.Stop()
	return lp.processor.Shutdown(ctx)
}

func (lp *logProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (lp *logProcessor) ConsumeLogs(ctx context.Context, ld pdata.Logs) error {
	var errors []error

	logs := groupLogsByTraceID(ld)
	for traceID, log := range logs {
		s := correlation.Signal{
			TraceID: traceID,
			Data:    log,
		}
		err := lp.processor.ConsumeSignal(s)
		if err != nil {
			errors = append(errors, err)
		}
	}

	return consumererror.Combine(errors)
}

func (lp *logProcessor) periodicMetrics() {
	for range lp.ticker.C {
		m := lp.processor.Metrics()

		stats.Record(context.Background(), mNumTracesInMemory.M(m.TraceCount))
		stats.Record(context.Background(), mNumSamplingDecisionInMemory.M(m.SamplingDecisionCount))
		stats.Record(context.Background(), mTracesEvicted.M(m.EvictCount))
	}
}

func groupLogsByTraceID(ld pdata.Logs) map[pdata.TraceID]pdata.Logs {
	m := make(map[pdata.TraceID]pdata.Logs)

	rlogs := ld.ResourceLogs()
	for i := 0; i < rlogs.Len(); i++ {
		rl := rlogs.At(i)
		illl := rl.InstrumentationLibraryLogs()
		for j := 0; j < illl.Len(); j++ {
			ill := illl.At(j)
			for k := 0; k < ill.Logs().Len(); k++ {
				logRecord := ill.Logs().At(k)
				traceID := logRecord.TraceID()
				//if traceID == pdata.InvalidTraceID() {
				//	traceID = random()
				//}

				if _, ok := m[traceID]; !ok {
					log := pdata.NewLogs()
					newRL := buildResourceLog(rl, ill, logRecord)
					log.ResourceLogs().Append(newRL)

					m[traceID] = log
				}

				newRL := buildResourceLog(rl, ill, logRecord)
				m[traceID].ResourceLogs().Append(newRL)
			}
		}
	}

	return m
}

func buildResourceLog(rl pdata.ResourceLogs, ill pdata.InstrumentationLibraryLogs, logRecord pdata.LogRecord) pdata.ResourceLogs {
	newRL := pdata.NewResourceLogs()
	newILL := pdata.NewInstrumentationLibraryLogs()

	rl.Resource().CopyTo(newRL.Resource())
	ill.InstrumentationLibrary().CopyTo(newILL.InstrumentationLibrary())

	newILL.Logs().Append(logRecord)
	newRL.InstrumentationLibraryLogs().Append(newILL)

	return newRL
}
