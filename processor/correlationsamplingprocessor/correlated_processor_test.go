package correlationsamplingprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/correlation"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
)

func TestBasicCorrelatedSampling(t *testing.T) {
	t.Cleanup(sampling.ResetTrackers)
	t.Cleanup(correlation.ResetShards)

	const decisionWait = time.Second
	const samplingPercentage = 10
	const delta = 5 // error margin
	numTraces := 10000

	_, traceBatches := generateTraceBatches(numTraces)
	_, logBatches := generateLogBatches(numTraces, true)

	cfg := Config{
		CorrelationID: "correlation",
		DecisionWait:  decisionWait,
		NumTraces:     uint64(numTraces),
		FilterCfgs: []sampling.FilterCfgs{
			{
				Name:          "test-policy",
				Type:          sampling.Percentage,
				PercentageCfg: sampling.PercentageCfg{SamplingPercentage: samplingPercentage},
			},
		},
	}

	traceSink := new(consumertest.TracesSink)
	logSink := new(consumertest.LogsSink)

	tp, err := newTracesProcessor(context.TODO(), zap.NewNop(), traceSink, cfg)
	assert.NoError(t, err)
	lp, err := newLogsProcessor(context.TODO(), zap.NewNop(), logSink, cfg)
	assert.NoError(t, err)

	err = tp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)
	err = lp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for _, batch := range traceBatches {
			err := tp.ConsumeTraces(context.Background(), batch)
			assert.NoError(t, err)
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for _, batch := range logBatches {
			err := lp.ConsumeLogs(context.Background(), batch)
			assert.NoError(t, err)
		}
		wg.Done()
	}()
	wg.Wait()

	time.Sleep(decisionWait)

	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)
	err = lp.Shutdown(context.Background())
	assert.NoError(t, err)

	tIDs, lIDs := check(traceSink.AllTraces(), logSink.AllLogs())
	traceRatio := (float64(len(tIDs)) / float64(numTraces)) * 100
	assert.InDelta(t, samplingPercentage, traceRatio, delta)
	logRatio := (float64(len(lIDs)) / float64(numTraces)) * 100
	assert.InDelta(t, samplingPercentage, logRatio, delta)
	assert.ElementsMatch(t, tIDs, lIDs)
}

func check(traces []pdata.Traces, logs []pdata.Logs) ([]string, []string) {
	t := groupSpansByTraceID(combineTraces(traces))
	l := groupLogsByTraceID(combineLogs(logs))

	x := make([]string, 0)
	for traceID := range t {
		x = append(x, traceID.HexString())
	}

	y := make([]string, 0)
	for traceID := range l {
		y = append(y, traceID.HexString())
	}

	return x, y
}
