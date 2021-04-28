package correlatedsamplingprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/consumer/pdata"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
	"go.uber.org/zap"
)

func TestBasicLogSampling(t *testing.T) {
	t.Cleanup(reset)

	const decisionWait = time.Second
	const delta = 5 // 5% error
	const samplingPercentage = 50
	numTraces := 1000
	traceIds, batches := generateLogBatches(numTraces)
	cfg := Config{
		ID:           "log",
		DecisionWait: decisionWait,
		NumTraces:    uint64(2 * len(traceIds)),
		FilterCfgs: []FilterCfgs{
			{
				Name:                  "test-policy",
				Type:                  PercentageSample,
				PercentageSamplingCfg: PercentageSamplingCfg{SamplingPercentage: samplingPercentage},
			},
		},
	}

	msp := new(consumertest.LogsSink)
	tp, _ := newLogProcessor(context.TODO(), zap.NewNop(), msp, cfg)

	err := tp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	for _, batch := range batches {
		err := tp.ConsumeLogs(context.Background(), batch)
		assert.NoError(t, err)
	}

	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)

	obsRatio := (float64(len(msp.AllLogs())) / float64(numTraces)) * 100
	assert.InDelta(t, samplingPercentage, obsRatio, delta)
}

func generateLogBatches(numIds int) ([]pdata.TraceID, []pdata.Logs) {
	traceIds := make([]pdata.TraceID, numIds)
	spanID := 0
	var lds []pdata.Logs
	for i := 0; i < numIds; i++ {
		traceIds[i] = tracetranslator.UInt64ToTraceID(1, uint64(i+1))
		// Send each log in a separate batch
		for j := 0; j <= i; j++ {
			ld := simpleLogs()
			log := ld.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).Logs().At(0)
			log.SetTraceID(traceIds[i])

			spanID++
			log.SetSpanID(tracetranslator.UInt64ToSpanID(uint64(spanID)))
			lds = append(lds, ld)
		}
	}

	return traceIds, lds
}

func simpleLogs() pdata.Logs {
	return simpleLogsWithID(pdata.NewTraceID([16]byte{1, 2, 3, 4}))
}

func simpleLogsWithID(traceID pdata.TraceID) pdata.Logs {
	logs := pdata.NewLogs()
	logs.ResourceLogs().Resize(1)
	rl := logs.ResourceLogs().At(0)
	rl.InstrumentationLibraryLogs().Resize(1)
	ill := rl.InstrumentationLibraryLogs().At(0)
	ill.Logs().Resize(1)
	ill.Logs().At(0).SetTraceID(traceID)
	return logs
}
