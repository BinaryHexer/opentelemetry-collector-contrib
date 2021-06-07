package correlationsamplingprocessor

import (
	"context"
	"testing"
	"time"

	"math/rand"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/consumer/pdata"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/correlation"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
	"sync"
)

func TestSequentialLogSampling(t *testing.T) {
	t.Cleanup(sampling.ResetTrackers)
	t.Cleanup(correlation.ResetShards)

	const decisionWait = time.Second
	const delta = 0
	const samplingPercentage = 100
	numTraces := 100
	_, batches1 := generateLogBatches(numTraces, true)
	_, batches2 := generateLogBatches(numTraces, true)
	cfg := Config{
		CorrelationID: "sequential-log",
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

	msp := new(consumertest.LogsSink)
	tp, _ := newLogsProcessor(context.TODO(), zap.NewNop(), msp, cfg)

	err := tp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	for _, batch := range batches1 {
		err := tp.ConsumeLogs(context.Background(), batch)
		assert.NoError(t, err)
	}

	time.Sleep(decisionWait)

	for _, batch := range batches2 {
		err := tp.ConsumeLogs(context.Background(), batch)
		assert.NoError(t, err)
	}

	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)

	obsRatio := (float64(getTraceIDCountFromLogs(msp.AllLogs())) / float64(numTraces)) * 100
	assert.InDelta(t, samplingPercentage, obsRatio, delta)
}

func TestConcurrentLogSampling(t *testing.T) {
	t.Cleanup(sampling.ResetTrackers)
	t.Cleanup(correlation.ResetShards)

	const decisionWait = time.Second
	const delta = 0
	const samplingPercentage = 100
	numTraces := 100
	_, batches := generateLogBatches(numTraces, true)
	cfg := Config{
		CorrelationID: "concurrent-log",
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

	msp := new(consumertest.LogsSink)
	tp, _ := newLogsProcessor(context.TODO(), zap.NewNop(), msp, cfg)

	err := tp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for _, batch := range batches {
			err := tp.ConsumeLogs(context.Background(), batch)
			assert.NoError(t, err)
		}
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for _, batch := range batches {
			err := tp.ConsumeLogs(context.Background(), batch)
			assert.NoError(t, err)
		}
		wg.Done()
	}()
	wg.Wait()

	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)

	obsRatio := (float64(getTraceIDCountFromLogs(msp.AllLogs())) / float64(numTraces)) * 100
	assert.InDelta(t, samplingPercentage, obsRatio, delta)
}

func TestLogsWithoutTraceIDSampling(t *testing.T) {
	t.Cleanup(sampling.ResetTrackers)
	t.Cleanup(correlation.ResetShards)

	const decisionWait = time.Second
	const delta = 0
	const samplingPercentage = 100
	numTraces := 1000
	count, batches := generateLogBatches(numTraces, false)
	cfg := Config{
		CorrelationID: "log-without-traceID",
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

	msp := new(consumertest.LogsSink)
	tp, _ := newLogsProcessor(context.TODO(), zap.NewNop(), msp, cfg)

	err := tp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	for _, batch := range batches {
		err := tp.ConsumeLogs(context.Background(), batch)
		assert.NoError(t, err)
	}

	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)

	obsRatio := (float64(len(msp.AllLogs())) / float64(count)) * 100
	assert.InDelta(t, samplingPercentage, obsRatio, delta)
}

func generateLogBatches(numIds int, setTraceID bool) (int64, []pdata.Logs) {
	r1 := rand.New(rand.NewSource(time.Now().UnixNano()))
	traceIds := make([]pdata.TraceID, numIds)
	spanID := 0
	count := int64(0)
	var lds []pdata.Logs
	for i := 0; i < numIds; i++ {
		traceIds[i] = tracetranslator.UInt64ToTraceID(1, uint64(i+1))
		// Send each log in a separate batch
		maxLogs := r1.Intn(100)
		spanIDOffset := r1.Uint64()
		for j := 0; j <= maxLogs; j++ {
			ld := simpleLogs()
			log := ld.ResourceLogs().At(0).InstrumentationLibraryLogs().At(0).Logs().At(0)

			if setTraceID {
				log.SetTraceID(traceIds[i])
				spanID++
				log.SetSpanID(tracetranslator.UInt64ToSpanID(spanIDOffset + uint64(spanID)))
			} else {
				log.SetTraceID(pdata.InvalidTraceID())
				log.SetSpanID(pdata.InvalidSpanID())
			}

			lds = append(lds, ld)
			count++
		}
	}

	return count, lds
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

func getTraceIDCountFromLogs(logs []pdata.Logs) int {
	t1 := combineLogs(logs)
	t2 := groupLogsByTraceID(t1)

	return len(t2)
}
