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
)

func TestBasicTraceSampling(t *testing.T) {
	t.Cleanup(sampling.ResetTrackers)
	t.Cleanup(correlation.ResetShards)

	const decisionWait = time.Second
	const delta = 0
	const samplingPercentage = 100
	numTraces := 100
	_, batches1 := generateTraceBatches(numTraces)
	_, batches2 := generateTraceBatches(numTraces)
	cfg := Config{
		ID:           "basic-trace",
		DecisionWait: decisionWait,
		NumTraces:    uint64(numTraces),
		FilterCfgs: []sampling.FilterCfgs{
			{
				Name:          "test-policy",
				Type:          sampling.Percentage,
				PercentageCfg: sampling.PercentageCfg{SamplingPercentage: samplingPercentage},
			},
		},
	}

	msp := new(consumertest.TracesSink)
	tp, _ := newTracesProcessor(context.TODO(), zap.NewNop(), msp, cfg)

	err := tp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	for _, batch := range batches1 {
		err := tp.ConsumeTraces(context.Background(), batch)
		assert.NoError(t, err)
	}

	time.Sleep(decisionWait)

	for _, batch := range batches2 {
		err := tp.ConsumeTraces(context.Background(), batch)
		assert.NoError(t, err)
	}

	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)

	obsRatio := (float64(getTraceIDCountFromTraces(msp.AllTraces())) / float64(numTraces)) * 100
	assert.InDelta(t, samplingPercentage, obsRatio, delta)
}

func generateTraceBatches(numIds int) ([]pdata.TraceID, []pdata.Traces) {
	r1 := rand.New(rand.NewSource(time.Now().UnixNano()))
	traceIds := make([]pdata.TraceID, numIds)
	spanID := 0
	var tds []pdata.Traces
	for i := 0; i < numIds; i++ {
		traceIds[i] = tracetranslator.UInt64ToTraceID(1, uint64(i+1))
		// Send each span in a separate batch
		maxSpans := r1.Intn(100)
		spanIDOffset := r1.Uint64()
		for j := 0; j <= maxSpans; j++ {
			td := simpleTraces()
			span := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans().At(0)
			span.SetTraceID(traceIds[i])

			spanID++
			span.SetSpanID(tracetranslator.UInt64ToSpanID(spanIDOffset + uint64(spanID)))
			tds = append(tds, td)
		}
	}

	return traceIds, tds
}

func simpleTraces() pdata.Traces {
	return simpleTracesWithID(pdata.NewTraceID([16]byte{1, 2, 3, 4}))
}

func simpleTracesWithInt(id uint64) pdata.Traces {
	traceID := tracetranslator.UInt64ToTraceID(1, id+1)

	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(1)
	rs := traces.ResourceSpans().At(0)
	rs.InstrumentationLibrarySpans().Resize(1)
	ils := rs.InstrumentationLibrarySpans().At(0)
	ils.Spans().Resize(1)
	ils.Spans().At(0).SetTraceID(traceID)
	return traces
}

func simpleTracesWithID(traceID pdata.TraceID) pdata.Traces {
	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(1)
	rs := traces.ResourceSpans().At(0)
	rs.InstrumentationLibrarySpans().Resize(1)
	ils := rs.InstrumentationLibrarySpans().At(0)
	ils.Spans().Resize(1)
	ils.Spans().At(0).SetTraceID(traceID)
	return traces
}

func getTraceIDCountFromTraces(traces []pdata.Traces) int {
	t1 := combineTraces(traces)
	t2 := groupSpansByTraceID(t1)

	return len(t2)
}
