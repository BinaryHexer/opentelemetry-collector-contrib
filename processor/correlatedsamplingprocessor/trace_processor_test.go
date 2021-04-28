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
	"github.com/CAFxX/fastrand"
)

func TestBasicTraceSampling(t *testing.T) {
	t.Cleanup(reset)

	const decisionWait = time.Second
	const delta = 5 // 5% error
	const samplingPercentage = 50
	numTraces := 1000
	traceIds, batches := generateTraceBatches(numTraces)
	cfg := Config{
		ID:           "trace",
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

	msp := new(consumertest.TracesSink)
	tp, _ := newTraceProcessor(context.TODO(), zap.NewNop(), msp, cfg)

	err := tp.Start(context.Background(), componenttest.NewNopHost())
	assert.NoError(t, err)

	for _, batch := range batches {
		err := tp.ConsumeTraces(context.Background(), batch)
		assert.NoError(t, err)
	}

	err = tp.Shutdown(context.Background())
	assert.NoError(t, err)

	obsRatio := (float64(len(msp.AllTraces())) / float64(numTraces)) * 100
	assert.InDelta(t, samplingPercentage, obsRatio, delta)
}

func BenchmarkBasic(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		// setup
		b.Cleanup(reset)
		const decisionWait = time.Second * 10
		const samplingPercentage = 100
		cfg := Config{
			DecisionWait: decisionWait,
			NumTraces:    uint64(100000),
			FilterCfgs: []FilterCfgs{
				{
					Name:                  "test-policy",
					Type:                  PercentageSample,
					PercentageSamplingCfg: PercentageSamplingCfg{SamplingPercentage: samplingPercentage},
				},
			},
		}
		msp := new(consumertest.TracesSink)
		tp, _ := newTraceProcessor(context.TODO(), zap.NewNop(), msp, cfg)
		err := tp.Start(context.Background(), componenttest.NewNopHost())
		assert.NoError(b, err)

		r1 := fastrand.NewShardedSplitMix64()
		// reset timer
		b.ResetTimer()
		// measure
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_ = tp.ConsumeTraces(context.Background(), simpleTracesWithInt(r1.Uint64()))
			}
		})
		err = tp.Shutdown(context.Background())
		assert.NoError(b, err)
	})
}

func BenchmarkTraceGen(b *testing.B) {
	b.Run("Simple", func(b *testing.B) {
		r1 := fastrand.NewShardedSplitMix64()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				simpleTracesWithInt(r1.Uint64())
			}
		})
	})
}

func generateTraceBatches(numIds int) ([]pdata.TraceID, []pdata.Traces) {
	traceIds := make([]pdata.TraceID, numIds)
	spanID := 0
	var tds []pdata.Traces
	for i := 0; i < numIds; i++ {
		traceIds[i] = tracetranslator.UInt64ToTraceID(1, uint64(i+1))
		// Send each span in a separate batch
		for j := 0; j <= i; j++ {
			td := simpleTraces()
			span := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans().At(0)
			span.SetTraceID(traceIds[i])

			spanID++
			span.SetSpanID(tracetranslator.UInt64ToSpanID(uint64(spanID)))
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
