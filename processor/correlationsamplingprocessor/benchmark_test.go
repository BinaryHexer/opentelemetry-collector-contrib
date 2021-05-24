package correlationsamplingprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/CAFxX/fastrand"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/correlation"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
)

func BenchmarkBasic(b *testing.B) {
	b.Run("CorrelatedSampling", func(b *testing.B) {
		// setup
		b.Cleanup(sampling.ResetTrackers)
		b.Cleanup(correlation.ResetShards)
		const decisionWait = time.Second * 10
		const samplingPercentage = 100
		cfg := Config{
			DecisionWait: decisionWait,
			NumTraces:    uint64(100000),
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

	//b.Run("TailSampling", func(b *testing.B) {
	//	// setup
	//	const decisionWait = time.Second * 10
	//	cfg := tailsamplingprocessor.Config{
	//		DecisionWait: decisionWait,
	//		NumTraces:    uint64(100000),
	//		PolicyCfgs: []tailsamplingprocessor.PolicyCfg{
	//			{
	//				Name: "test-policy",
	//				Type: tailsamplingprocessor.AlwaysSample,
	//			},
	//		},
	//	}
	//	msp := new(consumertest.TracesSink)
	//	tp, _ := tailsamplingprocessor.NewTracesProcessor(zap.NewNop(), msp, cfg)
	//	err := tp.Start(context.Background(), componenttest.NewNopHost())
	//	assert.NoError(b, err)
	//
	//	r1 := fastrand.NewShardedSplitMix64()
	//	// reset timer
	//	b.ResetTimer()
	//	// measure
	//	b.RunParallel(func(pb *testing.PB) {
	//		for pb.Next() {
	//			_ = tp.ConsumeTraces(context.Background(), simpleTracesWithInt(r1.Uint64()))
	//		}
	//	})
	//	err = tp.Shutdown(context.Background())
	//	assert.NoError(b, err)
	//	b.StopTimer()
	//	time.Sleep(12 * time.Second)
	//})
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
