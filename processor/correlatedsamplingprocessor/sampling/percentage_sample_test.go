package sampling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.opentelemetry.io/collector/consumer/pdata"
	"github.com/google/uuid"
	"github.com/CAFxX/fastrand"
)

func TestPercentageSampleFilter(t *testing.T) {
	const iter = 100000
	const delta = 1

	cases := map[string]struct {
		allowRatio int64
	}{
		"100%": {allowRatio: 100},
		"95%":  {allowRatio: 95},
		"90%":  {allowRatio: 90},
		"80%":  {allowRatio: 80},
		"70%":  {allowRatio: 70},
		"60%":  {allowRatio: 60},
		"50%":  {allowRatio: 50},
		"40%":  {allowRatio: 40},
		"30%":  {allowRatio: 30},
		"20%":  {allowRatio: 20},
		"10%":  {allowRatio: 10},
		"5%":   {allowRatio: 5},
		"0%":   {allowRatio: 0},
	}

	for name, tt := range cases {
		name := name
		tt := tt

		t.Run(name, func(t *testing.T) {
			// setup
			counter := 0
			filter := NewPercentageSample(zap.NewNop(), tt.allowRatio)

			// execute
			for i := 0; i < iter; i++ {
				u, _ := uuid.NewRandom()
				traceID := pdata.NewTraceID(u)
				trace := simpleTracesWithID(traceID)

				decision, err := filter.ApplyForTrace(traceID, trace)
				assert.NoError(t, err)

				if decision == Sampled {
					counter++
				}
			}

			// verify
			obsRatio := (float64(counter) / iter) * 100
			assert.InDelta(t, tt.allowRatio, obsRatio, delta)
		})
	}
}

func TestRand(t *testing.T)  {
	gen := fastrand.NewShardedSplitMix64()

	const iter = 1000
	for i := 0; i < iter; i++ {
		r := float64(gen.Uint64()) / float64(1 << 64)
		if r < 0 || r > 1 {
			t.Errorf("wrong value: %#v", r)
		}
	}
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
