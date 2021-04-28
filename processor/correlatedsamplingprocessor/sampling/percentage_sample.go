package sampling

import (
	"math"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"github.com/CAFxX/fastrand"
	"time"
)

type percentageSample struct {
	logger  *zap.Logger
	sampler Sampler
}

var _ Filter = (*percentageSample)(nil)

// NewPercentageSample creates a filter to sample a percentage of signals.
func NewPercentageSample(logger *zap.Logger, samplingPercentage int64) *percentageSample {
	allowRatio := math.Max(math.Min(float64(samplingPercentage), 100), 0) / 100

	return &percentageSample{
		logger:  logger,
		sampler: NewSampler(allowRatio),
	}
}

func (ps *percentageSample) Name() string {
	return "percentage_sample"
}

func (ps *percentageSample) ApplyForTrace(traceID pdata.TraceID, trace pdata.Traces) (Decision, error) {
	ps.logger.Debug("Evaluating spans in percentage-sample filter")

	if ps.sampler.Sample() {
		return Sampled, nil
	}

	return NotSampled, nil
}

func (ps *percentageSample) ApplyForLog(traceID pdata.TraceID, log pdata.Logs) (Decision, error) {
	ps.logger.Debug("Evaluating logs in percentage-sample filter")

	if ps.sampler.Sample() {
		return Sampled, nil
	}

	return NotSampled, nil
}

const (
	total = 1.0
)

type Sampler interface {
	Sample() bool
}

type sampler struct {
	gen       *fastrand.AtomicSplitMix64
	threshold float64 // 0 means all and 1.0 means none
}

func NewSampler(allowRatio float64) *sampler {
	gen := &fastrand.AtomicSplitMix64{}
	gen.Seed(uint64(time.Now().Unix()))

	return &sampler{
		gen:       gen,
		threshold: total - allowRatio,
	}
}

func (s *sampler) Sample() bool {
	t := float64(s.gen.Uint64()) / float64(1 << 64)
	return t > s.threshold
}
