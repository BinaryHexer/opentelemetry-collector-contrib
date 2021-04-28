package correlatedsamplingprocessor

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlatedsamplingprocessor/sampling"
)

func newFilter(logger *zap.Logger, cfg *FilterCfgs) (sampling.Filter, error) {
	switch cfg.Type {
	case NumericAttribute:
		return nil, nil
	case StringAttribute:
		return nil, nil
	case PercentageSample:
		return sampling.NewPercentageSample(logger, cfg.PercentageSamplingCfg.SamplingPercentage), nil
	default:
		return nil, fmt.Errorf("unknown sampling filter %s", cfg.Type)
	}
}
