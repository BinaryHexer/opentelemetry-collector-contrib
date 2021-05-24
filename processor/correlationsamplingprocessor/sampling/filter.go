package sampling

import (
	"fmt"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

// Decision gives the status of sampling decision.
type Decision int32

const (
	// Unspecified indicates that the status of the decision was not set yet.
	Unspecified Decision = iota
	// Pending indicates that the policy was not evaluated yet.
	Pending
	// Sampled is used to indicate that the decision was already taken
	// to sample the data.
	Sampled
	// NotSampled is used to indicate that the decision was already taken
	// to not sample the data.
	NotSampled
	// Dropped is used when data needs to be purged before the sampling policy
	// had a chance to evaluate it.
	Dropped
)

type Filter interface {
	Name() string
	ApplyForTrace(traceID pdata.TraceID, trace pdata.Traces) (Decision, error)
	ApplyForLog(traceID pdata.TraceID, log pdata.Logs) (Decision, error)
}

func NewFilter(logger *zap.Logger, cfg *FilterCfgs) (Filter, error) {
	switch cfg.Type {
	case NumericAttribute:
		return NewNumericAttributeFilter(logger, cfg.NumericAttributeCfg.Key, cfg.NumericAttributeCfg.MinValue, cfg.NumericAttributeCfg.MaxValue), nil
	case StringAttribute:
		return NewStringAttributeFilter(logger, cfg.StringAttributeCfg.Key, cfg.StringAttributeCfg.Values), nil
	case Property:
		return NewPropertyFilter(logger), nil
	case Percentage:
		return NewPercentageSample(logger, cfg.PercentageCfg.SamplingPercentage), nil
	default:
		return nil, fmt.Errorf("unknown sampling filter %s", cfg.Type)
	}
}
