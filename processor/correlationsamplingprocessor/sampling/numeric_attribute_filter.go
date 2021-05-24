package sampling

import (
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

type numericAttributeFilter struct {
	logger             *zap.Logger
	key                string
	minValue, maxValue int64
}

var _ Filter = (*numericAttributeFilter)(nil)

// NewNumericAttributeFilter creates a filter that samples all traces with the given attribute in the given numeric range.
func NewNumericAttributeFilter(logger *zap.Logger, key string, minValue, maxValue int64) *numericAttributeFilter {
	return &numericAttributeFilter{
		logger:   logger,
		key:      key,
		minValue: minValue,
		maxValue: maxValue,
	}
}

func (naf *numericAttributeFilter) Name() string {
	return "numeric_attribute_filter"
}

func (naf *numericAttributeFilter) ApplyForTrace(traceID pdata.TraceID, trace pdata.Traces) (Decision, error) {
	rspans := trace.ResourceSpans()
	for i := 0; i < rspans.Len(); i++ {
		rs := rspans.At(i)
		ilss := rs.InstrumentationLibrarySpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			for k := 0; k < ils.Spans().Len(); k++ {
				span := ils.Spans().At(k)
				if v, ok := span.Attributes().Get(naf.key); ok {
					value := v.IntVal()
					if value >= naf.minValue && value <= naf.maxValue {
						return Sampled, nil
					}
				}
			}
		}
	}

	return NotSampled, nil
}

func (naf *numericAttributeFilter) ApplyForLog(traceID pdata.TraceID, log pdata.Logs) (Decision, error) {
	rlogs := log.ResourceLogs()
	for i := 0; i < rlogs.Len(); i++ {
		rl := rlogs.At(i)
		illl := rl.InstrumentationLibraryLogs()
		for j := 0; j < illl.Len(); j++ {
			ill := illl.At(j)
			for k := 0; k < ill.Logs().Len(); k++ {
				log := ill.Logs().At(k)
				if v, ok := log.Attributes().Get(naf.key); ok {
					value := v.IntVal()
					if value >= naf.minValue && value <= naf.maxValue {
						return Sampled, nil
					}
				}
			}
		}
	}

	return NotSampled, nil
}
