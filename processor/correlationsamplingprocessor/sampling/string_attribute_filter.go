package sampling

import (
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

type stringAttributeFilter struct {
	logger *zap.Logger
	key    string
	values map[string]struct{}
}

var _ Filter = (*stringAttributeFilter)(nil)

// NewStringAttributeFilter creates a filter that samples all traces with the given attribute in the given string values.
func NewStringAttributeFilter(logger *zap.Logger, key string, values []string) *stringAttributeFilter {
	valuesMap := make(map[string]struct{})
	for _, value := range values {
		if value != "" {
			valuesMap[value] = struct{}{}
		}
	}

	return &stringAttributeFilter{
		logger: logger,
		key:    key,
		values: valuesMap,
	}
}

func (saf *stringAttributeFilter) Name() string {
	return "numeric_attribute_filter"
}

func (saf *stringAttributeFilter) ApplyForTrace(traceID pdata.TraceID, trace pdata.Traces) (Decision, error) {
	rspans := trace.ResourceSpans()
	for i := 0; i < rspans.Len(); i++ {
		rs := rspans.At(i)
		resource := rs.Resource()
		if v, ok := resource.Attributes().Get(saf.key); ok {
			if _, ok := saf.values[v.StringVal()]; ok {
				return Sampled, nil
			}
		}

		ilss := rs.InstrumentationLibrarySpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			for k := 0; k < ils.Spans().Len(); k++ {
				span := ils.Spans().At(k)
				if v, ok := span.Attributes().Get(saf.key); ok {
					if _, ok := saf.values[v.StringVal()]; ok {
						return Sampled, nil
					}
				}
			}
		}
	}

	return NotSampled, nil
}

func (saf *stringAttributeFilter) ApplyForLog(traceID pdata.TraceID, log pdata.Logs) (Decision, error) {
	rlogs := log.ResourceLogs()
	for i := 0; i < rlogs.Len(); i++ {
		rl := rlogs.At(i)
		resource := rl.Resource()
		if v, ok := resource.Attributes().Get(saf.key); ok {
			if _, ok := saf.values[v.StringVal()]; ok {
				return Sampled, nil
			}
		}

		illl := rl.InstrumentationLibraryLogs()
		for j := 0; j < illl.Len(); j++ {
			ill := illl.At(j)
			for k := 0; k < ill.Logs().Len(); k++ {
				log := ill.Logs().At(k)
				if v, ok := log.Attributes().Get(saf.key); ok {
					if _, ok := saf.values[v.StringVal()]; ok {
						return Sampled, nil
					}
				}
			}
		}
	}

	return NotSampled, nil
}
