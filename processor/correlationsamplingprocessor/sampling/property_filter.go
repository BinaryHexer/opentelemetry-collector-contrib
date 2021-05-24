package sampling

import (
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

type propertyFilter struct {
	logger         *zap.Logger
	spanProperties spanProperties
	logProperties  logProperties
}

type spanProperties struct {
	name   string
	status pdata.SpanStatus
}

type logProperties struct {
	name         string
	body         string
	severityText string
}

var _ Filter = (*propertyFilter)(nil)

// NewPropertyFilter creates a filter that samples all traces with the given attribute in the given numeric range.
func NewPropertyFilter(logger *zap.Logger) *propertyFilter {
	return &propertyFilter{
		logger: logger,
	}
}

func (pf *propertyFilter) Name() string {
	return "numeric_attribute_filter"
}

func (pf *propertyFilter) ApplyForTrace(traceID pdata.TraceID, trace pdata.Traces) (Decision, error) {
	rspans := trace.ResourceSpans()
	for i := 0; i < rspans.Len(); i++ {
		rs := rspans.At(i)
		ilss := rs.InstrumentationLibrarySpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			for k := 0; k < ils.Spans().Len(); k++ {
				span := ils.Spans().At(k)
				name := span.Name()
				status := span.Status()

				if name == pf.spanProperties.name {
					return Sampled, nil
				}
				if status == pf.spanProperties.status {
					return Sampled, nil
				}
			}
		}
	}

	return NotSampled, nil
}

func (pf *propertyFilter) ApplyForLog(traceID pdata.TraceID, log pdata.Logs) (Decision, error) {
	rlogs := log.ResourceLogs()
	for i := 0; i < rlogs.Len(); i++ {
		rl := rlogs.At(i)
		illl := rl.InstrumentationLibraryLogs()
		for j := 0; j < illl.Len(); j++ {
			ill := illl.At(j)
			for k := 0; k < ill.Logs().Len(); k++ {
				log := ill.Logs().At(k)
				name := log.Name()
				//body := log.Body() // TODO
				severityText := log.SeverityText()

				if name == pf.logProperties.name {
					return Sampled, nil
				}
				//if body == pf.logProperties.body {
				//	return Sampled, nil
				//}
				if severityText == pf.logProperties.severityText {
					return Sampled, nil
				}
			}
		}
	}

	return NotSampled, nil
}
