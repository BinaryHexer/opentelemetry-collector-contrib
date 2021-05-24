package sampling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

func TestStringTagTraceFilter(t *testing.T) {
	var empty = map[string]pdata.AttributeValue{}
	filter := NewStringAttributeFilter(zap.NewNop(), "example", []string{"value"})

	cases := []struct {
		Desc     string
		Trace    pdata.Traces
		Decision Decision
	}{
		{
			Desc:     "nonmatching node attribute key",
			Trace:    newTraceStringAttrs(map[string]pdata.AttributeValue{"non_matching": pdata.NewAttributeValueString("value")}, "", ""),
			Decision: NotSampled,
		},
		{
			Desc:     "nonmatching node attribute value",
			Trace:    newTraceStringAttrs(map[string]pdata.AttributeValue{"example": pdata.NewAttributeValueString("non_matching")}, "", ""),
			Decision: NotSampled,
		},
		{
			Desc:     "matching node attribute",
			Trace:    newTraceStringAttrs(map[string]pdata.AttributeValue{"example": pdata.NewAttributeValueString("value")}, "", ""),
			Decision: Sampled,
		},
		{
			Desc:     "nonmatching span attribute key",
			Trace:    newTraceStringAttrs(empty, "nonmatching", "value"),
			Decision: NotSampled,
		},
		{
			Desc:     "nonmatching span attribute value",
			Trace:    newTraceStringAttrs(empty, "example", "nonmatching"),
			Decision: NotSampled,
		},
		{
			Desc:     "matching span attribute",
			Trace:    newTraceStringAttrs(empty, "example", "value"),
			Decision: Sampled,
		},
	}

	for _, c := range cases {
		t.Run(c.Desc, func(t *testing.T) {
			decision, err := filter.ApplyForTrace(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), c.Trace)
			assert.NoError(t, err)
			assert.Equal(t, decision, c.Decision)
		})
	}
}

func TestStringTagLogFilter(t *testing.T) {
	var empty = map[string]pdata.AttributeValue{}
	filter := NewStringAttributeFilter(zap.NewNop(), "example", []string{"value"})

	cases := []struct {
		Desc     string
		Log      pdata.Logs
		Decision Decision
	}{
		{
			Desc:     "nonmatching node attribute key",
			Log:      newLogStringAttrs(map[string]pdata.AttributeValue{"non_matching": pdata.NewAttributeValueString("value")}, "", ""),
			Decision: NotSampled,
		},
		{
			Desc:     "nonmatching node attribute value",
			Log:      newLogStringAttrs(map[string]pdata.AttributeValue{"example": pdata.NewAttributeValueString("non_matching")}, "", ""),
			Decision: NotSampled,
		},
		{
			Desc:     "matching node attribute",
			Log:      newLogStringAttrs(map[string]pdata.AttributeValue{"example": pdata.NewAttributeValueString("value")}, "", ""),
			Decision: Sampled,
		},
		{
			Desc:     "nonmatching span attribute key",
			Log:      newLogStringAttrs(empty, "nonmatching", "value"),
			Decision: NotSampled,
		},
		{
			Desc:     "nonmatching span attribute value",
			Log:      newLogStringAttrs(empty, "example", "nonmatching"),
			Decision: NotSampled,
		},
		{
			Desc:     "matching span attribute",
			Log:      newLogStringAttrs(empty, "example", "value"),
			Decision: Sampled,
		},
	}

	for _, c := range cases {
		t.Run(c.Desc, func(t *testing.T) {
			decision, err := filter.ApplyForLog(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}), c.Log)
			assert.NoError(t, err)
			assert.Equal(t, decision, c.Decision)
		})
	}
}

func newTraceStringAttrs(nodeAttrs map[string]pdata.AttributeValue, spanAttrKey string, spanAttrValue string) pdata.Traces {
	traces := pdata.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().InitFromMap(nodeAttrs)
	ils := rs.InstrumentationLibrarySpans().AppendEmpty()
	span := ils.Spans().AppendEmpty()
	span.SetTraceID(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	attributes := make(map[string]pdata.AttributeValue)
	attributes[spanAttrKey] = pdata.NewAttributeValueString(spanAttrValue)
	span.Attributes().InitFromMap(attributes)

	return traces
}

func newLogStringAttrs(nodeAttrs map[string]pdata.AttributeValue, spanAttrKey string, spanAttrValue string) pdata.Logs {
	logs := pdata.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().InitFromMap(nodeAttrs)
	ill := rl.InstrumentationLibraryLogs().AppendEmpty()
	log := ill.Logs().AppendEmpty()
	log.SetTraceID(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	log.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	attributes := make(map[string]pdata.AttributeValue)
	attributes[spanAttrKey] = pdata.NewAttributeValueString(spanAttrValue)
	log.Attributes().InitFromMap(attributes)

	return logs
}
