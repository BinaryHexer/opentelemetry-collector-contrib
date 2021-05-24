package sampling

import (
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

func TestNumericTagTraceFilter(t *testing.T) {
	var empty = map[string]pdata.AttributeValue{}
	filter := NewNumericAttributeFilter(zap.NewNop(), "example", math.MinInt32, math.MaxInt32)

	resAttr := map[string]pdata.AttributeValue{}
	resAttr["example"] = pdata.NewAttributeValueInt(8)

	cases := []struct {
		Desc     string
		Trace    pdata.Traces
		Decision Decision
	}{
		{
			Desc:     "nonmatching span attribute",
			Trace:    newTraceIntAttrs(empty, "non_matching", math.MinInt32),
			Decision: NotSampled,
		},
		{
			Desc:     "span attribute with lower limit",
			Trace:    newTraceIntAttrs(empty, "example", math.MinInt32),
			Decision: Sampled,
		},
		{
			Desc:     "span attribute with upper limit",
			Trace:    newTraceIntAttrs(empty, "example", math.MaxInt32),
			Decision: Sampled,
		},
		{
			Desc:     "span attribute below min limit",
			Trace:    newTraceIntAttrs(empty, "example", math.MinInt32-1),
			Decision: NotSampled,
		},
		{
			Desc:     "span attribute above max limit",
			Trace:    newTraceIntAttrs(empty, "example", math.MaxInt32+1),
			Decision: NotSampled,
		},
	}

	for _, c := range cases {
		t.Run(c.Desc, func(t *testing.T) {
			u, _ := uuid.NewRandom()
			decision, err := filter.ApplyForTrace(pdata.NewTraceID(u), c.Trace)
			assert.NoError(t, err)
			assert.Equal(t, decision, c.Decision)
		})
	}
}

func TestNumericTagLogFilter(t *testing.T) {
	var empty = map[string]pdata.AttributeValue{}
	filter := NewNumericAttributeFilter(zap.NewNop(), "example", math.MinInt32, math.MaxInt32)

	resAttr := map[string]pdata.AttributeValue{}
	resAttr["example"] = pdata.NewAttributeValueInt(8)

	cases := []struct {
		Desc     string
		Log      pdata.Logs
		Decision Decision
	}{
		{
			Desc:     "nonmatching span attribute",
			Log:      newLogIntAttrs(empty, "non_matching", math.MinInt32),
			Decision: NotSampled,
		},
		{
			Desc:     "span attribute with lower limit",
			Log:      newLogIntAttrs(empty, "example", math.MinInt32),
			Decision: Sampled,
		},
		{
			Desc:     "span attribute with upper limit",
			Log:      newLogIntAttrs(empty, "example", math.MaxInt32),
			Decision: Sampled,
		},
		{
			Desc:     "span attribute below min limit",
			Log:      newLogIntAttrs(empty, "example", math.MinInt32-1),
			Decision: NotSampled,
		},
		{
			Desc:     "span attribute above max limit",
			Log:      newLogIntAttrs(empty, "example", math.MaxInt32+1),
			Decision: NotSampled,
		},
	}

	for _, c := range cases {
		t.Run(c.Desc, func(t *testing.T) {
			u, _ := uuid.NewRandom()
			decision, err := filter.ApplyForLog(pdata.NewTraceID(u), c.Log)
			assert.NoError(t, err)
			assert.Equal(t, decision, c.Decision)
		})
	}
}

func newTraceIntAttrs(nodeAttrs map[string]pdata.AttributeValue, spanAttrKey string, spanAttrValue int64) pdata.Traces {
	traces := pdata.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().InitFromMap(nodeAttrs)
	ils := rs.InstrumentationLibrarySpans().AppendEmpty()
	span := ils.Spans().AppendEmpty()
	span.SetTraceID(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	attributes := make(map[string]pdata.AttributeValue)
	attributes[spanAttrKey] = pdata.NewAttributeValueInt(spanAttrValue)
	span.Attributes().InitFromMap(attributes)

	return traces
}

func newLogIntAttrs(nodeAttrs map[string]pdata.AttributeValue, spanAttrKey string, spanAttrValue int64) pdata.Logs {
	logs := pdata.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().InitFromMap(nodeAttrs)
	ill := rl.InstrumentationLibraryLogs().AppendEmpty()
	log := ill.Logs().AppendEmpty()
	log.SetTraceID(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	log.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	attributes := make(map[string]pdata.AttributeValue)
	attributes[spanAttrKey] = pdata.NewAttributeValueInt(spanAttrValue)
	log.Attributes().InitFromMap(attributes)

	return logs
}
