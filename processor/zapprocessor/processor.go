// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zapprocessor

import (
	"context"

	"strconv"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
)

type processorImp struct {
	nextConsumer consumer.Logs
}

// newLogProcessor returns a processor that modifies log record.
func newLogProcessor() *processorImp {
	return &processorImp{}
}

func (e *processorImp) ProcessLogs(_ context.Context, ld pdata.Logs) (pdata.Logs, error) {
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rs := rls.At(i)
		ilss := rs.InstrumentationLibraryLogs()
		//resource := rs.Resource()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			logs := ils.Logs()
			//library := ils.InstrumentationLibrary()
			for k := 0; k < logs.Len(); k++ {
				convert(logs.At(j))
			}
		}
	}

	return ld, nil
}

func convert(lr pdata.LogRecord) {
	var body pdata.AttributeValue
	attr := lr.Attributes()
	bodyAttrs := lr.Body().MapVal()

	bodyAttrs.Range(func(k string, v pdata.AttributeValue) bool {
		switch k {
		case "ts":
			ts, _ := strconv.ParseInt(v.StringVal(), 10, 64)
			lr.SetTimestamp(pdata.Timestamp(ts))
		case "level":
			lvl, num := mapLevel(v.StringVal())
			lr.SetSeverityText(lvl)
			lr.SetSeverityNumber(num)
		case "msg":
			body = v
		case "caller":
			attr.Insert(k, v) // TODO: decide key
		default:
			attr.Insert(k, v)
		}

		return true
	})

	// TODO: Move to datadog log exporter later.
	{
		ddx := pdata.NewAttributeValueMap()
		dd := ddx.MapVal()
		dd.Insert("span_id", pdata.NewAttributeValueString(convertToDDTraceID(lr.SpanID().HexString())))
		dd.Insert("trace_id", pdata.NewAttributeValueString(convertToDDTraceID(lr.TraceID().HexString())))

		attr.Insert("dd", ddx)
	}

	body.CopyTo(lr.Body())
}

func mapLevel(lvl string) (string, pdata.SeverityNumber) {
	switch lvl {
	case "debug":
		return "DEBUG", pdata.SeverityNumberDEBUG
	case "info":
		return "INFO", pdata.SeverityNumberINFO
	case "warn":
		return "WARN", pdata.SeverityNumberWARN
	case "error":
		return "ERROR", pdata.SeverityNumberERROR
	case "fatal":
		return "FATAL", pdata.SeverityNumberFATAL
	}

	return "INFO", pdata.SeverityNumberINFO
}

func convertToDDTraceID(id string) string {
	if len(id) < 16 {
		return ""
	}
	if len(id) > 16 {
		id = id[16:]
	}
	intValue, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return ""
	}
	return strconv.FormatUint(intValue, 10)
}
