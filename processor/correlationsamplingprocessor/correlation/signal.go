package correlation

import (
	"go.opentelemetry.io/collector/consumer/pdata"
)

type SignalType int32

const (
	Unknown SignalType = iota
	Trace
	Log
)

type Signal struct {
	TraceID pdata.TraceID
	Data    interface{}
}
