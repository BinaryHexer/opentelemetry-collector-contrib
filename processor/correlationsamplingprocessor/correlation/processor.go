package correlation

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/pdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
)

type combinatorHook func(batches []*Signal) (batch interface{})

type decisionHook func(sampling.Filter, pdata.TraceID, interface{}) (sampling.Decision, error)

type samplingHook func(interface{})

type Processor interface {
	component.Processor
	ConsumeSignal(Signal) error
}
