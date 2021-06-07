package correlation

import (
	"time"

	"go.opentelemetry.io/collector/config"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
)

// Config defines configuration for the Correlation sampling processor.
type Config struct {
	config.ProcessorSettings `mapstructure:"-"`
	// ID is used for correlation sampling between traces and logs.
	CorrelationID string `mapstructure:"correlation_id"`
	// DecisionWait is the desired wait time from the arrival of the first span of
	// trace until the decision about sampling it or not is evaluated.
	DecisionWait time.Duration `mapstructure:"decision_wait"`
	// NumTraces is the number of traces kept on memory. Typically most of the data
	// of a trace is released after a sampling decision is taken.
	NumTraces uint64 `mapstructure:"num_traces"`
	// FilterCfgs sets the tail-based sampling policy which makes a sampling decision
	// for a given trace when requested.
	FilterCfgs []sampling.FilterCfgs `mapstructure:"filters"`
}
