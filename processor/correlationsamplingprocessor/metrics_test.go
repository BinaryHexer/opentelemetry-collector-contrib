package correlationsamplingprocessor

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestProcessorMetrics(t *testing.T) {
	expectedViewNames := []string{
		"processor/correlation_sampling/conf_num_traces",
		"processor/correlation_sampling/num_traces_in_memory",
		"processor/correlation_sampling/num_sampling_decision_in_memory",
		"processor/correlation_sampling/traces_evicted",
		"processor/correlation_sampling/spans_released",
		"processor/correlation_sampling/logs_released",
		"processor/correlation_sampling/traces_released",
	}

	views := MetricViews()
	for i, viewName := range expectedViewNames {
		assert.Equal(t, viewName, views[i].Name)
	}
}