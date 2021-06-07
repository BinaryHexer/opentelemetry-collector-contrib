package correlationsamplingprocessor

import (
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opentelemetry.io/collector/obsreport"
)

var (
	mNumTracesConf               = stats.Int64("conf_num_traces", "Maximum number of traces to hold in the internal storage", stats.UnitDimensionless)
	mNumTracesInMemory           = stats.Int64("num_traces_in_memory", "Number of traces currently in the in-memory storage", stats.UnitDimensionless)
	mNumSamplingDecisionInMemory = stats.Int64("num_sampling_decision_in_memory", "Number of sampling decisions currently in the in-memory storage", stats.UnitDimensionless)
	mTracesEvicted               = stats.Int64("traces_evicted", "Traces evicted from the internal buffer", stats.UnitDimensionless)
	mReleasedSpans               = stats.Int64("spans_released", "Spans released to the next consumer", stats.UnitDimensionless)
	mReleasedLogs                = stats.Int64("logs_released", "Logs released to the next consumer", stats.UnitDimensionless)
	mReleasedTraces              = stats.Int64("traces_released", "Traces released to the next consumer", stats.UnitDimensionless)
)

// MetricViews return the metrics views according to given telemetry level.
func MetricViews() []*view.View {
	legacyViews := []*view.View{
		{
			Name:        mNumTracesConf.Name(),
			Measure:     mNumTracesConf,
			Description: mNumTracesConf.Description(),
			Aggregation: view.LastValue(),
		},
		{
			Name:        mNumTracesInMemory.Name(),
			Measure:     mNumTracesInMemory,
			Description: mNumTracesInMemory.Description(),
			Aggregation: view.LastValue(),
		},
		{
			Name:        mNumSamplingDecisionInMemory.Name(),
			Measure:     mNumSamplingDecisionInMemory,
			Description: mNumSamplingDecisionInMemory.Description(),
			Aggregation: view.LastValue(),
		},
		{
			Name:        mTracesEvicted.Name(),
			Measure:     mTracesEvicted,
			Description: mTracesEvicted.Description(),
			// sum allows us to start from 0, count will only show up if there's at least one eviction, which might take a while to happen (if ever!)
			Aggregation: view.Sum(),
		},
		{
			Name:        mReleasedSpans.Name(),
			Measure:     mReleasedSpans,
			Description: mReleasedSpans.Description(),
			Aggregation: view.Sum(),
		},
		{
			Name:        mReleasedLogs.Name(),
			Measure:     mReleasedLogs,
			Description: mReleasedLogs.Description(),
			Aggregation: view.Sum(),
		},
		{
			Name:        mReleasedTraces.Name(),
			Measure:     mReleasedTraces,
			Description: mReleasedTraces.Description(),
			Aggregation: view.Sum(),
		},
	}

	return obsreport.ProcessorMetricViews(typeStr, legacyViews)
}
