package correlatedsamplingprocessor

import (
	"time"

	"go.opentelemetry.io/collector/config"
)

// FilterType indicates the type of filter.
type FilterType string

const (
	// NumericAttribute sample traces that have a given numeric attribute in a specified
	// range, e.g.: attribute "http.status_code" >= 399 and <= 999.
	NumericAttribute FilterType = "numeric_attribute"
	// StringAttribute sample traces that a attribute, of type string, matching
	// one of the listed values.
	StringAttribute FilterType = "string_attribute"
	// PercentageSample samples by the given percentage.
	PercentageSample FilterType = "percentage_sample"
)

// FilterCfgs holds the common configuration to all filters.
type FilterCfgs struct {
	// Name given to the instance of the policy to make easy to identify it in metrics and logs.
	Name string `mapstructure:"name"`
	// Type of the policy this will be used to match the proper configuration of the policy.
	Type FilterType `mapstructure:"type"`
	// Configs for numeric attribute filter sampling policy evaluator.
	NumericAttributeCfg NumericAttributeCfg `mapstructure:"numeric_attribute"`
	// Configs for string attribute filter sampling policy evaluator.
	StringAttributeCfg StringAttributeCfg `mapstructure:"string_attribute"`
	// Configs for percentage sample filter sampling policy evaluator.
	PercentageSamplingCfg PercentageSamplingCfg `mapstructure:"percentage_sample"`
}

// NumericAttributeCfg holds the configurable settings to create a numeric attribute filter
// sampling policy evaluator.
type NumericAttributeCfg struct {
	// Tag that the filter is going to be matching against.
	Key string `mapstructure:"key"`
	// MinValue is the minimum value of the attribute to be considered a match.
	MinValue int64 `mapstructure:"min_value"`
	// MaxValue is the maximum value of the attribute to be considered a match.
	MaxValue int64 `mapstructure:"max_value"`
}

// StringAttributeCfg holds the configurable settings to create a string attribute filter
// sampling policy evaluator.
type StringAttributeCfg struct {
	// Tag that the filter is going to be matching against.
	Key string `mapstructure:"key"`
	// Values is the set of attribute values that if any is equal to the actual attribute value to be considered a match.
	Values []string `mapstructure:"values"`
}

// PercentageSamplingCfg holds the configurable settings to create a percentage sampling policy evaluator.
type PercentageSamplingCfg struct {
	// SamplingPercentage is the percentage rate at which signals are going to be sampled. Defaults to zero, i.e.: no sample.
	// Values greater or equal 100 are treated as "sample all signals".
	SamplingPercentage int64 `mapstructure:"sampling_percentage"`
}

// Config defines configuration for the Routing processor.
type Config struct {
	*config.ProcessorSettings `mapstructure:"-"`
	// ID is used for correlation sampling between traces and logs.
	ID string `mapstructure:"id"`
	// DecisionWait is the desired wait time from the arrival of the first span of
	// trace until the decision about sampling it or not is evaluated.
	DecisionWait time.Duration `mapstructure:"decision_wait"`
	// NumTraces is the number of traces kept on memory. Typically most of the data
	// of a trace is released after a sampling decision is taken.
	NumTraces uint64 `mapstructure:"num_traces"`
	// FilterCfgs sets the tail-based sampling policy which makes a sampling decision
	// for a given trace when requested.
	FilterCfgs []FilterCfgs `mapstructure:"filters"`
}
