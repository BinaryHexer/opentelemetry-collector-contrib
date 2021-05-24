package sampling

// FilterType indicates the type of filter.
type FilterType string

const (
	// NumericAttribute sample signals that have a given numeric attribute in a specified
	// range, e.g.: attribute "http.status_code" >= 399 and <= 999.
	NumericAttribute FilterType = "numeric_attribute"
	// StringAttribute sample signals that a attribute, of type string, matching
	// one of the listed values.
	StringAttribute FilterType = "string_attribute"
	// Property samples signals by the given properties.
	Property FilterType = "property"
	// Percentage samples signals by the given percentage.
	Percentage FilterType = "percentage"
)

// FilterCfgs holds the common configuration to all filters.
type FilterCfgs struct {
	// Name given to the instance of the filter to make easy to identify it in metrics and logs.
	Name string `mapstructure:"name"`
	// Type of the filter this will be used to match the proper configuration of the filter.
	Type FilterType `mapstructure:"type"`
	// Configs for numeric attribute filter.
	NumericAttributeCfg NumericAttributeCfg `mapstructure:"numeric_attribute"`
	// Configs for string attribute filter.
	StringAttributeCfg StringAttributeCfg `mapstructure:"string_attribute"`
	// Configs for property filter.
	PropertyCfg PropertyCfg `mapstructure:"property"`
	// Configs for percentage filter.
	PercentageCfg PercentageCfg `mapstructure:"percentage"`
}

// NumericAttributeCfg holds the configurable settings to create a numeric attribute filter.
type NumericAttributeCfg struct {
	// Tag that the filter is going to be matching against.
	Key string `mapstructure:"key"`
	// MinValue is the minimum value of the attribute to be considered a match.
	MinValue int64 `mapstructure:"min_value"`
	// MaxValue is the maximum value of the attribute to be considered a match.
	MaxValue int64 `mapstructure:"max_value"`
}

// StringAttributeCfg holds the configurable settings to create a string attribute filter.
type StringAttributeCfg struct {
	// Tag that the filter is going to be matching against.
	Key string `mapstructure:"key"`
	// Values is the set of attribute values that if any is equal to the actual attribute value to be considered a match.
	Values []string `mapstructure:"values"`
}

// PropertyCfg holds the configurable settings to create a property filter.
type PropertyCfg struct {
	// TODO
}

// PercentageCfg holds the configurable settings to create a percentage filter.
type PercentageCfg struct {
	// SamplingPercentage is the percentage rate at which signals are going to be sampled. Defaults to zero, i.e.: no sample.
	// Values greater or equal 100 are treated as "sample all signals".
	SamplingPercentage int64 `mapstructure:"sampling_percentage"`
}
