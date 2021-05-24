package correlationsamplingprocessor

import (
	"go.opentelemetry.io/collector/component/componenttest"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/configtest"
	"path"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config"
	"time"
	"testing"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
)

func TestLoadConfig(t *testing.T) {
	factories, err := componenttest.NopFactories()
	assert.NoError(t, err)

	factory := NewFactory()
	factories.Processors[factory.Type()] = factory

	cfg, err := configtest.LoadConfigFile(t, path.Join(".", "testdata", "correlation_sampling_config.yaml"), factories)
	require.Nil(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, cfg.Processors["correlation_sampling"],
		&Config{
			ProcessorSettings: config.NewProcessorSettings(typeStr),
			ID:                "test",
			DecisionWait:      10 * time.Second,
			NumTraces:         100,
			FilterCfgs: []sampling.FilterCfgs{
				{
					Name:          "test-policy-1",
					Type:          sampling.Percentage,
					PercentageCfg: sampling.PercentageCfg{SamplingPercentage: 100},
				},
				{
					Name:                "test-policy-2",
					Type:                sampling.NumericAttribute,
					NumericAttributeCfg: sampling.NumericAttributeCfg{Key: "key1", MinValue: 50, MaxValue: 100},
				},
				{
					Name:               "test-policy-3",
					Type:               sampling.StringAttribute,
					StringAttributeCfg: sampling.StringAttributeCfg{Key: "key2", Values: []string{"value1", "value2"}},
				},
			},
		})
}
