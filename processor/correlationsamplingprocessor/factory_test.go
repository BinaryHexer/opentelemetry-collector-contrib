package correlationsamplingprocessor

import (
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/config/configcheck"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"testing"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlationsamplingprocessor/sampling"
	"context"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg, "failed to create default config")
	assert.NoError(t, configcheck.ValidateConfig(cfg))
}

func TestCreateProcessor(t *testing.T) {
	factory := NewFactory()

	cfg := factory.CreateDefaultConfig().(*Config)
	// Manually set required fields
	cfg.CorrelationID = "test"
	cfg.FilterCfgs = []sampling.FilterCfgs{
		{
			Name:          "test-policy",
			Type:          sampling.Percentage,
			PercentageCfg: sampling.PercentageCfg{SamplingPercentage: 100},
		},
	}

	params := component.ProcessorCreateParams{Logger: zap.NewNop()}

	tp, err := factory.CreateTracesProcessor(context.Background(), params, cfg, consumertest.NewNop())
	assert.NotNil(t, tp)
	assert.NoError(t, err, "cannot create trace processor")

	lp, err := factory.CreateLogsProcessor(context.Background(), params, cfg, consumertest.NewNop())
	assert.NotNil(t, lp)
	assert.NoError(t, err, "cannot create log processor")
}
