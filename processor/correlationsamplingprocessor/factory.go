package correlationsamplingprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.opencensus.io/stats/view"
)

const (
	// The value of "type" Correlation Sampling in configuration.
	typeStr = "correlation_sampling"
)

// NewFactory returns a new factory for the Correlation Sampling processor.
func NewFactory() component.ProcessorFactory {
	// TODO: find a more appropriate way to get this done, as we are swallowing the error here
	_ = view.Register(MetricViews()...)

	return processorhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		processorhelper.WithTraces(createTracesProcessor),
		processorhelper.WithLogs(createLogsProcessor),
	)
}

func createDefaultConfig() config.Processor {
	return &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewID(typeStr)),
		DecisionWait:      time.Second,
		NumTraces:         50000,
	}
}

func createTracesProcessor(
	ctx context.Context,
	params component.ProcessorCreateParams,
	cfg config.Processor,
	nextConsumer consumer.Traces,
) (component.TracesProcessor, error) {
	tCfg := cfg.(*Config)
	return newTracesProcessor(ctx, params.Logger, nextConsumer, *tCfg)
}

func createLogsProcessor(
	ctx context.Context,
	params component.ProcessorCreateParams,
	cfg config.Processor,
	nextConsumer consumer.Logs,
) (component.LogsProcessor, error) {
	tCfg := cfg.(*Config)
	return newLogsProcessor(ctx, params.Logger, nextConsumer, *tCfg)
}
