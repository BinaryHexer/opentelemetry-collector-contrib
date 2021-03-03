// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loadbalancingexporter

import (
	"context"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/batchpertrace"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.uber.org/zap"
	"sync"
	"time"
)

var _ component.LogsExporter = (*logExporterImp)(nil)

type logExporterImp struct {
	logger *zap.Logger

	loadBalancer loadBalancer

	stopped    bool
	shutdownWg sync.WaitGroup
}

// Create new logs exporter
func newLogsExporter(params component.ExporterCreateParams, cfg configmodels.Exporter) (*logExporterImp, error) {
	exporterFactory := otlpexporter.NewFactory()

	tmplParams := component.ExporterCreateParams{
		Logger:               params.Logger,
		ApplicationStartInfo: params.ApplicationStartInfo,
	}

	loadBalancer, _ := newLoadBalancer(params, cfg, func(ctx context.Context, endpoint string) (component.Exporter, error) {
		oCfg := cfg.(*Config).Protocol.OTLP
		oCfg.Endpoint = endpoint
		return exporterFactory.CreateLogsExporter(ctx, tmplParams, &oCfg)
	})

	return &logExporterImp{
		logger:       params.Logger,
		loadBalancer: loadBalancer,
	}, nil
}

func (e *logExporterImp) Start(ctx context.Context, host component.Host) error {
	if err := e.loadBalancer.Start(ctx, host); err != nil {
		return err
	}

	return nil
}

func (e *logExporterImp) Shutdown(context.Context) error {
	e.stopped = true
	e.shutdownWg.Wait()
	return nil
}

func (e *logExporterImp) ConsumeLogs(ctx context.Context, ld pdata.Logs) error {
	var errors []error
	batches := batchpertrace.SplitLogs(ld)
	for _, batch := range batches {
		if err := e.consumeLog(ctx, batch); err != nil {
			errors = append(errors, err)
		}
	}

	return componenterror.CombineErrors(errors)
}

func (e *logExporterImp) consumeLog(ctx context.Context, ld pdata.Logs) error {
	traceID := traceIDFromLogs(ld)
	if traceID == pdata.InvalidTraceID() {
		return errNoTracesInBatch
	}

	exp, endpoint, err := e.loadBalancer.Exporter(traceID)
	if err != nil {
		return err
	}

	start := time.Now()
	err = exp.(component.LogsExporter).ConsumeLogs(ctx, ld)
	duration := time.Since(start)
	ctx, _ = tag.New(ctx, tag.Upsert(tag.MustNewKey("endpoint"), endpoint))

	if err == nil {
		sCtx, _ := tag.New(ctx, tag.Upsert(tag.MustNewKey("success"), "true"))
		stats.Record(sCtx, mBackendLatency.M(duration.Milliseconds()))
	} else {
		fCtx, _ := tag.New(ctx, tag.Upsert(tag.MustNewKey("success"), "false"))
		stats.Record(fCtx, mBackendLatency.M(duration.Milliseconds()))
	}

	return err
}

func traceIDFromLogs(ld pdata.Logs) pdata.TraceID {
	rl := ld.ResourceLogs()
	if rl.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	ill := rl.At(0).InstrumentationLibraryLogs()
	if ill.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	logs := ill.At(0).Logs()
	if logs.Len() == 0 {
		return pdata.InvalidTraceID()
	}

	return logs.At(0).TraceID()
}
