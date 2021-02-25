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
	"fmt"
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
	config Config
	host   component.Host

	res  resolver
	ring *hashRing

	exporters            map[string]component.LogsExporter
	exporterFactory      component.ExporterFactory
	templateCreateParams component.ExporterCreateParams

	stopped    bool
	shutdownWg sync.WaitGroup
	updateLock sync.RWMutex
}

// Crete new logs exporter
func newLogsExporter(params component.ExporterCreateParams, cfg configmodels.Exporter) (*logExporterImp, error) {
	oCfg := cfg.(*Config)

	tmplParams := component.ExporterCreateParams{
		Logger:               params.Logger,
		ApplicationStartInfo: params.ApplicationStartInfo,
	}

	if oCfg.Resolver.DNS != nil && oCfg.Resolver.Static != nil {
		return nil, errMultipleResolversProvided
	}

	var res resolver
	if oCfg.Resolver.Static != nil {
		var err error
		res, err = newStaticResolver(oCfg.Resolver.Static.Hostnames)
		if err != nil {
			return nil, err
		}
	}
	if oCfg.Resolver.DNS != nil {
		dnsLogger := params.Logger.With(zap.String("resolver", "dns"))

		var err error
		res, err = newDNSResolver(dnsLogger, oCfg.Resolver.DNS.Hostname, oCfg.Resolver.DNS.Port)
		if err != nil {
			return nil, err
		}
	}

	if res == nil {
		return nil, errNoResolver
	}

	return &logExporterImp{
		logger: params.Logger,
		config: *oCfg,

		res: res,

		exporters:            map[string]component.LogsExporter{},
		exporterFactory:      otlpexporter.NewFactory(),
		templateCreateParams: tmplParams,
	}, nil
}

func (e *logExporterImp) Start(ctx context.Context, host component.Host) error {
	e.res.onChange(e.onBackendChanges)
	e.host = host
	if err := e.res.start(ctx); err != nil {
		return err
	}

	return nil
}

func (e *logExporterImp) onBackendChanges(resolved []string) {
	newRing := newHashRing(resolved)

	if !newRing.equal(e.ring) {
		e.updateLock.Lock()
		defer e.updateLock.Unlock()

		e.ring = newRing

		// TODO: set a timeout?
		ctx := context.Background()

		// add the missing exporters first
		e.addMissingExporters(ctx, resolved)
		e.removeExtraExporters(ctx, resolved)
	}
}

func (e *logExporterImp) addMissingExporters(ctx context.Context, endpoints []string) {
	for _, endpoint := range endpoints {
		endpoint = endpointWithPort(endpoint)

		if _, exists := e.exporters[endpoint]; !exists {
			cfg := e.buildExporterConfig(endpoint)
			exp, err := e.exporterFactory.CreateLogsExporter(ctx, e.templateCreateParams, &cfg)
			if err != nil {
				e.logger.Error("failed to create new log exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
				continue
			}
			if err = exp.Start(ctx, e.host); err != nil {
				e.logger.Error("failed to start new log exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
				continue
			}
			e.exporters[endpoint] = exp
		}
	}
}

func (e *logExporterImp) buildExporterConfig(endpoint string) otlpexporter.Config {
	oCfg := e.config.Protocol.OTLP
	oCfg.Endpoint = endpoint
	return oCfg
}

func (e *logExporterImp) removeExtraExporters(ctx context.Context, endpoints []string) {
	for existing := range e.exporters {
		if !endpointFound(existing, endpoints) {
			e.exporters[existing].Shutdown(ctx)
			delete(e.exporters, existing)
		}
	}
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

	// NOTE: make rolling updates of next tier of collectors work. currently this may cause
	// data loss because the latest batches sent to outdated backend will never find their way out.
	// for details: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/1690
	e.updateLock.RLock()
	endpoint := e.ring.endpointFor(traceID)
	exp, found := e.exporters[endpoint]
	e.updateLock.RUnlock()
	if !found {
		// something is really wrong... how come we couldn't find the exporter??
		return fmt.Errorf("couldn't find the exporter for the endpoint %q", endpoint)
	}

	start := time.Now()
	err := exp.ConsumeLogs(ctx, ld)
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

func (e *logExporterImp) GetCapabilities() component.ProcessorCapabilities {
	return component.ProcessorCapabilities{MutatesConsumedData: false}
}
