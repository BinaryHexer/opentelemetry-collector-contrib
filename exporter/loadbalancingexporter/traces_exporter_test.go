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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configtest"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.uber.org/zap"
	"math/rand"
	"path"
	"testing"
)

func TestTraceExporterStart(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newTracesExporter(params, config)
	require.NotNil(t, p)
	require.NoError(t, err)
	p.res = &mockResolver{}

	// test
	res := p.Start(context.Background(), componenttest.NewNopHost())
	defer p.Shutdown(context.Background())

	// verify
	assert.Nil(t, res)
}

func TestTraceExporterShutdown(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newTracesExporter(params, config)
	require.NotNil(t, p)
	require.NoError(t, err)

	// test
	res := p.Shutdown(context.Background())

	// verify
	assert.Nil(t, res)
}

func TestConsumeTraces(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newTracesExporter(params, config)
	require.NotNil(t, p)
	require.NoError(t, err)

	// pre-load an exporter here, so that we don't use the actual OTLP exporter
	p.exporters["endpoint-1"] = &componenttest.ExampleExporterConsumer{}
	p.res = &mockResolver{
		triggerCallbacks: true,
		onResolve: func(ctx context.Context) ([]string, error) {
			return []string{"endpoint-1"}, nil
		},
	}

	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	defer p.Shutdown(context.Background())

	// test
	res := p.ConsumeTraces(context.Background(), simpleTraces())

	// verify
	assert.Nil(t, res)
}

func TestBuildExporterConfig(t *testing.T) {
	// prepare
	factories, err := componenttest.ExampleComponents()
	require.NoError(t, err)

	factories.Exporters[typeStr] = NewFactory()

	cfg, err := configtest.LoadConfigFile(t, path.Join(".", "testdata", "test-build-exporter-config.yaml"), factories)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.Exporters["loadbalancing"])

	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	e, err := newTracesExporter(params, cfg.Exporters["loadbalancing"])
	require.NotNil(t, e)
	require.NoError(t, err)

	// test
	defaultCfg := e.exporterFactory.CreateDefaultConfig().(*otlpexporter.Config)
	exporterCfg := e.buildExporterConfig("the-endpoint")

	// verify
	grpcSettings := defaultCfg.GRPCClientSettings
	grpcSettings.Endpoint = "the-endpoint"
	assert.Equal(t, grpcSettings, exporterCfg.GRPCClientSettings)

	assert.Equal(t, defaultCfg.ExporterSettings, exporterCfg.ExporterSettings)
	assert.Equal(t, defaultCfg.TimeoutSettings, exporterCfg.TimeoutSettings)
	assert.Equal(t, defaultCfg.QueueSettings, exporterCfg.QueueSettings)
	assert.Equal(t, defaultCfg.RetrySettings, exporterCfg.RetrySettings)
}

func TestNoTracesInBatch(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		batch pdata.Traces
	}{
		{
			"no resource spans",
			pdata.NewTraces(),
		},
		{
			"no instrumentation library spans",
			func() pdata.Traces {
				batch := pdata.NewTraces()
				rs := pdata.NewResourceSpans()
				batch.ResourceSpans().Append(rs)
				return batch
			}(),
		},
		{
			"no spans",
			func() pdata.Traces {
				batch := pdata.NewTraces()
				rs := pdata.NewResourceSpans()
				ils := pdata.NewInstrumentationLibrarySpans()
				rs.InstrumentationLibrarySpans().Append(ils)
				batch.ResourceSpans().Append(rs)
				return batch
			}(),
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			res := traceIDFromTraces(tt.batch)
			assert.Equal(t, pdata.InvalidTraceID(), res)
		})
	}
}

func TestBatchWithTwoTraces(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newTracesExporter(params, config)
	require.NotNil(t, p)
	require.NoError(t, err)

	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	sink := &componenttest.ExampleExporterConsumer{}
	p.loadBalancer.exporters["endpoint-1"] = sink

	first := simpleTraces()
	second := simpleTraceWithID(pdata.NewTraceID([16]byte{2, 3, 4, 5}))
	batch := pdata.NewTraces()
	batch.ResourceSpans().Append(first.ResourceSpans().At(0))
	batch.ResourceSpans().Append(second.ResourceSpans().At(0))

	// test
	err = p.ConsumeTraces(context.Background(), batch)

	// verify
	assert.NoError(t, err)
	assert.Len(t, sink.Traces, 2)
}

func randomTraces() pdata.Traces {
	v1 := uint8(rand.Intn(256))
	v2 := uint8(rand.Intn(256))
	v3 := uint8(rand.Intn(256))
	v4 := uint8(rand.Intn(256))
	return simpleTraceWithID(pdata.NewTraceID([16]byte{v1, v2, v3, v4}))
}

func simpleTraces() pdata.Traces {
	return simpleTraceWithID(pdata.NewTraceID([16]byte{1, 2, 3, 4}))
}

func simpleTraceWithID(id pdata.TraceID) pdata.Traces {
	traces := pdata.NewTraces()
	traces.ResourceSpans().Resize(1)
	rs := traces.ResourceSpans().At(0)
	rs.InstrumentationLibrarySpans().Resize(1)
	ils := rs.InstrumentationLibrarySpans().At(0)
	ils.Spans().Resize(1)
	ils.Spans().At(0).SetTraceID(id)

	return traces
}

func simpleConfig() *Config {
	return &Config{
		Resolver: ResolverSettings{
			Static: &StaticResolver{Hostnames: []string{"endpoint-1"}},
		},
	}
}

type mockTracesExporter struct {
	ConsumeTracesFn func(ctx context.Context, td pdata.Traces) error
	StartFn         func(ctx context.Context, host component.Host) error
	ShutdownFn      func(ctx context.Context) error
}

func (e *mockTracesExporter) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	if e.ConsumeTracesFn == nil {
		return nil
	}
	return e.ConsumeTracesFn(ctx, td)
}

func (e *mockTracesExporter) Start(ctx context.Context, host component.Host) error {
	if e.StartFn == nil {
		return nil
	}
	return e.StartFn(ctx, host)
}

func (e *mockTracesExporter) Shutdown(ctx context.Context) error {
	if e.ShutdownFn == nil {
		return nil
	}
	return e.ShutdownFn(ctx)
}
