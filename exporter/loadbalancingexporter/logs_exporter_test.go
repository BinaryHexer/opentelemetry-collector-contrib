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
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"math/rand"
	"testing"
)

func TestLogExporterStart(t *testing.T) {
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

func TestLogExporterShutdown(t *testing.T) {
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

func TestConsumeLogs(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newLogsExporter(params, config)
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
	res := p.ConsumeLogs(context.Background(), simpleLogs())

	// verify
	assert.Nil(t, res)
}

func TestNoTracesInLogBatch(t *testing.T) {
	for _, tt := range []struct {
		desc  string
		batch pdata.Logs
	}{
		{
			"no resource logs",
			pdata.NewLogs(),
		},
		{
			"no instrumentation library logs",
			func() pdata.Logs {
				batch := pdata.NewLogs()
				rl := pdata.NewResourceLogs()
				batch.ResourceLogs().Append(rl)
				return batch
			}(),
		},
		{
			"no logs",
			func() pdata.Logs {
				batch := pdata.NewLogs()
				rl := pdata.NewResourceLogs()
				ill := pdata.NewInstrumentationLibraryLogs()
				rl.InstrumentationLibraryLogs().Append(ill)
				batch.ResourceLogs().Append(rl)
				return batch
			}(),
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			res := traceIDFromLogs(tt.batch)
			assert.Equal(t, pdata.InvalidTraceID(), res)
		})
	}
}

func TestLogBatchWithTwoTraces(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newLogsExporter(params, config)
	require.NotNil(t, p)
	require.NoError(t, err)

	err = p.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	sink := &componenttest.ExampleExporterConsumer{}
	p.exporters["endpoint-1"] = sink

	first := simpleLogs()
	second := simpleLogWithID(pdata.NewTraceID([16]byte{2, 3, 4, 5}))
	batch := pdata.NewLogs()
	batch.ResourceLogs().Append(first.ResourceLogs().At(0))
	batch.ResourceLogs().Append(second.ResourceLogs().At(0))

	// test
	err = p.ConsumeLogs(context.Background(), batch)

	// verify
	assert.NoError(t, err)
	assert.Len(t, sink.Logs, 2)
}

func randomLogs() pdata.Logs {
	v1 := uint8(rand.Intn(256))
	v2 := uint8(rand.Intn(256))
	v3 := uint8(rand.Intn(256))
	v4 := uint8(rand.Intn(256))
	return simpleLogWithID(pdata.NewTraceID([16]byte{v1, v2, v3, v4}))
}

func simpleLogs() pdata.Logs {
	return simpleLogWithID(pdata.NewTraceID([16]byte{1, 2, 3, 4}))
}

func simpleLogWithID(id pdata.TraceID) pdata.Logs {
	logs := pdata.NewLogs()
	logs.ResourceLogs().Resize(1)
	rl := logs.ResourceLogs().At(0)
	rl.InstrumentationLibraryLogs().Resize(1)
	ill := rl.InstrumentationLibraryLogs().At(0)
	ill.Logs().Resize(1)
	ill.Logs().At(0).SetTraceID(id)

	return logs
}

type mockLogsExporter struct {
	ConsumeLogsFn func(ctx context.Context, ld pdata.Logs) error
	StartFn       func(ctx context.Context, host component.Host) error
	ShutdownFn    func(ctx context.Context) error
}

func (e *mockLogsExporter) ConsumeLogs(ctx context.Context, ld pdata.Logs) error {
	if e.ConsumeLogsFn == nil {
		return nil
	}
	return e.ConsumeLogsFn(ctx, ld)
}

func (e *mockLogsExporter) Start(ctx context.Context, host component.Host) error {
	if e.StartFn == nil {
		return nil
	}
	return e.StartFn(ctx, host)
}

func (e *mockLogsExporter) Shutdown(ctx context.Context) error {
	if e.ShutdownFn == nil {
		return nil
	}
	return e.ShutdownFn(ctx)
}
