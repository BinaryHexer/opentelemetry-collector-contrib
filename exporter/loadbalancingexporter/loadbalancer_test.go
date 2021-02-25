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
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"go.uber.org/zap"
	"testing"
)

func TestNewLoadBalancerNoResolver(t *testing.T) {
	// prepare
	config := &Config{}
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}

	// test
	p, err := newLoadBalancer(params, config, nil)

	// verify
	require.Nil(t, p)
	require.Equal(t, errNoResolver, err)
}

func TestNewLoadBalancerInvalidStaticResolver(t *testing.T) {
	// prepare
	config := &Config{
		Resolver: ResolverSettings{
			Static: &StaticResolver{Hostnames: []string{}},
		},
	}
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}

	// test
	p, err := newLoadBalancer(params, config, nil)

	// verify
	require.Nil(t, p)
	require.Equal(t, errNoEndpoints, err)
}

func TestNewLoadBalancerInvalidDNSResolver(t *testing.T) {
	// prepare
	config := &Config{
		Resolver: ResolverSettings{
			DNS: &DNSResolver{
				Hostname: "",
			},
		},
	}
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}

	// test
	p, err := newLoadBalancer(params, config, nil)

	// verify
	require.Nil(t, p)
	require.Equal(t, errNoHostname, err)
}

func TestLoadBalancerStart(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newLoadBalancer(params, config, nil)
	require.NotNil(t, p)
	require.NoError(t, err)
	p.res = &mockResolver{}

	// test
	res := p.Start(context.Background(), componenttest.NewNopHost())
	defer p.Shutdown(context.Background())

	// verify
	assert.Nil(t, res)
}

func TestWithDNSResolver(t *testing.T) {
	config := &Config{
		Resolver: ResolverSettings{
			DNS: &DNSResolver{
				Hostname: "service-1",
			},
		},
	}
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newLoadBalancer(params, config, nil)
	require.NotNil(t, p)
	require.NoError(t, err)

	// test
	res, ok := p.res.(*dnsResolver)

	// verify
	assert.NotNil(t, res)
	assert.True(t, ok)
}

func TestMultipleResolvers(t *testing.T) {
	config := &Config{
		Resolver: ResolverSettings{
			Static: &StaticResolver{
				Hostnames: []string{"endpoint-1", "endpoint-2"},
			},
			DNS: &DNSResolver{
				Hostname: "service-1",
			},
		},
	}
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}

	// test
	p, err := newLoadBalancer(params, config, nil)

	// verify
	assert.Nil(t, p)
	assert.Equal(t, errMultipleResolversProvided, err)
}

func TestStartFailureStaticResolver(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newLoadBalancer(params, config, nil)
	require.NotNil(t, p)
	require.NoError(t, err)

	expectedErr := errors.New("some expected err")
	p.res = &mockResolver{
		onStart: func(context.Context) error {
			return expectedErr
		},
	}

	// test
	res := p.Start(context.Background(), componenttest.NewNopHost())

	// verify
	assert.Equal(t, expectedErr, res)
}

func TestLoadBalancerShutdown(t *testing.T) {
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

func TestOnBackendChanges(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newLoadBalancer(params, config, nil)
	require.NotNil(t, p)
	require.NoError(t, err)

	// test
	p.onBackendChanges([]string{"endpoint-1"})
	require.Len(t, p.ring.items, defaultWeight)

	// this should resolve to two endpoints
	endpoints := []string{"endpoint-1", "endpoint-2"}
	p.onBackendChanges(endpoints)

	// verify
	assert.Len(t, p.ring.items, 2*defaultWeight)
}

func TestRemoveExtraExporters(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	p, err := newLoadBalancer(params, config, nil)
	require.NotNil(t, p)
	require.NoError(t, err)

	p.exporters["endpoint-1"] = &componenttest.ExampleExporterConsumer{}
	p.exporters["endpoint-2"] = &componenttest.ExampleExporterConsumer{}
	resolved := []string{"endpoint-1"}

	// test
	p.removeExtraExporters(context.Background(), resolved)

	// verify
	assert.Len(t, p.exporters, 1)
	assert.NotContains(t, p.exporters, "endpoint-2")
}

func TestAddMissingExporters(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	exporterFactory := exporterhelper.NewFactory("otlp", func() configmodels.Exporter {
		return &otlpexporter.Config{}
	}, exporterhelper.WithTraces(func(
		_ context.Context,
		_ component.ExporterCreateParams,
		_ configmodels.Exporter,
	) (component.TracesExporter, error) {
		return &componenttest.ExampleExporterConsumer{}, nil
	}))
	tmplParams := component.ExporterCreateParams{
		Logger:               params.Logger,
		ApplicationStartInfo: params.ApplicationStartInfo,
	}
	fn := func(ctx context.Context, endpoint string) (component.Exporter, error) {
		oCfg := config.Protocol.OTLP
		oCfg.Endpoint = endpoint
		return exporterFactory.CreateTracesExporter(ctx, tmplParams, &oCfg)
	}

	p, err := newLoadBalancer(params, config, fn)
	require.NotNil(t, p)
	require.NoError(t, err)

	p.exporters["endpoint-1:55680"] = &componenttest.ExampleExporterConsumer{}
	resolved := []string{"endpoint-1", "endpoint-2"}

	// test
	p.addMissingExporters(context.Background(), resolved)

	// verify
	assert.Len(t, p.exporters, 2)
	assert.Contains(t, p.exporters, "endpoint-2:55680")
}

func TestFailedToAddMissingExporters(t *testing.T) {
	// prepare
	config := simpleConfig()
	params := component.ExporterCreateParams{
		Logger: zap.NewNop(),
	}
	expectedErr := errors.New("some expected error")
	exporterFactory := exporterhelper.NewFactory("otlp", func() configmodels.Exporter {
		return &otlpexporter.Config{}
	}, exporterhelper.WithTraces(func(
		_ context.Context,
		_ component.ExporterCreateParams,
		_ configmodels.Exporter,
	) (component.TracesExporter, error) {
		return nil, expectedErr
	}))
	tmplParams := component.ExporterCreateParams{
		Logger:               params.Logger,
		ApplicationStartInfo: params.ApplicationStartInfo,
	}
	fn := func(ctx context.Context, endpoint string) (component.Exporter, error) {
		oCfg := config.Protocol.OTLP
		oCfg.Endpoint = endpoint
		return exporterFactory.CreateTracesExporter(ctx, tmplParams, &oCfg)
	}

	p, err := newLoadBalancer(params, config, fn)
	require.NotNil(t, p)
	require.NoError(t, err)

	p.exporters["endpoint-1"] = &componenttest.ExampleExporterConsumer{}
	resolved := []string{"endpoint-1", "endpoint-2"}

	// test
	p.addMissingExporters(context.Background(), resolved)

	// verify
	assert.Len(t, p.exporters, 1)
	assert.NotContains(t, p.exporters, "endpoint-2")
}

func TestEndpointFound(t *testing.T) {
	for _, tt := range []struct {
		endpoint  string
		endpoints []string
		expected  bool
	}{
		{
			"endpoint-1",
			[]string{"endpoint-1", "endpoint-2"},
			true,
		},
		{
			"endpoint-3",
			[]string{"endpoint-1", "endpoint-2"},
			false,
		},
	} {
		assert.Equal(t, tt.expected, endpointFound(tt.endpoint, tt.endpoints))
	}
}

func TestEndpointWithPort(t *testing.T) {
	for _, tt := range []struct {
		input, expected string
	}{
		{
			"endpoint-1",
			"endpoint-1:55680",
		},
		{
			"endpoint-1:55690",
			"endpoint-1:55690",
		},
	} {
		assert.Equal(t, tt.expected, endpointWithPort(tt.input))
	}
}

// func TestFailedExporterInRing(t *testing.T) {
// 	// this test is based on the discussion in the original PR for this exporter:
// 	// https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/1542#discussion_r521268180
// 	// prepare
// 	config := &Config{
// 		Resolver: ResolverSettings{
// 			Static: &StaticResolver{Hostnames: []string{"endpoint-1", "endpoint-2"}},
// 		},
// 	}
// 	params := component.ExporterCreateParams{
// 		Logger: zap.NewNop(),
// 	}
// 	p, err := newTracesExporter(params, config)
// 	require.NotNil(t, p)
// 	require.NoError(t, err)
//
// 	err = p.Start(context.Background(), componenttest.NewNopHost())
// 	require.NoError(t, err)
//
// 	// simulate the case where one of the exporters failed to be created and do not exist in the internal map
// 	// this is a case that we are not even sure that might happen, so, this test case is here to document
// 	// this behavior. As the solution would require more locks/syncs/checks, we should probably wait to see
// 	// if this is really a problem in the real world
// 	delete(p.exporters, "endpoint-2")
//
// 	// sanity check
// 	require.Contains(t, p.res.(*staticResolver).endpoints, "endpoint-2")
//
// 	// test
// 	// this trace ID will reach the endpoint-2 -- see the consistent hashing tests for more info
// 	traceForMissingEndpoint := simpleTraceWithID(pdata.NewTraceID([16]byte{128, 128, 0, 0}))
// 	err = p.ConsumeTraces(context.Background(), traceForMissingEndpoint)
//
// 	// verify
// 	assert.Error(t, err)
// }
//
// func TestRollingUpdatesWhenConsumeTraces(t *testing.T) {
// 	// this test is based on the discussion in the following issue for this exporter:
// 	// https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/1690
// 	// prepare
//
// 	// simulate rolling updates, the dns resolver should resolve in the following order
// 	// ["127.0.0.1"] -> ["127.0.0.1", "127.0.0.2"] -> ["127.0.0.2"]
// 	res, err := newDNSResolver(zap.NewNop(), "service-1", "")
// 	require.NoError(t, err)
//
// 	resolverCh := make(chan struct{}, 1)
// 	counter := 0
// 	resolve := [][]net.IPAddr{
// 		{
// 			{IP: net.IPv4(127, 0, 0, 1)},
// 		}, {
// 			{IP: net.IPv4(127, 0, 0, 1)},
// 			{IP: net.IPv4(127, 0, 0, 2)},
// 		}, {
// 			{IP: net.IPv4(127, 0, 0, 2)},
// 		},
// 	}
// 	res.resolver = &mockDNSResolver{
// 		onLookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
// 			defer func() {
// 				counter++
// 			}()
//
// 			if counter <= 2 {
// 				return resolve[counter], nil
// 			}
//
// 			if counter == 3 {
// 				// stop as soon as rolling updates end
// 				resolverCh <- struct{}{}
// 			}
//
// 			return resolve[2], nil
// 		},
// 	}
// 	res.resInterval = 10 * time.Millisecond
//
// 	config := &Config{
// 		Resolver: ResolverSettings{
// 			DNS: &DNSResolver{Hostname: "service-1", Port: ""},
// 		},
// 	}
// 	params := component.ExporterCreateParams{Logger: zap.NewNop()}
// 	p, err := newTracesExporter(params, config)
// 	require.NotNil(t, p)
// 	require.NoError(t, err)
//
// 	p.res = res
//
// 	var counter1, counter2 int64
// 	defaultExporters := map[string]component.TracesExporter{
// 		"127.0.0.1": &mockTracesExporter{
// 			ConsumeTracesFn: func(ctx context.Context, td pdata.Traces) error {
// 				atomic.AddInt64(&counter1, 1)
// 				// simulate an unreachable backend
// 				time.Sleep(10 * time.Second)
// 				return nil
// 			},
// 		},
// 		"127.0.0.2": &mockTracesExporter{
// 			ConsumeTracesFn: func(ctx context.Context, td pdata.Traces) error {
// 				atomic.AddInt64(&counter2, 1)
// 				return nil
// 			},
// 		},
// 	}
//
// 	// test
// 	err = p.Start(context.Background(), componenttest.NewNopHost())
// 	require.NoError(t, err)
// 	defer p.Shutdown(context.Background())
// 	// ensure using default exporters
// 	p.updateLock.Lock()
// 	p.exporters = defaultExporters
// 	p.updateLock.Unlock()
// 	p.res.onChange(func(endpoints []string) {
// 		p.updateLock.Lock()
// 		p.exporters = defaultExporters
// 		p.updateLock.Unlock()
// 	})
//
// 	ctx, cancel := context.WithCancel(context.Background())
// 	// keep consuming traces every 2ms
// 	consumeCh := make(chan struct{})
// 	go func(ctx context.Context) {
// 		ticker := time.NewTicker(2 * time.Millisecond)
// 		for {
// 			select {
// 			case <-ctx.Done():
// 				consumeCh <- struct{}{}
// 				return
// 			case <-ticker.C:
// 				go p.ConsumeTraces(ctx, randomTraces())
// 			}
// 		}
// 	}(ctx)
//
// 	// give limited but enough time to rolling updates. otherwise this test
// 	// will still pass due to the 10 secs of sleep that is used to simulate
// 	// unreachable backends.
// 	go func() {
// 		time.Sleep(50 * time.Millisecond)
// 		resolverCh <- struct{}{}
// 	}()
//
// 	<-resolverCh
// 	cancel()
// 	<-consumeCh
//
// 	// verify
// 	require.Equal(t, []string{"127.0.0.2"}, res.endpoints)
// 	require.Greater(t, atomic.LoadInt64(&counter1), int64(0))
// 	require.Greater(t, atomic.LoadInt64(&counter2), int64(0))
// }
