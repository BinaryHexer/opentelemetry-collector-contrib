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
	"fmt"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configmodels"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"strings"
	"sync"
)

const (
	defaultPort = "55680"
)

var (
	errNoResolver                = errors.New("no resolvers specified for the exporter")
	errMultipleResolversProvided = errors.New("only one resolver should be specified")
)

var _ LoadBalancer = (*loadBalancerImp)(nil)

type ComponentFactory func(ctx context.Context, endpoint string) (component.Exporter, error)

type LoadBalancer interface {
	component.Component
	Exporter(traceID pdata.TraceID) (component.Exporter, string, error)
}

type loadBalancerImp struct {
	logger *zap.Logger
	host   component.Host

	res  resolver
	ring *hashRing

	componentFactory ComponentFactory
	exporters        map[string]component.Exporter

	stopped    bool
	updateLock sync.RWMutex
}

// Create new load balancer
func newLoadBalancer(params component.ExporterCreateParams, cfg configmodels.Exporter, factory ComponentFactory) (*loadBalancerImp, error) {
	oCfg := cfg.(*Config)

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

	return &loadBalancerImp{
		logger:           params.Logger,
		res:              res,
		componentFactory: factory,
		exporters:        map[string]component.Exporter{},
	}, nil
}

func (lb *loadBalancerImp) Start(ctx context.Context, host component.Host) error {
	lb.res.onChange(lb.onBackendChanges)
	lb.host = host
	if err := lb.res.start(ctx); err != nil {
		return err
	}

	return nil
}

func (lb *loadBalancerImp) onBackendChanges(resolved []string) {
	newRing := newHashRing(resolved)

	if !newRing.equal(lb.ring) {
		lb.updateLock.Lock()
		defer lb.updateLock.Unlock()

		lb.ring = newRing

		// TODO: set a timeout?
		ctx := context.Background()

		// add the missing exporters first
		lb.addMissingExporters(ctx, resolved)
		lb.removeExtraExporters(ctx, resolved)
	}
}

func (lb *loadBalancerImp) addMissingExporters(ctx context.Context, endpoints []string) {
	for _, endpoint := range endpoints {
		endpoint = endpointWithPort(endpoint)

		if _, exists := lb.exporters[endpoint]; !exists {
			exp, err := lb.componentFactory(ctx, endpoint)
			if err != nil {
				lb.logger.Error("failed to create new xxx exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
				continue
			}

			if err = exp.Start(ctx, lb.host); err != nil {
				lb.logger.Error("failed to start new xxx exporter for endpoint", zap.String("endpoint", endpoint), zap.Error(err))
				continue
			}
			lb.exporters[endpoint] = exp
		}
	}
}

func endpointWithPort(endpoint string) string {
	if !strings.Contains(endpoint, ":") {
		endpoint = fmt.Sprintf("%s:%s", endpoint, defaultPort)
	}
	return endpoint
}

func (lb *loadBalancerImp) removeExtraExporters(ctx context.Context, endpoints []string) {
	for existing := range lb.exporters {
		if !endpointFound(existing, endpoints) {
			lb.exporters[existing].Shutdown(ctx)
			delete(lb.exporters, existing)
		}
	}
}

func endpointFound(endpoint string, endpoints []string) bool {
	for _, candidate := range endpoints {
		if candidate == endpoint {
			return true
		}
	}

	return false
}

func (lb *loadBalancerImp) Shutdown(context.Context) error {
	lb.stopped = true
	return nil
}

func (lb *loadBalancerImp) Exporter(traceID pdata.TraceID) (component.Exporter, string, error) {
	// NOTE: make rolling updates of next tier of collectors work. currently this may cause
	// data loss because the latest batches sent to outdated backend will never find their way out.
	// for details: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/1690
	lb.updateLock.RLock()
	endpoint := lb.ring.endpointFor(traceID)
	exp, found := lb.exporters[endpoint]
	lb.updateLock.RUnlock()
	if !found {
		// something is really wrong... how come we couldn't find the exporter??
		return nil, endpoint, fmt.Errorf("couldn't find the exporter for the endpoint %q", endpoint)
	}

	return exp, endpoint, nil
}
