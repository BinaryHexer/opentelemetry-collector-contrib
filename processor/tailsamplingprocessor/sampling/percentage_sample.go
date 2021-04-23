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

package sampling

import (
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

type percentageSample struct {
	logger *zap.Logger
	sampler Sampler
}

var _ PolicyEvaluator = (*percentageSample)(nil)

// NewPercentageSample creates a policy evaluator the samples all traces.
func NewPercentageSample(logger *zap.Logger, samplingPercentage float32) PolicyEvaluator {
	return &percentageSample{
		logger: logger,
		sampler: NewSampler(samplingPercentage),
	}
}

// OnLateArrivingSpans notifies the evaluator that the given list of spans arrived
// after the sampling decision was already taken for the trace.
// This gives the evaluator a chance to log any message/metrics and/or update any
// related internal state.
func (ps *percentageSample) OnLateArrivingSpans(Decision, []*pdata.Span) error {
	ps.logger.Debug("Triggering action for late arriving spans in percentage-sample filter")
	return nil
}

func (ps *percentageSample) OnLateArrivingLogs(earlyDecision Decision, logs []*pdata.LogRecord) error {
	ps.logger.Debug("Triggering action for late arriving logs in percentage-sample filter")
	return nil
}

// EvaluateTrace looks at the trace data and returns a corresponding SamplingDecision.
func (ps *percentageSample) EvaluateTrace(pdata.TraceID, *TraceData) (Decision, error) {
	ps.logger.Debug("Evaluating spans in percentage-sample filter")

	if ps.sampler.Sample() {
		return Sampled, nil
	}

	return NotSampled, nil
}

func (ps *percentageSample) EvaluateLog(pdata.TraceID, *LogData) (Decision, error) {
	ps.logger.Debug("Evaluating logs in percentage-sample filter")

	if ps.sampler.Sample() {
		return Sampled, nil
	}

	return NotSampled, nil
}

const (
	total = 1.0
)

type Sampler interface {
	Sample() bool
}

type sampler struct {
	gen       *rand.Rand
	threshold float32 // 0 means all and 1.0 means none
}

func NewSampler(allowRatio float32) *sampler {
	gen := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &sampler{
		gen:       gen,
		threshold: total - allowRatio,
	}
}

func (s *sampler) Sample() bool {
	return s.gen.Float32() > s.threshold
}