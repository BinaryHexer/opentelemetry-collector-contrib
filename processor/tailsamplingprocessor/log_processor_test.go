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

package tailsamplingprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
)

func TestLogSamplingPolicyTypicalPath(t *testing.T) {
	traceIds, batches := generateLogBatches(128)
	cfg := Config{
		DecisionWait:            defaultTestDecisionWait,
		NumTraces:               uint64(2 * len(traceIds)),
		ExpectedNewTracesPerSec: 64,
		PolicyCfgs:              testPolicy,
	}
	msp := new(consumertest.LogsSink)
	tsp, _ := newLogProcessor(zap.NewNop(), msp, cfg)

	tsp.Start(context.Background(), componenttest.NewNopHost())

	for _, batch := range batches {
		tsp.ConsumeLogs(context.Background(), batch)
	}

	time.Sleep(32 * time.Second)

	assert.Equal(t, 8256, msp.LogRecordsCount())
}
