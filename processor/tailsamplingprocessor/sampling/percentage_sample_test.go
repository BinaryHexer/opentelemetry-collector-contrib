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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.opentelemetry.io/collector/consumer/pdata"
	"github.com/google/uuid"
)

func TestPercentageSampleFilter(t *testing.T) {
	const iter = 100
	const delta = 0.1

	cases := map[string]struct {
		allowRatio float32
	}{
		"100%": {allowRatio: 1.00},
		"95%":  {allowRatio: 0.95},
		"90%":  {allowRatio: 0.9},
		"80%":  {allowRatio: 0.8},
		"70%":  {allowRatio: 0.7},
		"60%":  {allowRatio: 0.6},
		"50%":  {allowRatio: 0.5},
		"40%":  {allowRatio: 0.4},
		"30%":  {allowRatio: 0.3},
		"20%":  {allowRatio: 0.2},
		"10%":  {allowRatio: 0.1},
		"5%":   {allowRatio: 0.05},
		"0%":   {allowRatio: 0.00},
	}

	for name, tt := range cases {
		name := name
		tt := tt

		t.Run(name, func(t *testing.T) {
			// setup
			counter := 0
			filter := NewPercentageSample(zap.NewNop(), tt.allowRatio)

			// execute
			for i := 0; i < iter; i++ {
				var empty = map[string]pdata.AttributeValue{}
				trace := newTraceStringAttrs(empty, "example", "value")
				u, _ := uuid.NewRandom()

				decision, err := filter.EvaluateTrace(pdata.NewTraceID(u), trace)
				assert.NoError(t, err)

				if decision == Sampled {
					counter++
				}
			}

			// verify
			obsRatio := float64(counter) / iter
			assert.InDelta(t, tt.allowRatio, obsRatio, delta)
		})
	}
}
