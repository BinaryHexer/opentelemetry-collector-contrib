package correlation

import (
	"context"
	"hash/crc32"
	"math"
	"math/rand"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
	"go.opentelemetry.io/collector/consumer"
)

const numShards = 100
const minTraces = 10

var _ Processor = (*shardedProcessor)(nil)

var ids = map[string][]string{}
var lock = &sync.RWMutex{}

func ResetShards() {
	lock.RLock()
	ids = map[string][]string{}
	lock.RUnlock()
}

type shardedProcessor struct {
	processors []Processor
}

func NewShardedProcessor(logger *zap.Logger, cfg Config, ch combinatorHook, dh decisionHook, sh samplingHook) (*shardedProcessor, error) {
	processors := make([]Processor, numShards)
	IDs := newIDs(cfg.CorrelationID)
	for i := 0; i < numShards; i++ {
		c := cfg
		c.CorrelationID = IDs[i]
		c.NumTraces = uint64(math.Max(float64(cfg.NumTraces/numShards), minTraces))
		proc, err := NewProcessor(logger, c, ch, dh, sh)
		if err != nil {
			return nil, err
		}

		processors[i] = proc
	}

	return &shardedProcessor{processors: processors}, nil
}

func newIDs(ID string) []string {
	lock.RLock()
	IDs, ok := ids[ID]
	lock.RUnlock()

	if ok {
		return IDs
	}

	lock.Lock()
	newIDs := make([]string, numShards)
	for i := 0; i < numShards; i++ {
		newIDs[i] = uuid.NewString()
	}
	ids[ID] = newIDs
	lock.Unlock()

	return newIDs
}

func (s *shardedProcessor) Start(ctx context.Context, host component.Host) error {
	for _, p := range s.processors {
		err := p.Start(ctx, host)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *shardedProcessor) Shutdown(ctx context.Context) error {
	var errors []error

	for _, p := range s.processors {
		err := p.Shutdown(ctx)
		if err != nil {
			errors = append(errors, err)
		}
	}

	return consumererror.Combine(errors)
}

func (s *shardedProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *shardedProcessor) ConsumeSignal(s2 Signal) error {
	traceID := s2.TraceID
	if traceID == pdata.InvalidTraceID() {
		traceID = random()
	}

	processor, err := s.findProcessor(traceID)
	if err != nil {
		return err
	}

	return processor.ConsumeSignal(s2)
}

func (s *shardedProcessor) findProcessor(traceID pdata.TraceID) (Processor, error) {
	b := traceID.Bytes()
	hasher := crc32.NewIEEE()

	_, err := hasher.Write(b[:])
	if err != nil {
		return nil, err
	}

	hash := hasher.Sum32()
	pos := hash % numShards

	return s.processors[pos], nil
}

func (s *shardedProcessor) Metrics() ProcessorMetrics {
	metrics := ProcessorMetrics{}
	for _, p := range s.processors {
		m := p.Metrics()
		metrics.TraceCount += m.TraceCount
		metrics.SamplingDecisionCount += m.SamplingDecisionCount
		metrics.EvictCount += m.EvictCount
	}

	return metrics
}

func random() pdata.TraceID {
	v1 := uint8(rand.Intn(256))
	v2 := uint8(rand.Intn(256))
	v3 := uint8(rand.Intn(256))
	v4 := uint8(rand.Intn(256))
	return pdata.NewTraceID([16]byte{v1, v2, v3, v4})
}
