package sampling

import (
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

var _ Tracker = (*trackerImp)(nil)

var trackers = map[string]*trackerImp{}
var lock = &sync.RWMutex{}

func ResetTrackers() {
	lock.RLock()
	trackers = map[string]*trackerImp{}
	lock.RUnlock()
}

type Tracker interface {
	GetDecision(pdata.TraceID) Decision
	SetDecision(pdata.TraceID, Decision)
	Delete(pdata.TraceID)
	Count() int64
}

type trackerImp struct {
	logger      *zap.Logger
	decisionMap *DecisionSyncMap
	traceCount  uint64
}

func NewTracker(logger *zap.Logger, ID string) *trackerImp {
	lock.RLock()
	tracker, ok := trackers[ID]
	lock.RUnlock()

	if ok {
		return tracker
	}

	lock.Lock()
	newTracker := &trackerImp{
		logger:      logger,
		decisionMap: NewDecisionSyncMap(),
	}
	trackers[ID] = newTracker
	lock.Unlock()

	return newTracker
}

func (s *trackerImp) GetDecision(traceID pdata.TraceID) Decision {
	d, _ := s.decisionMap.Load(traceID)

	return d
}

func (s *trackerImp) SetDecision(traceID pdata.TraceID, decision Decision) {
	d, ok := s.decisionMap.Load(traceID)
	if !ok {
		// new trace
		atomic.AddUint64(&s.traceCount, 1)
	}

	if d != Sampled {
		s.decisionMap.Store(traceID, decision)
	}
}

func (s *trackerImp) Delete(traceID pdata.TraceID) {
	s.decisionMap.Delete(traceID)
	// subtract one from traceCount per https://godoc.org/sync/atomic#AddUint64
	atomic.AddUint64(&s.traceCount, ^uint64(0))
}

func (s *trackerImp) Count() int64 {
	return int64(atomic.LoadUint64(&s.traceCount))
}
