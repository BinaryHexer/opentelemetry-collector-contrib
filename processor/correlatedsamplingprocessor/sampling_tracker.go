package correlatedsamplingprocessor

import (
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/correlatedsamplingprocessor/sampling"
)

var _ samplingTracker = (*samplingTrackerImp)(nil)

var trackers = map[string]*samplingTrackerImp{}
var lock = &sync.RWMutex{}

func reset() {
	lock.RLock()
	trackers = map[string]*samplingTrackerImp{}
	lock.RUnlock()
}

type samplingTracker interface {
	GetDecision(pdata.TraceID) sampling.Decision
	SetDecision(pdata.TraceID, sampling.Decision)
	Delete(pdata.TraceID)
}

type samplingTrackerImp struct {
	logger     *zap.Logger
	traceIDMap map[pdata.TraceID]sampling.Decision
	mapLock    *sync.RWMutex
	traceCount uint64
}

func newSamplingTracker(logger *zap.Logger, ID string) *samplingTrackerImp {
	lock.RLock()
	tracker, ok := trackers[ID]
	lock.RUnlock()

	if ok {
		return tracker
	}

	lock.Lock()
	newTracker := &samplingTrackerImp{
		logger:     logger,
		traceIDMap: make(map[pdata.TraceID]sampling.Decision),
		mapLock:    &sync.RWMutex{},
	}
	trackers[ID] = newTracker
	lock.Unlock()

	return newTracker
}

func (s *samplingTrackerImp) GetDecision(traceID pdata.TraceID) sampling.Decision {
	s.mapLock.RLock()
	defer s.mapLock.RUnlock()

	return s.traceIDMap[traceID]
}

func (s *samplingTrackerImp) SetDecision(traceID pdata.TraceID, decision sampling.Decision) {
	s.mapLock.Lock()
	defer s.mapLock.Unlock()

	d, ok := s.traceIDMap[traceID]
	if !ok {
		// new trace
		atomic.AddUint64(&s.traceCount, 1)
	}

	if d != sampling.Sampled {
		s.traceIDMap[traceID] = decision
	}
}

func (s *samplingTrackerImp) Delete(traceID pdata.TraceID) {
	s.mapLock.Lock()
	defer s.mapLock.Unlock()

	delete(s.traceIDMap, traceID)
	// Subtract one from traceCount per https://godoc.org/sync/atomic#AddUint64
	atomic.AddUint64(&s.traceCount, ^uint64(0))
}
